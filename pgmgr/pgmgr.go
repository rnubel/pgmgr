package pgmgr

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Migration directions
const (
	DOWN = iota
	UP
)

const datetimeFormat = "20060102130405"
const createRoleFunction = `
CREATE OR REPLACE FUNCTION public.create_role_if_not_exists(rolename VARCHAR)
RETURNS TEXT
AS
$create_role_if_not_exists$
DECLARE
BEGIN
	IF NOT EXISTS (
				SELECT *
				FROM   pg_catalog.pg_roles
				WHERE  rolname = rolename) THEN
			EXECUTE 'CREATE ROLE ' || quote_ident(rolename) || ' ;';
			RETURN '**!!**DROP ROLE ''' || rolename || ''';**!!**';
		ELSE
			RETURN FALSE;
		END IF;
END;
$create_role_if_not_exists$
LANGUAGE PLPGSQL;
`
const createGrantedByFunction = `
CREATE OR REPLACE FUNCTION public.granted_by(rolename VARCHAR, sql TEXT)
RETURNS BOOLEAN
AS
$role_exists$
DECLARE
BEGIN
	IF EXISTS (
			SELECT *
			FROM   pg_catalog.pg_roles
			WHERE  rolname = rolename) THEN
		EXECUTE sql;
		RETURN TRUE;
	ELSE
		RAISE NOTICE 'Rolename % does not exist, cannot set granted by', rolename;
		RETURN FALSE;
	END IF;
END;
$role_exists$
LANGUAGE PLPGSQL;
`

// Migration directions used for error message building
const (
	MIGRATION = "migration"
	ROLLBACK  = "rollback"
)

// Regular expressions matching ACL format of pg_dump
var /* const */ expressions = []*regexp.Regexp{
	regexp.MustCompile(`^GRANT .*\s(\S+);$`),
	regexp.MustCompile(`^REVOKE .*\s(\S+);$`),
	regexp.MustCompile(`ALTER DEFAULT PRIVILEGES .*\s(\S+);$`),
	regexp.MustCompile(`.* OWNER TO.*\s(\S+);$`),
}

// Migration stores a single migration's version and filename.
type Migration struct {
	Filename string
	Version  int64
}

// WrapInTransaction returns whether the migration should be run within
// a transaction.
func (m Migration) WrapInTransaction() bool {
	return !strings.Contains(m.Filename, ".no_txn.")
}

// Create creates the database specified by the configuration.
func Create(c *Config) error {
	return sh("createdb", []string{c.Database})
}

// Drop drops the database specified by the configuration.
func Drop(c *Config) error {
	return sh("dropdb", []string{c.Database})
}

// Dump dumps the schema and contents of the database to the dump file.
func Dump(c *Config) error {
	// dump schema first...
	schema, err := shRead("pg_dump", []string{"--schema-only", c.Database})
	if err != nil {
		return err
	}

	// then selected data...
	args := []string{c.Database, "--data-only", "--disable-triggers"}
	if len(c.SeedTables) > 0 {
		for _, table := range c.SeedTables {
			println("pulling data for", table)
			args = append(args, "-t", table)
		}
	}
	println(strings.Join(args, ""))

	seeds, err := shRead("pg_dump", args)
	if err != nil {
		return err
	}

	// then roles...
	users := make(map[string]string, 0)
	var match []int
	sqlBuffer := bytes.NewBuffer(*schema)
	scanner := bufio.NewScanner(sqlBuffer)
	for scanner.Scan() {
		for _, expression := range expressions {
			match = expression.FindSubmatchIndex(scanner.Bytes())
			if len(match) > 0 {
				data := scanner.Bytes()[match[2]:match[3]]
				users[string(regexp.MustCompile(`"`).ReplaceAll(data, []byte("")))] = "-1"
				break
			}
		}
	}
	roles, err := dumpRolesAndMemberships(c, users)
	if err != nil {
		return err
	}

	// then settings...
	settings, err := dumpSettings(c)
	if err != nil {
		return err
	}

	// finally database ownership
	ownership, err := alterDatabaseOwnership(c)
	if err != nil {
		return err
	}

	// and combine into one file.
	file, err := os.OpenFile(c.DumpFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	// must create roles before schema which includes ACL
	file.Write(*roles)
	file.Write(*schema)
	file.Write(*seeds)
	file.Write(*settings)
	file.Write(*ownership)
	file.Close()

	return nil
}

// Load loads the database from the dump file using psql.
func Load(c *Config) error {
	return sh("psql", []string{"-d", c.Database, "-f", c.DumpFile})
}

// Migrate applies un-applied migrations in the specified MigrationFolder.
func Migrate(c *Config) error {
	migrations, err := migrations(c, "up")
	if err != nil {
		return err
	}

	// ensure the version table is created
	if err := Initialize(c); err != nil {
		return err
	}

	appliedAny := false
	for _, m := range migrations {
		if applied, _ := migrationIsApplied(c, m.Version); !applied {
			fmt.Println("== Applying", m.Filename, "==")
			t0 := time.Now()

			if err = applyMigration(c, m, UP); err != nil { // halt the migration process and return the error.
				printFailedMigrationMessage(err, MIGRATION)
				return err
			}

			fmt.Println("== Completed in", time.Now().Sub(t0).Nanoseconds()/1e6, "ms ==")
			appliedAny = true
		}
	}

	if !appliedAny {
		fmt.Println("Nothing to do; all migrations already applied.")
	}

	return nil
}

// Rollback un-applies the latest migration, if possible.
func Rollback(c *Config) error {
	migrations, err := migrations(c, "down")
	if err != nil {
		return err
	}

	v, _ := Version(c)
	var toRollback *Migration
	for _, m := range migrations {
		if m.Version == v {
			toRollback = &m
			break
		}
	}

	if toRollback == nil {
		return nil
	}

	// rollback only the last migration
	fmt.Println("== Reverting", toRollback.Filename, "==")
	t0 := time.Now()

	if err = applyMigration(c, *toRollback, DOWN); err != nil {
		printFailedMigrationMessage(err, ROLLBACK)
		return err
	}

	fmt.Println("== Completed in", time.Now().Sub(t0).Nanoseconds()/1e6, "ms ==")

	return nil
}

func alterDatabaseOwnership(c *Config) (*[]byte, error) {
	db, err := openConnection(c)
	defer db.Close()

	if err != nil {
		return nil, err
	}

	var owner string
	err = db.QueryRow(fmt.Sprintf("SELECT rolname FROM pg_database d JOIN pg_roles ON pg_roles.oid = d.datdba WHERE datname = '%s'", c.Database)).Scan(&owner)
	if err != nil {
		return nil, err
	}

	sql := []byte(fmt.Sprintf(`ALTER DATABASE "%s" OWNER TO "%s";`, c.Database, owner))
	return &sql, nil
}

func dumpSettings(c *Config) (*[]byte, error) {
	db, err := openConnection(c)
	defer db.Close()

	if err != nil {
		return nil, err
	}

	rows, err := db.Query(fmt.Sprintf(`
		SELECT UNNEST(setconfig) AS config
		FROM pg_catalog.pg_db_role_setting
		JOIN pg_database ON pg_database.oid = setdatabase
		-- 0 = default, for all users
		WHERE setrole = 0
		AND datname = '%s'`, c.Database))

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var setting string
	var buffer bytes.Buffer
	for rows.Next() {
		if err := rows.Scan(&setting); err != nil {
			return nil, err
		}
		// wipe out all spaces
		setting = regexp.MustCompile(`\s+`).ReplaceAllString(setting, "")
		// chomp
		setting = regexp.MustCompile(`\r?\n`).ReplaceAllString(setting, "")
		// if there is still data left
		if len(setting) > 0 {
			// append an empty string if the setting was being assigned a value of nothing
			if match := regexp.MustCompile(`=$`).FindStringSubmatchIndex(setting); len(match) > 0 {
				setting += "''"
			}
			buffer.WriteString(fmt.Sprintf("ALTER DATABASE :DBNAME SET %s;", setting))
		}
	}

	// TODO: write panics if buffer too large
	data := buffer.Bytes()
	return &data, nil
}

func dumpRolesAndMemberships(c *Config, roles map[string]string) (*[]byte, error) {
	err := markUserRoles(c, roles)
	if err != nil {
		return nil, err
	}

	db, err := openConnection(c)
	defer db.Close()

	if err != nil {
		return nil, err
	}

	var roles_to_dump []string
	var oids_to_avoid []string

	for role, oid := range roles {
		if oid == "-1" {
			roles_to_dump = append(roles_to_dump, role)
		} else {
			oids_to_avoid = append(oids_to_avoid, oid)
		}
	}

	rows, err := db.Query(
		`WITH RECURSIVE memberships(roleid, member, admin_option, grantor) AS (
				SELECT ur.oid AS roleid,
							 NULL::oid AS member,
							 NULL::boolean AS admin_option,
							 NULL::oid AS grantor
				FROM pg_roles ur
				WHERE ur.rolname = ANY($1::TEXT[])
				UNION
				SELECT COALESCE(a.roleid, r.oid) AS roleid,
							 a.member AS member,
							 a.admin_option AS admin_option,
							 a.grantor AS grantor
				FROM pg_auth_members a
				FULL OUTER JOIN pg_roles r ON FALSE
				JOIN memberships
					ON (memberships.roleid = a.member)
					OR (memberships.roleid = r.oid OR memberships.member = r.oid)
					OR (memberships.roleid = a.roleid AND COALESCE(memberships.member, 0::oid) <> a.member AND a.member <> ANY($2::OID[]))
			)
			SELECT DISTINCT ON(ur.rolname, um.rolname)
						 ur.rolname AS roleid,
						 um.rolname AS member,
						 memberships.admin_option,
						 ug.rolname AS grantor,
						 ur.rolname,
						 ur.rolsuper,
						 ur.rolinherit,
						 ur.rolcreaterole,
						 ur.rolcreatedb,
						 ur.rolcanlogin,
						 ur.rolconnlimit,
						 ur.rolvaliduntil,
						 ur.rolreplication,
						 pg_catalog.shobj_description(memberships.roleid, 'pg_authid') as rolcomment
			FROM memberships
			LEFT JOIN pg_roles ur on ur.oid = memberships.roleid
			LEFT JOIN pg_roles um on um.oid = memberships.member
			LEFT JOIN pg_roles ug on ug.oid = memberships.grantor
			ORDER BY 1,2 NULLS LAST`, "{"+strings.Join(roles_to_dump, ",")+"}", "{"+strings.Join(oids_to_avoid, ",")+"}")

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type pgRoles struct {
		roleid         sql.NullString
		member         sql.NullString
		adminOption    sql.NullBool
		grantor        sql.NullString
		rolname        string
		rolsuper       sql.NullBool
		rolinherit     sql.NullBool
		rolcreaterole  sql.NullBool
		rolcreatedb    sql.NullBool
		rolcanlogin    sql.NullBool
		rolconnlimit   sql.NullInt64
		rolvaliduntil  sql.NullString
		rolreplication sql.NullBool
		rolcomment     sql.NullString
	}

	rolesAndMemberships := []pgRoles{}
	for rows.Next() {
		var row pgRoles
		if err := rows.Scan(&row.roleid, &row.member, &row.adminOption, &row.grantor, &row.rolname, &row.rolsuper, &row.rolinherit,
			&row.rolcreaterole, &row.rolcreatedb, &row.rolcanlogin, &row.rolconnlimit, &row.rolvaliduntil, &row.rolreplication, &row.rolcomment); err != nil {
			return nil, err
		}
		rolesAndMemberships = append(rolesAndMemberships, row)
	}

	createdRoles := make(map[string]bool)
	var buffer bytes.Buffer
	buffer.WriteString(createRoleFunction)
	buffer.WriteString(createGrantedByFunction)
	for _, r := range rolesAndMemberships {
		if _, ok := createdRoles[r.rolname]; !ok {
			createdRoles[r.rolname] = true
			buffer.WriteString(fmt.Sprintf("SELECT * FROM create_role_if_not_exists('%s');\n", r.rolname))
			buffer.WriteString(fmt.Sprintf(`ALTER ROLE "%s" WITH `, r.rolname))
			if r.rolsuper.Valid {
				if r.rolsuper.Bool == true {
					buffer.WriteString("SUPERUSER ")
				} else {
					buffer.WriteString("NO SUPERUSER ")
				}
			}
			if r.rolinherit.Valid {
				if r.rolinherit.Bool == true {
					buffer.WriteString("INHERIT ")
				} else {
					buffer.WriteString("NO INHERIT ")
				}
			}
			if r.rolcreaterole.Valid {
				if r.rolcreaterole.Bool == true {
					buffer.WriteString("CREATEROLE ")
				} else {
					buffer.WriteString("NO CREATEROLE ")
				}
			}
			if r.rolcreatedb.Valid {
				if r.rolcreatedb.Bool == true {
					buffer.WriteString("CREATEDB ")
				} else {
					buffer.WriteString("NO CREATEDB ")
				}
			}
			if r.rolcanlogin.Valid {
				if r.rolcanlogin.Bool == true {
					buffer.WriteString("LOGIN ")
				} else {
					buffer.WriteString("NO LOGIN ")
				}
			}
			if r.rolreplication.Valid {
				if r.rolreplication.Bool == true {
					buffer.WriteString("REPLICATION ")
				} else {
					buffer.WriteString("NO REPLICATION ")
				}
			}
			if r.rolconnlimit.Valid {
				if r.rolconnlimit.Int64 != -1 {
					buffer.WriteString(fmt.Sprintf("CONNECTION LIMIT %d ", r.rolconnlimit.Int64))
				}
			}
			if r.rolvaliduntil.Valid {
				buffer.WriteString(fmt.Sprintf("VALID UNTIL '%s' ", r.rolvaliduntil.String))
			}

			buffer.WriteString(";\n")

			if r.rolcomment.Valid {
				buffer.WriteString(fmt.Sprintf(`COMMENT ON ROLE "%s" IS '%s';\n`, r.rolname, r.rolcomment.String))
			}

			if r.member.Valid {
				buffer.WriteString(fmt.Sprintf(`GRANT "%s" TO "%s" `, r.roleid.String, r.member.String))
				if r.adminOption.Valid {
					buffer.WriteString("WITH ADMIN OPTION ")
				}
				buffer.WriteString(";\n")
				if r.grantor.Valid {
					buffer.WriteString(fmt.Sprintf(`SELECT * FROM granted_by('%s', $$GRANT "%s" TO "%s" `, r.grantor.String, r.roleid.String, r.member.String))
					if r.adminOption.Valid {
						buffer.WriteString("WITH ADMIN OPTION ")
					}
					buffer.WriteString(fmt.Sprintf(`GRANTED BY "%s");\n`, r.grantor.String))
				}
			}
		}
	}
	buffer.WriteString("DROP FUNCTION public.create_role_if_not_exists(VARCHAR);\n")
	buffer.WriteString("DROP FUNCTION public.granted_by(VARCHAR, TEXT);")

	// TODO: write panics if buffer too large
	data := buffer.Bytes()
	return &data, nil
}

func markUserRoles(c *Config, roles map[string]string) error {
	db, err := openConnection(c)
	defer db.Close()

	if err != nil {
		return err
	}

	usersFromAcl := make([]string, len(roles))
	i := 0
	for role, _ := range roles {
		usersFromAcl[i] = role
		i++
	}

	for _, role := range c.UserRoles {
		usersFromAcl = append(usersFromAcl, role)
	}

	rows, err := db.Query(
		`SELECT oid, rolname
		FROM pg_roles
		WHERE rolcanlogin
		AND NOT rolsuper
		AND rolname <> ANY($1::TEXT[])`, "{"+strings.Join(usersFromAcl, ",")+"}") //not necessary to quote
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			oid     string
			rolname string
		)
		if err := rows.Scan(&oid, &rolname); err != nil {
			return err
		}
		roles[rolname] = oid
	}

	return nil
}

// Version returns the highest version number stored in the database. This is not
// necessarily enough info to uniquely identify the version, since there may
// be backdated migrations which have not yet applied.
func Version(c *Config) (int64, error) {
	db, err := openConnection(c)
	defer db.Close()

	if err != nil {
		return -1, err
	}

	exists, err := migrationTableExists(c, db)
	if err != nil {
		return -1, err
	}

	if !exists {
		return -1, nil
	}

	var version int64
	err = db.QueryRow(fmt.Sprintf(
		`SELECT COALESCE(MAX(version)::text, '-1') FROM %s`,
		c.quotedMigrationTable(),
	)).Scan(&version)

	return version, err
}

// Initialize creates the schema_migrations table if necessary.
func Initialize(c *Config) error {
	db, err := openConnection(c)
	defer db.Close()
	if err != nil {
		return err
	}

	if err := createSchemaUnlessExists(c, db); err != nil {
		return err
	}

	tableExists, err := migrationTableExists(c, db)
	if err != nil {
		return err
	}

	if tableExists {
		return nil
	}

	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (version %s NOT NULL UNIQUE);",
		c.quotedMigrationTable(),
		c.versionColumnType(),
	))

	if err != nil {
		return err
	}

	return nil
}

// CreateMigration generates new, empty migration files.
func CreateMigration(c *Config, name string, noTransaction bool) error {
	version := generateVersion(c)
	prefix := fmt.Sprint(version, "_", name)

	if noTransaction {
		prefix += ".no_txn"
	}

	upFilepath := filepath.Join(c.MigrationFolder, prefix+".up.sql")
	downFilepath := filepath.Join(c.MigrationFolder, prefix+".down.sql")

	err := ioutil.WriteFile(upFilepath, []byte(`-- Migration goes here.`), 0644)
	if err != nil {
		return err
	}
	fmt.Println("Created", upFilepath)

	err = ioutil.WriteFile(downFilepath, []byte(`-- Rollback of migration goes here. If you don't want to write it, delete this file.`), 0644)
	if err != nil {
		return err
	}
	fmt.Println("Created", downFilepath)

	return nil
}

func generateVersion(c *Config) string {
	// TODO: guarantee no conflicts by incrementing if there is a conflict
	t := time.Now()

	if c.Format == "datetime" {
		return t.Format(datetimeFormat)
	}

	return strconv.FormatInt(t.Unix(), 10)
}

// need access to the original query contents in order to print it out properly,
// unfortunately.
func formatPgErr(contents *[]byte, pgerr *pq.Error) string {
	pos, _ := strconv.Atoi(pgerr.Position)
	lineNo := bytes.Count((*contents)[:pos], []byte("\n")) + 1
	columnNo := pos - bytes.LastIndex((*contents)[:pos], []byte("\n")) - 1

	return fmt.Sprint("PGERROR: line ", lineNo, " pos ", columnNo, ": ", pgerr.Message, ". ", pgerr.Detail)
}

type execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func applyMigration(c *Config, m Migration, direction int) error {
	var tx *sql.Tx
	var exec execer

	rollback := func() {
		if tx != nil {
			tx.Rollback()
		}
	}

	contents, err := ioutil.ReadFile(filepath.Join(c.MigrationFolder, m.Filename))
	if err != nil {
		return err
	}

	db, err := openConnection(c)
	defer db.Close()
	if err != nil {
		return err
	}
	exec = db

	if m.WrapInTransaction() {
		tx, err = db.Begin()
		if err != nil {
			return err
		}
		exec = tx
	}

	if _, err = exec.Exec(string(contents)); err != nil {
		rollback()
		return errors.New(formatPgErr(&contents, err.(*pq.Error)))
	}

	if direction == UP {
		if err = insertSchemaVersion(c, exec, m.Version); err != nil {
			rollback()
			return errors.New(formatPgErr(&contents, err.(*pq.Error)))
		}
	} else {
		if err = deleteSchemaVersion(c, exec, m.Version); err != nil {
			rollback()
			return errors.New(formatPgErr(&contents, err.(*pq.Error)))
		}
	}

	if tx != nil {
		err = tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

func createSchemaUnlessExists(c *Config, db *sql.DB) error {
	// If there's no schema name in the config, we don't need to create the schema.
	if !strings.Contains(c.MigrationTable, ".") {
		return nil
	}

	var exists bool

	schema := strings.SplitN(c.MigrationTable, ".", 2)[0]
	err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)`,
		schema,
	).Scan(&exists)

	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	_, err = db.Exec(fmt.Sprintf(
		"CREATE SCHEMA %s;",
		pq.QuoteIdentifier(schema),
	))
	return err
}

func insertSchemaVersion(c *Config, tx execer, version int64) error {
	_, err := tx.Exec(
		fmt.Sprintf(`INSERT INTO %s (version) VALUES ($1) RETURNING version;`, c.quotedMigrationTable()),
		typedVersion(c, version),
	)
	return err
}

func deleteSchemaVersion(c *Config, tx execer, version int64) error {
	_, err := tx.Exec(
		fmt.Sprintf(`DELETE FROM %s WHERE version = $1`, c.quotedMigrationTable()),
		typedVersion(c, version),
	)
	return err
}

func typedVersion(c *Config, version int64) interface{} {
	if c.ColumnType == "string" {
		return strconv.FormatInt(version, 10)
	}
	return version
}

func migrationTableExists(c *Config, db *sql.DB) (bool, error) {
	var hasTable bool
	var err error

	if strings.Contains(c.MigrationTable, ".") {
		tokens := strings.SplitN(c.MigrationTable, ".", 2)
		err = db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename = $2)`,
			tokens[0], tokens[1],
		).Scan(&hasTable)
	} else {
		err = db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = $1)`,
			c.MigrationTable,
		).Scan(&hasTable)
	}

	return hasTable, err
}

func migrationIsApplied(c *Config, version int64) (bool, error) {
	db, err := openConnection(c)
	defer db.Close()
	if err != nil {
		return false, err
	}

	var applied bool
	err = db.QueryRow(
		fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE version = $1)`, c.quotedMigrationTable()),
		version,
	).Scan(&applied)

	if err != nil {
		return false, err
	}

	return applied, nil
}

func openConnection(c *Config) (*sql.DB, error) {
	db, err := sql.Open("postgres", sqlConnectionString(c))
	return db, err
}

func sqlConnectionString(c *Config) string {
	return fmt.Sprint(
		" user='", c.Username, "'",
		" dbname='", c.Database, "'",
		" password='", c.Password, "'",
		" host='", c.Host, "'",
		" port=", c.Port,
		" sslmode=", c.SslMode)
}

func migrations(c *Config, direction string) ([]Migration, error) {
	re := regexp.MustCompile("^[0-9]+")

	migrations := []Migration{}
	files, err := ioutil.ReadDir(c.MigrationFolder)
	if err != nil {
		return migrations, err
	}

	for _, file := range files {
		if match, _ := regexp.MatchString("^[0-9]+_.+\\."+direction+"\\.sql$", file.Name()); match {
			version, _ := strconv.ParseInt(re.FindString(file.Name()), 10, 64)
			migrations = append(migrations, Migration{Filename: file.Name(), Version: version})
		}
	}

	return migrations, nil
}

func sh(command string, args []string) error {
	c := exec.Command(command, args...)
	output, err := c.CombinedOutput()
	fmt.Println(string(output))
	if err != nil {
		return err
	}

	return nil
}

func shRead(command string, args []string) (*[]byte, error) {
	c := exec.Command(command, args...)
	output, err := c.CombinedOutput()
	return &output, err
}

func printFailedMigrationMessage(err error, migrationType string) {
	fmt.Fprintf(os.Stderr, err.Error())
	fmt.Fprintf(os.Stderr, "\n\n")
	fmt.Fprintf(os.Stderr, "ERROR! Aborting the "+migrationType+" process.\n")
}
