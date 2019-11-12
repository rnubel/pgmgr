package pgmgr

import (
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

// Migration directions used for error message building
const (
	MIGRATION = "migration"
	ROLLBACK  = "rollback"
)

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
	if err := c.DumpToEnv(); err != nil {
		return err
	}

	return sh("createdb", []string{c.Database})
}

// Drop drops the database specified by the configuration.
func Drop(c *Config) error {
	if err := c.DumpToEnv(); err != nil {
		return err
	}

	return sh("dropdb", []string{c.Database})
}

// Dump dumps the schema and contents of the database to the dump file.
func Dump(c *Config) error {
	if err := c.DumpToEnv(); err != nil {
		return err
	}

	// dump schema first...
	schema, err := shRead("pg_dump", []string{"--schema-only", c.Database})
	if err != nil {
		return err
	}

	// then selected data...
	args := []string{c.Database, "--data-only"}
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

	// and combine into one file.
	file, err := os.OpenFile(c.DumpFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	file.Write(*schema)
	file.Write(*seeds)
	file.Close()

	return nil
}

// Load loads the database from the dump file using psql.
func Load(c *Config) error {
	if err := c.DumpToEnv(); err != nil {
		return err
	}

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
	if c.MigrationDriver == "psql" {
		return applyMigrationByPsql(c, m, direction)
	}

	return applyMigrationByPq(c, m, direction)
}

func applyMigrationByPsql(c *Config, m Migration, direction int) error {
	if err := c.DumpToEnv(); err != nil {
		return err
	}

	contents, err := ioutil.ReadFile(filepath.Join(c.MigrationFolder, m.Filename))
	if err != nil {
		return err
	}

	tmpfile, err := ioutil.TempFile("", "migration")
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile.Name()) // clean up

	if _, err := tmpfile.Write(contents); err != nil {
		return err
	}

	tmpfile.WriteString(
		fmt.Sprintf(`; INSERT INTO %s (version) VALUES ('%d');`, c.quotedMigrationTable(), m.Version),
	)

	if err := tmpfile.Close(); err != nil {
		return err
	}

	migrationFilePath := tmpfile.Name()
	args := []string{"-f", migrationFilePath, "-v", "ON_ERROR_STOP=1"}

	if m.WrapInTransaction() {
		args = append(args, "-1")
	}

	if err := sh("psql", args); err != nil {
		return err
	}

	return nil
}

func applyMigrationByPq(c *Config, m Migration, direction int) error {
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
	db, err := sql.Open("postgres", SQLConnectionString(c))
	return db, err
}

// SQLConnectionString formats the values pulled from the config into a connection string
func SQLConnectionString(c *Config) string {
	args := make([]interface{}, 0)

	if c.Username != "" {
		args = append(args, " user='", c.Username, "'")
	}

	if c.Database != "" {
		args = append(args, " dbname='", c.Database, "'")
	}

	if c.Password != "" {
		args = append(args, " password='", c.Password, "'")
	}

	args = append(args,
		" host='", c.Host, "'",
		" port=", c.Port,
		" sslmode=", c.SslMode)

	return fmt.Sprint(args...)
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
