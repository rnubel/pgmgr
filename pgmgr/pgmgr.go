package pgmgr

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Migration struct {
	Filename string
	Version  int
}

const (
	DOWN = iota
	UP   = iota
)

// Creates the database specified by the configuration.
func Create(c *Config) error {
	return sh("createdb", []string{c.Database})
}

// Drops the database specified by the configuration.
func Drop(c *Config) error {
	return sh("dropdb", []string{c.Database})
}

// Dumps the schema and contents of the database to the dump file.
func Dump(c *Config) error {
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

// Loads the database from the dump file.
func Load(c *Config) error {
	return sh("psql", []string{"-d", c.Database, "-f", c.DumpFile})
}

// Applies un-applied migrations in the specified MigrationFolder.
func Migrate(c *Config) error {
	migrations, err := migrations(c, "up")
	if err != nil {
		return err
	}

	// ensure the version table is created
	_, err = getOrInitializeVersion(c)
	if err != nil {
		return err
	}

	appliedAny := false
	for _, m := range migrations {
		if applied, _ := migrationIsApplied(c, m.Version); !applied {
			fmt.Println("== Applying", m.Filename, "==")
			t0 := time.Now()

			if err = applyMigration(c, m, UP); err != nil { // halt the migration process and return the error.
				fmt.Println(err)
				fmt.Println("")
				fmt.Println("ERROR! Aborting the migration process.")
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

// Un-applies the latest migration, if possible.
func Rollback(c *Config) error {
	migrations, err := migrations(c, "down")
	if err != nil {
		return err
	}

	v, _ := Version(c)
	var to_rollback *Migration
	for _, m := range migrations {
		if m.Version == v {
			to_rollback = &m
			break
		}
	}

	if to_rollback == nil {
		return nil
	}

	// rollback only the last migration
	fmt.Println("== Reverting", to_rollback.Filename, "==")
	t0 := time.Now()

	if err = applyMigration(c, *to_rollback, DOWN); err != nil {
		return err
	}

	fmt.Println("== Completed in", time.Now().Sub(t0).Nanoseconds()/1e6, "ms ==")

	return nil
}

// Returns the highest version number stored in the database. This is not
// necessarily enough info to uniquely identify the version, since there may
// be backdated migrations which have not yet applied.
func Version(c *Config) (int, error) {
	db, err := openConnection(c)
	defer db.Close()

	if err != nil {
		return -1, err
	}

	// if the table doesn't exist, we're at version -1
	var hasTable bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_tables WHERE tablename='schema_migrations')").Scan(&hasTable)
	if hasTable != true {
		return -1, err
	}

	var version int
	err = db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version)

	return version, err
}

// Creates the schema_migrations table on what should be a new database.
func Initialize(c *Config) error {
	db, err := openConnection(c)
	defer db.Close()
	if err != nil {
		return err
	}

	_, err = db.Exec("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
	if err != nil {
		return err
	}

	return nil
}

// Creates new, blank migration files.
func CreateMigration(c *Config, name string) error {
	version := generateVersion()
	up_filepath := filepath.Join(c.MigrationFolder, fmt.Sprint(version, "_", name, ".up.sql"))
	down_filepath := filepath.Join(c.MigrationFolder, fmt.Sprint(version, "_", name, ".down.sql"))

	err := ioutil.WriteFile(up_filepath, []byte(`-- Migration goes here.`), 0644)
	if err != nil {
		return err
	}
	fmt.Println("Created", up_filepath)

	err = ioutil.WriteFile(down_filepath, []byte(`-- Rollback of migration goes here. If you don't want to write it, delete this file.`), 0644)
	if err != nil {
		return err
	}
	fmt.Println("Created", down_filepath)

	return nil
}

func generateVersion() int {
	// TODO: guarantee no conflicts by incrementing if there is a conflict
	v := int(time.Now().Unix())
	return v
}

// need access to the original query contents in order to print it out properly,
// unfortunately.
func formatPgErr(contents *[]byte, pgerr *pq.Error) string {
	pos, _ := strconv.Atoi(pgerr.Position)
	lineNo := bytes.Count((*contents)[:pos], []byte("\n")) + 1
	columnNo := pos - bytes.LastIndex((*contents)[:pos], []byte("\n")) - 1

	return fmt.Sprint("PGERROR: line ", lineNo, " pos ", columnNo, ": ", pgerr.Message, ". ", pgerr.Detail)
}

func applyMigration(c *Config, m Migration, direction int) error {
	db, err := openConnection(c)
	defer db.Close()
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	contents, err := ioutil.ReadFile(filepath.Join(c.MigrationFolder, m.Filename))
	if err != nil {
		return err
	}

	if _, err = tx.Exec(string(contents)); err != nil {
		tx.Rollback()
		return errors.New(formatPgErr(&contents, err.(*pq.Error)))
	}

	if direction == UP {
		if err = insertSchemaVersion(tx, m.Version); err != nil {
			tx.Rollback()
			return errors.New(formatPgErr(&contents, err.(*pq.Error)))
		}
	} else {
		if err = deleteSchemaVersion(tx, m.Version); err != nil {
			tx.Rollback()
			return errors.New(formatPgErr(&contents, err.(*pq.Error)))
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func insertSchemaVersion(tx *sql.Tx, version int) error {
	_, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES ($1) RETURNING version", version)
	return err
}

func deleteSchemaVersion(tx *sql.Tx, version int) error {
	_, err := tx.Exec("DELETE FROM schema_migrations WHERE version = $1", version)
	return err
}

func migrationIsApplied(c *Config, version int) (bool, error) {
	db, err := openConnection(c)
	defer db.Close()
	if err != nil {
		return false, err
	}

	var is_applied bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1);", version).Scan(&is_applied)
	if err != nil {
		return false, err
	}

	return is_applied, nil
}

func getOrInitializeVersion(c *Config) (int, error) {
	var v int
	if v, _ = Version(c); v < 0 {
		if err := Initialize(c); err != nil {
			return -1, err
		}
	}

	return v, nil
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
		" sslmode=", "disable")
}

func migrations(c *Config, direction string) ([]Migration, error) {
	files, err := ioutil.ReadDir(c.MigrationFolder)
	migrations := []Migration{}
	if err != nil {
		return []Migration{}, err
	}

	for _, file := range files {
		if match, _ := regexp.MatchString("[0-9]+_.+."+direction+".sql", file.Name()); match {
			re := regexp.MustCompile("^[0-9]+")
			version, _ := strconv.Atoi(re.FindString(file.Name()))
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
