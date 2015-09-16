package pgmgr

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testDBName      = "pgmgr_testdb"
	migrationFolder = "/tmp/migrations/"
	dumpFile        = "/tmp/pgmgr_dump.sql"
)

func globalConfig() *Config {
	return &Config{
		Database:        testDBName,
		Host:            "localhost",
		Port:            5432,
		DumpFile:        dumpFile,
		MigrationFolder: migrationFolder,
		MigrationTable:  "schema_migrations",
	}
}

func TestCreate(t *testing.T) {
	dropDB(t)

	if err := Create(globalConfig()); err != nil {
		t.Log(err)
		t.Fatal("Could not create database")
	}

	// if we can't remove that db, it couldn't have been created by us above.
	if err := dropDB(t); err != nil {
		t.Fatal("database doesn't seem to have been created!")
	}
}

func TestDrop(t *testing.T) {
	if err := createDB(t); err != nil {
		t.Fatal("createdb failed: ", err)
	}

	if err := Drop(globalConfig()); err != nil {
		t.Log(err)
		t.Fatal("Could not drop database")
	}

	if err := createDB(t); err != nil {
		t.Fatal("database doesn't seem to have been dropped!")
	}
}

func TestDump(t *testing.T) {
	resetDB(t)
	psqlMustExec(t, `CREATE TABLE bars (bar_id INTEGER);`)
	psqlMustExec(t, `INSERT INTO bars (bar_id) VALUES (123), (456);`)
	psqlMustExec(t, `CREATE TABLE foos (foo_id INTEGER);`)
	psqlMustExec(t, `INSERT INTO foos (foo_id) VALUES (789);`)

	c := globalConfig()
	err := Dump(c)

	if err != nil {
		t.Log(err)
		t.Fatal("Could not dump database to file")
	}

	file, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		t.Log(err)
		t.Fatal("Could not read dump")
	}

	if !strings.Contains(string(file), "CREATE TABLE bars") {
		t.Fatal("dump does not contain the table definition")
	}

	if !strings.Contains(string(file), "123") {
		t.Fatal("dump does not contain the table data when --seed-tables is not specified")
	}

	c.SeedTables = append(c.SeedTables, "foos")
	err = Dump(c)

	if err != nil {
		t.Log(err)
		t.Fatal("Could not dump database to file")
	}

	file, err = ioutil.ReadFile(dumpFile)
	if err != nil {
		t.Log(err)
		t.Fatal("Could not read dump")
	}

	if strings.Contains(string(file), "123") {
		t.Fatal("dump contains table data for non-seed tables, when --seed-tables was given")
	}

	if !strings.Contains(string(file), "789") {
		t.Fatal("dump does not contain table data for seed tables, when --seed-tables was given")
	}
}

func TestLoad(t *testing.T) {
	resetDB(t)

	ioutil.WriteFile(dumpFile, []byte(`
		CREATE TABLE foos (foo_id INTEGER);
		INSERT INTO foos (foo_id) VALUES (1), (2), (3);
	`), 0644)

	err := Load(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not load database from file")
	}

	psqlMustExec(t, `SELECT * FROM foos;`)
}

func TestInitialize(t *testing.T) {
	config := globalConfig()

	// Default config should create public.schema_migrations
	resetDB(t)

	if err := Initialize(config); err != nil {
		t.Fatal("Initialize failed:", err)
	}

	if err := Initialize(config); err != nil {
		t.Fatal("Initialize was not safe to run twice:", err)
	}

	psqlMustExec(t, `SELECT * FROM public.schema_migrations;`)

	// If we specify a table, it should create public.<table_name>
	resetDB(t)
	config.MigrationTable = "applied_migrations"

	if err := Initialize(config); err != nil {
		t.Fatal("Initialize failed: ", err)
	}

	psqlMustExec(t, `SELECT * FROM public.applied_migrations;`)

	// If we specify a schema-qualified table, the schema should be
	// created if it does not yet exist.
	resetDB(t)
	config.MigrationTable = "pgmgr.applied_migrations"
	if err := Initialize(config); err != nil {
		t.Fatal("Initialize failed: ", err)
	}

	psqlMustExec(t, `SELECT * FROM pgmgr.applied_migrations`)

	// If we specify a schema-qualified table, and the schema already existed,
	// that's fine too.
	resetDB(t)
	psqlMustExec(t, `CREATE SCHEMA pgmgr;`)
	if err := Initialize(config); err != nil {
		t.Fatal("Initialize failed: ", err)
	}

	psqlMustExec(t, `SELECT * FROM pgmgr.applied_migrations`)
}

func TestVersion(t *testing.T) {
	resetDB(t)

	version, err := Version(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not fetch version info")
	}

	if version != -1 {
		t.Fatal("expected version to be -1 before table exists, got", version)
	}

	Initialize(globalConfig())
	psqlMustExec(t, `INSERT INTO schema_migrations (version) VALUES (1);`)

	version, err = Version(globalConfig())
	if version != 1 {
		t.Fatal("expected version to be 1, got", version)
	}
}

func TestColumnTypeString(t *testing.T) {
	resetDB(t)

	config := globalConfig()
	config.ColumnType = "string"
	Initialize(config)

	psqlMustExec(t, `INSERT INTO schema_migrations (version) VALUES ('20150910120933');`)
	version, err := Version(config)
	if err != nil {
		t.Fatal(err)
	}

	if version != 20150910120933 {
		t.Fatal("expected version to be 20150910120933, got", version)
	}
}

func TestMigrate(t *testing.T) {
	resetDB(t)
	clearMigrationFolder(t)

	// add our first migration
	writeMigration(t, "002_this_is_a_migration.up.sql", `
		CREATE TABLE foos (foo_id INTEGER);
		INSERT INTO foos (foo_id) VALUES (1), (2), (3);
	`)

	writeMigration(t, "002_this_is_a_migration.down.sql", `DROP TABLE foos;`)

	err := Migrate(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Migrations failed to run.")
	}

	// test simple idempotency
	err = Migrate(globalConfig())
	if err != nil {
		t.Log(err)
		t.Fatal("Running migrations again was not idempotent!")
	}

	psqlMustExec(t, `SELECT * FROM foos;`)

	// add a new migration with an older version, as if another dev's branch was merged in
	writeMigration(t, "001_this_is_an_older_migration.up.sql", `
		CREATE TABLE bars (bar_id INTEGER);
		INSERT INTO bars (bar_id) VALUES (4), (5), (6);
	`)

	err = Migrate(globalConfig())
	if err != nil {
		t.Log(err)
		t.Fatal("Could not apply second migration!")
	}

	psqlMustExec(t, `SELECT * FROM bars;`)

	// rollback the initial migration, since it has the latest version
	err = Rollback(globalConfig())

	if err := psqlExec(t, `SELECT * FROM foos;`); err == nil {
		t.Fatal("Should not have been able to select from foos table")
	}

	v, err := Version(globalConfig())
	if err != nil || v != 1 {
		t.Log(err)
		t.Fatal("Rollback did not reset version! Still on version ", v)
	}
}

func TestMigrateColumnTypeString(t *testing.T) {
	resetDB(t)
	clearMigrationFolder(t)

	config := globalConfig()
	config.ColumnType = "string"

	// migrate up
	writeMigration(t, "20150910120933_some_migration.up.sql", `
		CREATE TABLE foos (foo_id INTEGER);
		INSERT INTO foos (foo_id) VALUES (1), (2), (3);
	`)

	err := Migrate(config)
	if err != nil {
		t.Fatal(err)
	}

	v, err := Version(config)
	if err != nil {
		t.Fatal(err)
	}

	if v != 20150910120933 {
		t.Fatal("Expected version 20150910120933 after migration, got", v)
	}

	// migrate down
	writeMigration(t, "20150910120933_some_migration.down.sql", `DROP TABLE foos;`)

	err = Rollback(config)
	if err != nil {
		t.Fatal(err)
	}

	v, err = Version(config)
	if err != nil {
		t.Fatal(err)
	}

	if v != -1 {
		t.Fatal("Expected version -1 after rollback, got", v)
	}
}

func TestMigrateNoTransaction(t *testing.T) {
	resetDB(t)
	clearMigrationFolder(t)

	// CREATE INDEX CONCURRENTLY can not run inside a transaction, so we can assert
	// that no transaction was used by verifying it ran successfully.
	writeMigration(t, "001_create_foos.up.sql", `CREATE TABLE foos (foo_id INTEGER);`)
	writeMigration(t, "002_index_foos.no_txn.up.sql", `CREATE INDEX CONCURRENTLY idx_foo_id ON foos(foo_id);`)

	err := Migrate(globalConfig())
	if err != nil {
		t.Fatal(err)
	}
}

func TestMigrateCustomMigrationTable(t *testing.T) {
	resetDB(t)
	clearMigrationFolder(t)
	writeMigration(t, "001_create_foos.up.sql", `CREATE TABLE foos (foo_id INTEGER);`)

	config := globalConfig()
	config.MigrationTable = "pgmgr.migrations"
	if err := Migrate(config); err != nil {
		t.Fatal(err)
	}

	v, err := Version(config)
	if err != nil {
		t.Fatal(err)
	}

	if v != 1 {
		t.Fatal("Expected version 1, got ", v)
	}
}

func TestCreateMigration(t *testing.T) {
	clearMigrationFolder(t)

	assertFileExists := func(filename string) {
		err := testSh(t, "stat", []string{filepath.Join(migrationFolder, filename)})
		if err != nil {
			t.Fatal(err)
		}
	}

	expectedVersion := time.Now().Unix()
	err := CreateMigration(globalConfig(), "new_migration", false)
	if err != nil {
		t.Fatal(err)
	}

	assertFileExists(fmt.Sprint(expectedVersion, "_new_migration.up.sql"))
	assertFileExists(fmt.Sprint(expectedVersion, "_new_migration.down.sql"))

	expectedStringVersion := time.Now().Format(datetimeFormat)
	config := globalConfig()
	config.Format = "datetime"
	err = CreateMigration(config, "rails_style", false)
	if err != nil {
		t.Fatal(err)
	}

	assertFileExists(fmt.Sprint(expectedStringVersion, "_rails_style.up.sql"))
	assertFileExists(fmt.Sprint(expectedStringVersion, "_rails_style.down.sql"))

	err = CreateMigration(config, "create_index", true)
	if err != nil {
		t.Fatal(err)
	}

	assertFileExists(fmt.Sprint(expectedStringVersion, "_create_index.no_txn.up.sql"))
	assertFileExists(fmt.Sprint(expectedStringVersion, "_create_index.no_txn.down.sql"))
}

// redundant, but I'm also lazy
func testSh(t *testing.T, command string, args []string) error {
	c := exec.Command(command, args...)
	output, err := c.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		return err
	}

	return nil
}

func psqlExec(t *testing.T, statement string) error {
	return testSh(t, "psql", []string{"-d", testDBName, "-c", statement})
}

func psqlMustExec(t *testing.T, statement string) {
	err := psqlExec(t, statement)
	if err != nil {
		t.Fatalf("Failed to execute statement: '%s': %s", statement, err)
	}
}

func resetDB(t *testing.T) {
	if err := dropDB(t); err != nil {
		t.Fatal("dropdb failed: ", err)
	}

	if err := createDB(t); err != nil {
		t.Fatal("createdb failed: ", err)
	}
}

func dropDB(t *testing.T) error {
	return testSh(t, "dropdb", []string{testDBName})
}

func createDB(t *testing.T) error {
	return testSh(t, "createdb", []string{testDBName})
}

func clearMigrationFolder(t *testing.T) {
	testSh(t, "rm", []string{"-r", migrationFolder})
	testSh(t, "mkdir", []string{migrationFolder})
}

func writeMigration(t *testing.T, name, contents string) {
	filename := path.Join(migrationFolder, name)
	err := ioutil.WriteFile(filename, []byte(contents), 0644)
	if err != nil {
		t.Fatalf("Failed to write %s: %s", filename, err)
	}
}
