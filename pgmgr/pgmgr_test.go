package pgmgr

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"../pgmgr"
)

func globalConfig() *pgmgr.Config {
	return &pgmgr.Config{
		Database:        "testdb",
		Host:            "localhost",
		Port:            5432,
		DumpFile:        "/tmp/dump.sql",
		MigrationFolder: "/tmp/migrations/",
	}
}

func TestCreate(t *testing.T) {
	testSh(t, "dropdb", []string{"testdb"})
	err := pgmgr.Create(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not create database")
	}

	// if we can't remove that db, it couldn't have been created by us above.
	if err = testSh(t, "dropdb", []string{"testdb"}); err != nil {
		t.Fatal("database doesn't seem to have been created!")
	}
}

func TestDrop(t *testing.T) {
	testSh(t, "createdb", []string{"testdb"})
	err := pgmgr.Drop(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not drop database")
	}

	if err = testSh(t, "createdb", []string{"testdb"}); err != nil {
		t.Fatal("database doesn't seem to have been dropped!")
	}
}

func TestDump(t *testing.T) {
	testSh(t, "dropdb", []string{"testdb"})
	testSh(t, "createdb", []string{"testdb"})
	testSh(t, "psql", []string{"-d", "testdb", "-c", "CREATE TABLE bars (bar_id INTEGER);"})
	testSh(t, "psql", []string{"-d", "testdb", "-c", "INSERT INTO bars (bar_id) VALUES (123), (456);"})
	testSh(t, "psql", []string{"-d", "testdb", "-c", "CREATE TABLE foos (foo_id INTEGER);"})
	testSh(t, "psql", []string{"-d", "testdb", "-c", "INSERT INTO foos (foo_id) VALUES (789);"})

	c := globalConfig()
	err := pgmgr.Dump(c)

	if err != nil {
		t.Log(err)
		t.Fatal("Could not dump database to file")
	}

	file, err := ioutil.ReadFile("/tmp/dump.sql")
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
	err = pgmgr.Dump(c)

	if err != nil {
		t.Log(err)
		t.Fatal("Could not dump database to file")
	}

	file, err = ioutil.ReadFile("/tmp/dump.sql")
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
	testSh(t, "dropdb", []string{"testdb"})
	testSh(t, "createdb", []string{"testdb"})

	ioutil.WriteFile("/tmp/dump.sql", []byte(`
		CREATE TABLE foos (foo_id INTEGER);
		INSERT INTO foos (foo_id) VALUES (1), (2), (3);
	`), 0644)

	err := pgmgr.Load(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not load database from file")
	}

	err = testSh(t, "psql", []string{"-d", "testdb", "-c", "SELECT * FROM foos;"})
	if err != nil {
		t.Log(err)
		t.Fatal("Could not query the table; schema didn't load, probably")
	}
}

func TestVersion(t *testing.T) {
	testSh(t, "dropdb", []string{"testdb"})
	testSh(t, "createdb", []string{"testdb"})

	version, err := pgmgr.Version(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not fetch version info")
	}

	if version != -1 {
		t.Fatal("expected version to be -1 before table exists, got", version)
	}

	pgmgr.Initialize(globalConfig())

	testSh(t, "psql", []string{"-e", "-d", "testdb", "-c", "INSERT INTO schema_migrations (version) VALUES (1)"})

	version, err = pgmgr.Version(globalConfig())

	if version != 1 {
		t.Fatal("expected version to be 1, got", version)
	}
}

func TestColumnTypeString(t *testing.T) {
	testSh(t, "dropdb", []string{"testdb"})
	testSh(t, "createdb", []string{"testdb"})

	config := globalConfig()
	config.ColumnType = "string"
	pgmgr.Initialize(config)

	testSh(t, "psql", []string{"-e", "-d", "testdb", "-c", "INSERT INTO schema_migrations (version) VALUES ('20150910120933')"})
	version, err := pgmgr.Version(config)
	if err != nil {
		t.Fatal(err)
	}

	if version != 20150910120933 {
		t.Fatal("expected version to be 20150910120933, got", version)
	}
}

func TestMigrate(t *testing.T) {
	// start with an empty DB
	testSh(t, "dropdb", []string{"testdb"})
	testSh(t, "createdb", []string{"testdb"})
	testSh(t, "rm", []string{"-r", "/tmp/migrations"})
	testSh(t, "mkdir", []string{"/tmp/migrations"})

	// add our first migration
	ioutil.WriteFile("/tmp/migrations/002_this_is_a_migration.up.sql", []byte(`
		CREATE TABLE foos (foo_id INTEGER);
		INSERT INTO foos (foo_id) VALUES (1), (2), (3);
	`), 0644)

	ioutil.WriteFile("/tmp/migrations/002_this_is_a_migration.down.sql", []byte(`
		DROP TABLE foos;
	`), 0644)

	err := pgmgr.Migrate(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Migrations failed to run.")
	}

	// test simple idempotency
	err = pgmgr.Migrate(globalConfig())
	if err != nil {
		t.Log(err)
		t.Fatal("Running migrations again was not idempotent!")
	}

	err = testSh(t, "psql", []string{"-d", "testdb", "-c", "SELECT * FROM foos;"})
	if err != nil {
		t.Log(err)
		t.Fatal("Could not query the table; migration didn't apply, probably")
	}

	// add a new migration with an older version, as if another dev's branch was merged in
	ioutil.WriteFile("/tmp/migrations/001_this_is_an_older_migration.up.sql", []byte(`
		CREATE TABLE bars (bar_id INTEGER);
		INSERT INTO bars (bar_id) VALUES (4), (5), (6);
	`), 0644)

	err = pgmgr.Migrate(globalConfig())
	if err != nil {
		t.Log(err)
		t.Fatal("Could not apply second migration!")
	}

	err = testSh(t, "psql", []string{"-d", "testdb", "-c", "SELECT * FROM bars;"})
	if err != nil {
		t.Log(err)
		t.Fatal("Could not query the table; second migration didn't apply, probably")
	}

	// rollback the initial migration, since it has the latest version
	err = pgmgr.Rollback(globalConfig())

	err = testSh(t, "psql", []string{"-d", "testdb", "-c", "SELECT * FROM foos;"})
	if err == nil {
		t.Log(err)
		t.Fatal("Could query the table; migration didn't downgrade")
	}

	v, err := pgmgr.Version(globalConfig())
	if err != nil || v != 1 {
		t.Log(err)
		t.Fatal("Rollback did not reset version! Still on version ", v)
	}
}

func TestMigrateColumnTypeString(t *testing.T) {
	// start with an empty DB
	testSh(t, "dropdb", []string{"testdb"})
	testSh(t, "createdb", []string{"testdb"})
	testSh(t, "rm", []string{"-r", "/tmp/migrations"})
	testSh(t, "mkdir", []string{"/tmp/migrations"})

	config := globalConfig()
	config.ColumnType = "string"

	// migrate up
	ioutil.WriteFile("/tmp/migrations/20150910120933_some_migration.up.sql", []byte(`
		CREATE TABLE foos (foo_id INTEGER);
		INSERT INTO foos (foo_id) VALUES (1), (2), (3);
	`), 0644)

	err := pgmgr.Migrate(config)
	if err != nil {
		t.Fatal(err)
	}

	v, err := pgmgr.Version(config)
	if err != nil {
		t.Fatal(err)
	}

	if v != 20150910120933 {
		t.Fatal("Expected version 20150910120933 after migration, got", v)
	}

	// migrate down
	ioutil.WriteFile("/tmp/migrations/20150910120933_some_migration.down.sql", []byte(`
        DROP TABLE foos;
	`), 0644)

	err = pgmgr.Rollback(config)
	if err != nil {
		t.Fatal(err)
	}

	v, err = pgmgr.Version(config)
	if err != nil {
		t.Fatal(err)
	}

	if v != -1 {
		t.Fatal("Expected version -1 after rollback, got", v)
	}
}

func TestMigrateNoTransaction(t *testing.T) {
	// start with an empty DB
	testSh(t, "dropdb", []string{"testdb"})
	testSh(t, "createdb", []string{"testdb"})
	testSh(t, "rm", []string{"-r", "/tmp/migrations"})
	testSh(t, "mkdir", []string{"/tmp/migrations"})

	// CREATE INDEX CONCURRENTLY can not run inside a transaction, so we can assert
	// that no transaction was used by verifying it ran successfully.
	ioutil.WriteFile("/tmp/migrations/001_create_foos.up.sql", []byte(`
		CREATE TABLE foos (foo_id INTEGER);
	`), 0644)

	ioutil.WriteFile("/tmp/migrations/002_index_foos.no_txn.up.sql", []byte(`
		CREATE INDEX CONCURRENTLY idx_foo_id ON foos(foo_id);
	`), 0644)

	err := pgmgr.Migrate(globalConfig())
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateMigration(t *testing.T) {
	testSh(t, "rm", []string{"-r", "/tmp/migrations"})
	testSh(t, "mkdir", []string{"/tmp/migrations"})

	assertFileExists := func(filename string) {
		err := testSh(t, "stat", []string{filepath.Join("/tmp/migrations", filename)})
		if err != nil {
			t.Fatal(err)
		}
	}

	expectedVersion := time.Now().Unix()
	err := pgmgr.CreateMigration(globalConfig(), "new_migration", false)
	if err != nil {
		t.Fatal(err)
	}

	assertFileExists(fmt.Sprint(expectedVersion, "_new_migration.up.sql"))
	assertFileExists(fmt.Sprint(expectedVersion, "_new_migration.down.sql"))

	expectedStringVersion := time.Now().Format(datetimeFormat)
	config := globalConfig()
	config.Format = "datetime"
	err = pgmgr.CreateMigration(config, "rails_style", false)
	if err != nil {
		t.Fatal(err)
	}

	assertFileExists(fmt.Sprint(expectedStringVersion, "_rails_style.up.sql"))
	assertFileExists(fmt.Sprint(expectedStringVersion, "_rails_style.down.sql"))

	err = pgmgr.CreateMigration(config, "create_index", true)
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
