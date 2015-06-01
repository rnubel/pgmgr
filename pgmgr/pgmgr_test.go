package pgmgr

import (
	"testing"
	"../pgmgr"
	"os/exec"
	"io/ioutil"
	"strings"
	"time"
	"fmt"
)

func globalConfig() *pgmgr.Config {
	return &pgmgr.Config{
		Database: "testdb",
		Host:			"localhost",
		Port:			5432,
		DumpFile: "/tmp/dump.sql",
		MigrationFolder: "/tmp/migrations/",
	}
}

func TestCreate(t *testing.T) {
	sh(t, "dropdb", []string{"testdb"})
	err := pgmgr.Create(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not create database")
	}

	// if we can't remove that db, it couldn't have been created by us above.
	if err = sh(t, "dropdb", []string{"testdb"}); err != nil {
		t.Fatal("database doesn't seem to have been created!")
	}
}

func TestDrop(t *testing.T) {
	sh(t, "createdb", []string{"testdb"})
	err := pgmgr.Drop(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not drop database")
	}

	if err = sh(t, "createdb", []string{"testdb"}); err != nil {
		t.Fatal("database doesn't seem to have been dropped!")
	}
}

func TestDump(t *testing.T) {
	sh(t, "dropdb", []string{"testdb"})
	sh(t, "createdb", []string{"testdb"})
	sh(t, "psql", []string{"-d", "testdb", "-c","CREATE TABLE bars (bar_id INTEGER);"})
	sh(t, "psql", []string{"-d", "testdb", "-c","INSERT INTO bars (bar_id) VALUES (123), (456);"})
	sh(t, "psql", []string{"-d", "testdb", "-c","CREATE TABLE foos (foo_id INTEGER);"})
	sh(t, "psql", []string{"-d", "testdb", "-c","INSERT INTO foos (foo_id) VALUES (789);"})

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
	sh(t, "dropdb", []string{"testdb"})
	sh(t, "createdb", []string{"testdb"})

	ioutil.WriteFile("/tmp/dump.sql", []byte(`
		CREATE TABLE foos (foo_id INTEGER);
		INSERT INTO foos (foo_id) VALUES (1), (2), (3);
	`), 0644)

	err := pgmgr.Load(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not load database from file")
	}

	err = sh(t, "psql", []string{"-d", "testdb", "-c","SELECT * FROM foos;"})
	if err != nil {
		t.Log(err)
		t.Fatal("Could not query the table; schema didn't load, probably")
	}
}

func TestVersion(t *testing.T) {
	sh(t, "dropdb", []string{"testdb"})
	sh(t, "createdb", []string{"testdb"})

	version, err := pgmgr.Version(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Could not fetch version info")
	}

	if version != -1 {
		t.Fatal("expected version to be -1 before table exists, got", version)
	}

	pgmgr.Initialize(globalConfig())

	sh(t, "psql", []string{"-e", "-d", "testdb", "-c", "INSERT INTO schema_migrations (version) VALUES (1)"})

	version, err = pgmgr.Version(globalConfig())

	if version != 1 {
		t.Fatal("expected version to be 1, got", version)
	}
}

func TestMigrate(t *testing.T) {
	// start with an empty DB
	sh(t, "dropdb", []string{"testdb"})
	sh(t, "createdb", []string{"testdb"})
	sh(t, "rm", []string{"-r", "/tmp/migrations"})
	sh(t, "mkdir", []string{"/tmp/migrations"})

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

	err = sh(t, "psql", []string{"-d", "testdb", "-c","SELECT * FROM foos;"})
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

	err = sh(t, "psql", []string{"-d", "testdb", "-c","SELECT * FROM bars;"})
	if err != nil {
		t.Log(err)
		t.Fatal("Could not query the table; second migration didn't apply, probably")
	}

	// rollback the initial migration, since it has the latest version
	err = pgmgr.Rollback(globalConfig())

	err = sh(t, "psql", []string{"-d", "testdb", "-c","SELECT * FROM foos;"})
	if err == nil {
		t.Log(err)
		t.Fatal("Could query the table; migration didn't downgrade")
	}
}

func TestCreateMigration(t *testing.T) {
	sh(t, "rm", []string{"-r", "/tmp/migrations"})
	sh(t, "mkdir", []string{"/tmp/migrations"})

	expectedVersion := time.Now().Format("2006010215150405")
	err := pgmgr.CreateMigration(globalConfig(), "new_migration")
	if err != nil {
		t.Fatal(err)
	}

	err = sh(t, "stat", []string{fmt.Sprint("/tmp/migrations/", expectedVersion, "_new_migration.up.sql")})
	if err != nil {
		t.Fatal(err)
	}

	err = sh(t, "stat", []string{fmt.Sprint("/tmp/migrations/", expectedVersion, "_new_migration.down.sql")})
	if err != nil {
		t.Fatal(err)
	}
}

// redundant, but I'm also lazy
func sh(t *testing.T, command string, args []string) error {
	c := exec.Command(command, args...)
	output, err := c.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		return err
	}

	return nil
}
