package pgmgr

import (
	"testing"
	"../pgmgr"
	"os/exec"
	"io/ioutil"
	"strings"
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

	err := pgmgr.Dump(globalConfig())

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

func TestMigrate(t *testing.T) {
	sh(t, "dropdb", []string{"testdb"})
	sh(t, "createdb", []string{"testdb"})
	sh(t, "mkdir", []string{"/tmp/migrations"})

	ioutil.WriteFile("/tmp/migrations/001_this_is_a_migration.up.sql", []byte(`
		CREATE TABLE foos (foo_id INTEGER);
		INSERT INTO foos (foo_id) VALUES (1), (2), (3);
	`), 0644)

	ioutil.WriteFile("/tmp/migrations/001_this_is_a_migration.down.sql", []byte(`
		DROP TABLE foos;
	`), 0644)

	err := pgmgr.Migrate(globalConfig())

	if err != nil {
		t.Log(err)
		t.Fatal("Migrations failed to run.")
	}

	err = sh(t, "psql", []string{"-d", "testdb", "-c","SELECT * FROM foos;"})
	if err != nil {
		t.Log(err)
		t.Fatal("Could not query the table; migration didn't apply, probably")
	}

	err = pgmgr.Rollback(globalConfig())

	err = sh(t, "psql", []string{"-d", "testdb", "-c","SELECT * FROM foos;"})
	if err == nil {
		t.Log(err)
		t.Fatal("Could query the table; migration didn't downgrade")
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
