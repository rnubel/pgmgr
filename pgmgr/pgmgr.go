package pgmgr

import (
	"fmt"
	"os/exec"
	"io/ioutil"
	"path/filepath"
	"regexp"
)

type Config struct {
	// connection
	Username	string
	Password	string
	Database	string
	Host			string
	Port			int

	// filepaths
	DumpFile	string
	MigrationFolder	string
}

func Create(c *Config) error {
	return sh("createdb", []string{c.Database})
}

func Drop(c *Config) error {
	return sh("dropdb", []string{c.Database})
}

func Dump(c *Config) error {
	return sh("pg_dump", []string{"-f", c.DumpFile, c.Database})
}

func Load(c *Config) error {
	return sh("psql", []string{"-d", c.Database, "-f", c.DumpFile})
}

func Migrate(c *Config) error {
	files, err := migrationFiles(c, "up")
	if err != nil {
		return err
	}

	for _, file := range files {
		err = sh("psql", []string{"-d", c.Database, "-f", filepath.Join(c.MigrationFolder, file)})
		if err != nil { // halt the migration process and return the error.
			return err
		}
	}

	return nil
}

func Rollback(c *Config) error {
	// TODO: we should probably rollback the latest version, not just the last .down file.
	files, err := migrationFiles(c, "down")
	if err != nil {
		return err
	}

	// rollback only the last migration
	to_rollback := files[len(files) - 1]
	err = sh("psql", []string{"-d", c.Database, "-f", filepath.Join(c.MigrationFolder, to_rollback)})
	if err != nil {
		return err
	}

	return nil
}

func migrationFiles(c *Config, direction string) ([]string, error) {
	files, err := ioutil.ReadDir(c.MigrationFolder)
	migrations := []string{}
	if err != nil {
		return []string{}, err
	}

	for _, file := range files {
		if match, _ := regexp.MatchString("[0-9]+_.+." + direction + ".sql", file.Name()); match {
			migrations = append(migrations, file.Name())
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
