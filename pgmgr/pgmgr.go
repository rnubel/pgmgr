package pgmgr

import (
	"fmt"
	"os/exec"
)

type Config struct {
	// connection
	Username	string
	Password	string
	Database  string
	Host			string
	Port			int

	// filepaths
	DumpFile  string
}

func Create(c *Config) error {
	return sh("createdb", []string{c.Database})
}

func Drop(c *Config) error {
	return sh("dropdb", []string{c.Database})
}

func Dump(c *Config) error {
	return sh("pg_dump", []string{"-f", c.DumpFile})
}

func Load(c *Config) error {
	return sh("psql", []string{"-f", c.DumpFile})
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
