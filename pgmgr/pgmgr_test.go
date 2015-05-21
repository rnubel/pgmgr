package pgmgr

import (
	"testing"
	"../pgmgr"
	"os/exec"
)

func TestCreate(t *testing.T) {
	sh(t, "dropdb", []string{"testdb"})
	err := pgmgr.Create("testdb")

	if err != nil {
		t.Log(err)
		t.Fatal("Could not create database")
	}

	// if we can't remove that db, it couldn't have been created by us above.
	if err = sh(t, "dropdb", []string{"testdb"}); err != nil {
		t.Fatal("database doesn't seem to have been created!")
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
