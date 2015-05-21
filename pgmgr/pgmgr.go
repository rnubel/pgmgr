package pgmgr

import (
	"fmt"
	"os/exec"
)

func Create(dbname string) error {
	return sh("createdb", []string{dbname})
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
