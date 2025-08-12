package pgmgr

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorIsLockingError(t *testing.T) {
	cases := [](struct {
		ErrStr         string
		ExpectedResult bool
	}){
		{"ERROR:  current transaction is aborted, commands ignored until end of transaction block", false},
		{"ERROR:  canceling statement due to lock timeout\n", true},
		{"ERROR:  canceling statement due to statement timeout \r\n", true},
		{"ERROR:  could not obtain lock on relation \r\n", true},
		{"WARNING: could not obtain lock on relation", false},
	}

	for _, c := range cases {
		t.Run(c.ErrStr, func(t *testing.T) {
			if ErrorIsLockingError(errors.New(c.ErrStr)) != c.ExpectedResult {
				t.Error("failed on: ", c.ErrStr, "; should have returned", c.ExpectedResult)
			}
		})
	}
}

func TestRetryUntilNonLockingError(t *testing.T) {
	var runTestCase = func(stopAt int) bool {
		retried := 0
		succeeded := false

		RetryUntilNonLockingError(func() error {
			retried++

			if retried == stopAt {
				succeeded = true
				return nil
			}

			return errors.New("ERROR: could not obtain lock on relation")
		}, 0, 10)

		return succeeded
	}

	cases := [](struct {
		numTriesToSucceed int
		shouldSucceed     bool
	}){
		{1, true},
		{10, true},
		{11, true}, // the config says "10 retries", so 11 tries total is okay
		{12, false},
		{-1, false}, // don't cause an infinite loop, eh?
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("a patch that applies on try %d", c.numTriesToSucceed), func(t *testing.T) {
			if runTestCase(c.numTriesToSucceed) != c.shouldSucceed {
				t.Error("result did not match expected result: ", c.shouldSucceed)
			}
		})
	}
}
