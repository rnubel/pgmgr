package pgmgr

import (
	"fmt"
	"regexp"
	"time"
)

// LockConfig encapsulates a few settings related to controlling behavior
// related to locks when applying migrations. The default values should be
// appropriate for almost all use cases, though for certain workloads you may
// need to customize them. To do so within a migration, just use normal
// postgres SET statements:
//    SET statement_timeout TO 10000;
//    SET lock_timeout TO 1000;
// Or, add a lock-config key to your pgmgr.json.
type LockConfig struct {
	// StatementTimeout is how long each statement in the migration has to
	// complete, in milliseconds.
	StatementTimeout int

	// LockTimeout is how long each statement will wait to acquire its needed
	// lock, in milliseconds. Must be less than StatementTimeout.
	LockTimeout int

	// MaxRetries is how many times pgmgr will re-try your migration when the
	// above timeouts error out. It will not retry for other failures. Set to
	// -1 if you would like to disable retries.
	MaxRetries int

	// RetryDelay is how many seconds pgmgr will sleep between retries. Set to
	// -1 to have no delay (not recommended).
	RetryDelay int
}

// applyDefaults overwrites 0 values to default values
func (l *LockConfig) applyDefaults() *LockConfig {
	if l.StatementTimeout == 0 {
		l.StatementTimeout = 1000
	}

	if l.LockTimeout == 0 {
		l.LockTimeout = 200
	}

	if l.MaxRetries == 0 {
		l.MaxRetries = 10
	}

	if l.RetryDelay == 0 {
		l.RetryDelay = 5
	}

	return l
}

const lockingErrorPattern = "ERROR:.+(canceling statement due to (statement|lock) timeout|could not obtain lock on relation)"

// ErrorIsLockingError returns true if the given postgres error is due to locking
func ErrorIsLockingError(err error) bool {
	return regexp.MustCompile(lockingErrorPattern).MatchString(err.Error())
}

// RetryUntilNonLockingError will run the given block until the error it returns
// is either nil or no longer a locking error at most `retriesRemaining` + 1 times.
func RetryUntilNonLockingError(block func() error, retryDelay int, retriesRemaining int) error {
	err := block()

	switch {
	case err == nil:
		return nil
	case ErrorIsLockingError(err) && retriesRemaining <= 0:
		return fmt.Errorf("retries exceeded: %w", err)
	case ErrorIsLockingError(err):
		if retryDelay > 0 {
			fmt.Printf("retrying in %d seconds (%d retries remaining) after rescuing from lock-related error: %s\n", retryDelay, retriesRemaining, err.Error())
			time.Sleep(time.Duration(retryDelay) * time.Second)
		} else {
			fmt.Printf("retrying immediately (%d retries remaining) after rescuing from lock-related error: %s\n", retriesRemaining, err.Error())
		}
		return RetryUntilNonLockingError(block, retryDelay, retriesRemaining-1)
	default:
		return err
	}
}
