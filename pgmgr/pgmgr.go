package pgmgr

import (
	"fmt"
	"errors"
	"strconv"
	"os/exec"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"database/sql"
	"github.com/lib/pq"
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

type Migration struct {
	Filename string
	Version	int
}

// Creates the database specified by the configuration.
func Create(c *Config) error {
	return sh("createdb", []string{c.Database})
}

// Drops the database specified by the configuration.
func Drop(c *Config) error {
	return sh("dropdb", []string{c.Database})
}

// Dumps the schema and contents of the database to the dump file.
func Dump(c *Config) error {
	return sh("pg_dump", []string{"-f", c.DumpFile, c.Database})
}

// Loads the database from the dump file.
func Load(c *Config) error {
	return sh("psql", []string{"-d", c.Database, "-f", c.DumpFile})
}

// Applies un-applied migrations in the specified MigrationFolder.
func Migrate(c *Config) error {
	migrations, err := migrations(c, "up")
	if err != nil {
		return err
	}

	// ensure the version table is created
	_, err = getOrInitializeVersion(c)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if applied, _ := migrationIsApplied(c, m.Version); !applied {
			if err = applyMigration(c, m); err != nil { // halt the migration process and return the error.
				return err
			}
		}
	}

	return nil
}

// Un-applies the latest migration, if possible.
func Rollback(c *Config) error {
	migrations, err := migrations(c, "down")
	if err != nil {
		return err
	}

	v, _ := Version(c)
	to_rollback := Migration{}
	for _, m := range migrations {
		if m.Version == v {
			to_rollback = m
			break
		}
	}

	if to_rollback == (Migration{}) {
		return nil
	}

	// rollback only the last migration
	err = sh("psql", []string{"-d", c.Database,
													  "-f", filepath.Join(c.MigrationFolder, to_rollback.Filename)})
	if err != nil {
		return err
	}

	return nil
}

// Returns the highest version number stored in the database. This is not
// necessarily enough info to uniquely identify the version, since there may
// be backdated migrations which have not yet applied.
func Version(c *Config) (int, error) {
	db, err := openConnection(c)
	defer db.Close()

	if err != nil {
		return -1, err
	}

	// if the table doesn't exist, we're at version -1
	var hasTable bool
	err = db.QueryRow("SELECT true FROM pg_catalog.pg_tables WHERE tablename='schema_migrations'").Scan(&hasTable)
	if hasTable != true {
		return -1, nil
  }

	var version int
	err = db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version)

	return version, err
}

// Creates the schema_migrations table on what should be a new database.
func Initialize(c *Config) error {
	db, err := openConnection(c)
	defer db.Close()
	if err != nil {
		return err
	}

  _, err = db.Exec("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
	if err != nil {
		return err
	}

	return nil
}

func applyMigration(c *Config, m Migration) error {
	db, err := openConnection(c)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	contents, err := ioutil.ReadFile(filepath.Join(c.MigrationFolder, m.Filename))
	if err != nil {
		return err
	}

	if _, err = tx.Exec(string(contents)); err != nil {
		pgerr := err.(*pq.Error)

		// trigger a rollback, if needed.
		tx.Rollback()
		return errors.New(fmt.Sprint("Error when applying migration ", m.Version, ": line ", pgerr.Line, " pos ", pgerr.Position, ": ", pgerr.Message, ". ", pgerr.Detail))
	}

	if err = insertSchemaVersion(tx, m.Version); err != nil {
		pgerr := err.(*pq.Error)

		// trigger a rollback, if needed.
		tx.Rollback()
		return errors.New(fmt.Sprint("Error when inserting version ", m.Version, ": line ", pgerr.Line, " pos ", pgerr.Position, ": ", pgerr.Message, ". ", pgerr.Detail))
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func insertSchemaVersion(tx *sql.Tx, version int) error {
	_, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES ($1) RETURNING version", version)
	return err
}

func migrationIsApplied(c *Config, version int) (bool, error) {
	db, err := openConnection(c)
	defer db.Close()
	if err != nil {
		return false, err
	}

	var is_applied bool
  err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1);", version).Scan(&is_applied)
	if err != nil {
		return false, err
	}

	return is_applied, nil
}

func getOrInitializeVersion(c *Config) (int, error) {
	var v int
	if v, _ = Version(c); v < 0 {
		if err := Initialize(c); err != nil {
			return -1, err
		}
	}

	return v, nil
}

func openConnection(c *Config) (*sql.DB, error) {
	db, err := sql.Open("postgres", sqlConnectionString(c))
	return db, err
}

func sqlConnectionString(c * Config) string {
	return fmt.Sprint(
		" user='"			, c.Username, "'",
		" dbname='"		, c.Database, "'",
		" password='"	, c.Password, "'",
		" host='"			, c.Host, "'",
		" sslmode="		, "disable")
}

func migrations(c *Config, direction string) ([]Migration, error) {
	files, err := ioutil.ReadDir(c.MigrationFolder)
	migrations := []Migration{}
	if err != nil {
		return []Migration{}, err
	}

	for _, file := range files {
		if match, _ := regexp.MatchString("[0-9]+_.+." + direction + ".sql", file.Name()); match {
			re := regexp.MustCompile("^[0-9]+")
			version, _ := strconv.Atoi(re.FindString(file.Name()))
			migrations = append(migrations, Migration{Filename: file.Name(), Version: version})
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
