package pgmgr

import (
	"fmt"
	"strconv"
	"os/exec"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"database/sql"
	_ "github.com/lib/pq"
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
	migrations, err := migrations(c, "up")
	if err != nil {
		return err
	}

	currentVersion, err := getOrInitializeVersion(c)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if m.Version > currentVersion {
			err = sh("psql", []string{"-d", c.Database, "-f", filepath.Join(c.MigrationFolder, m.Filename)})
			if err != nil { // halt the migration process and return the error.
				return err
			}
		}
	}

	return nil
}

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

func Version(c *Config) (int, error) {
	db, err := openConnection(c)
	if err != nil {
		return -1, err
	}

	// if the table doesn't exist, we're simply at version zero
	hasTable := false
	err = db.QueryRow("SELECT true FROM pg_catalog.pg_tables WHERE tablename='schema_migrations'").Scan(&hasTable)
	if hasTable == false {
		return 0, nil
  }

	// if the query fails, return zero. probably means the table is empty
	version := 0
	db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version)

	return version, nil
}

func Initialize(c *Config) error {
	db, err := openConnection(c)
	if err != nil {
		return err
	}

  err = db.QueryRow("CREATE TABLE schema_migrations (version INTEGER NOT NULL)").Scan()
	if err != nil {
		return err
	}

	return nil
}

func getOrInitializeVersion(c *Config) (int, error) {
	var v int
	if v, _ := Version(c); v == 0 {
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
