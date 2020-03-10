package pgmgr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

// Something that stores key-value pairs of various types,
// e.g., cli.Context.
type argumentContext interface {
	String(string) string
	Int(string) int
	StringSlice(string) []string
}

// Config stores the options used by pgmgr.
type Config struct {
	// connection
	Username string
	Password string
	Database string
	Host     string
	Port     int
	URL      string
	SslMode  string

	// dump
	DumpConfig DumpConfig `json:"dump-options"`

	// filepaths
	MigrationFolder string `json:"migration-folder"`

	// options
	MigrationTable  string `json:"migration-table"`
	MigrationDriver string `json:"migration-driver"`
	ColumnType      string `json:"column-type"`
	Format          string

	// deprecated -- see dump_config.go
	DumpFile   string   `json:"dump-file"`
	SeedTables []string `json:"seed-tables"`
}

// LoadConfig reads the config file, applies CLI arguments as
// overrides, and returns an error if the configuration is invalid.
func LoadConfig(config *Config, ctx argumentContext) error {
	// load configuration from file first; then override with
	// flags or env vars if they're present.
	configFile := ctx.String("config-file")
	config.populateFromFile(configFile)

	// apply defaults from Postgres environment variables, but allow
	// them to be overridden in the next step
	config.populateFromPostgresVars()

	// apply some other, sane defaults
	config.applyDefaults()

	// override if passed-in from the CLI or via environment variables
	config.applyArguments(ctx)

	// if a connection URL was passed, use that instead for our connection
	// configuration
	if config.URL != "" {
		config.overrideFromURL()
	}
	return config.validate()
}

func (config *Config) populateFromFile(configFile string) {
	contents, err := ioutil.ReadFile(configFile)
	if err == nil {
		json.Unmarshal(contents, &config)
	} else {
		fmt.Println("error reading config file: ", err)
	}
	if config.DumpFile != "" {
		deprecatedDumpFieldWarning("dump-file", "dump-file", "top-level key in .pgmgr.json")
	}
	if len(config.SeedTables) != 0 {
		deprecatedDumpFieldWarning("seed-tables", "include-tables", "top-level key in .pgmgr.json")
	}
}

func (config *Config) populateFromPostgresVars() {
	if os.Getenv("PGUSER") != "" {
		config.Username = os.Getenv("PGUSER")
	}
	if os.Getenv("PGPASSWORD") != "" {
		config.Password = os.Getenv("PGPASSWORD")
	}
	if os.Getenv("PGDATABASE") != "" {
		config.Database = os.Getenv("PGDATABASE")
	}
	if os.Getenv("PGHOST") != "" {
		config.Host = os.Getenv("PGHOST")
	}
	if os.Getenv("PGPORT") != "" {
		config.Port, _ = strconv.Atoi(os.Getenv("PGPORT"))
	}
	if os.Getenv("PGSSLMODE") != "" {
		config.SslMode = os.Getenv("PGSSLMODE")
	}
}

// DumpToEnv applies all applicable keys as PG environment variables, so that
// shell commands will work on the correct target.
func (config *Config) DumpToEnv() error {
	if err := os.Setenv("PGUSER", config.Username); err != nil {
		return err
	}
	if err := os.Setenv("PGPASSWORD", config.Password); err != nil {
		return err
	}
	if err := os.Setenv("PGDATABASE", config.Database); err != nil {
		return err
	}
	if err := os.Setenv("PGHOST", config.Host); err != nil {
		return err
	}
	if err := os.Setenv("PGPORT", fmt.Sprint(config.Port)); err != nil {
		return err
	}
	if err := os.Setenv("PGSSLMODE", config.SslMode); err != nil {
		return err
	}

	return nil
}

func (config *Config) applyDefaults() {
	if config.Port == 0 {
		config.Port = 5432
	}
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Format == "" {
		config.Format = "unix"
	}
	if config.ColumnType == "" {
		config.ColumnType = "integer"
	}
	if config.MigrationTable == "" {
		config.MigrationTable = "schema_migrations"
	}
	if config.MigrationDriver == "" {
		config.MigrationDriver = "pq"
	}
	if config.SslMode == "" {
		config.SslMode = "disable"
	}
	config.DumpConfig.applyDefaults()
}

func (config *Config) applyArguments(ctx argumentContext) {
	if ctx.String("username") != "" {
		config.Username = ctx.String("username")
	}
	if ctx.String("password") != "" {
		config.Password = ctx.String("password")
	}
	if ctx.String("database") != "" {
		config.Database = ctx.String("database")
	}
	if ctx.String("host") != "" {
		config.Host = ctx.String("host")
	}
	if ctx.Int("port") != 0 {
		config.Port = ctx.Int("port")
	}
	if ctx.String("url") != "" {
		config.URL = ctx.String("url")
	}
	if ctx.String("sslmode") != "" {
		config.SslMode = ctx.String("sslmode")
	}
	if ctx.String("migration-folder") != "" {
		config.MigrationFolder = ctx.String("migration-folder")
	}
	if ctx.String("migration-driver") != "" {
		config.MigrationDriver = ctx.String("migration-driver")
	}
	config.DumpConfig.applyArguments(ctx)
}

func (config *Config) overrideFromURL() {
	// parse the DSN and populate the other configuration values. Some of the pg commands
	// accept a DSN parameter, but not all, so this will help unify things.
	r := regexp.MustCompile("^postgres://(.*)@(.*):([0-9]+)/([^?]+)")
	m := r.FindStringSubmatch(config.URL)
	if len(m) > 0 {
		user := m[1]
		config.Host = m[2]
		config.Port, _ = strconv.Atoi(m[3])
		config.Database = m[4]

		userRegex := regexp.MustCompile("^(.*):(.*)$")
		userMatch := userRegex.FindStringSubmatch(user)

		if len(userMatch) > 0 {
			config.Username = userMatch[1]
			config.Password = userMatch[2]
		} else {
			config.Username = user
		}

		queryRegex := regexp.MustCompile("([a-zA-Z0-9_-]+)=([a-zA-Z0-9_-]+)")
		matches := queryRegex.FindAllStringSubmatch(config.URL, -1)
		for _, match := range matches {
			if match[1] == "sslmode" {
				config.SslMode = match[2]
			}
		}
	} else {
		println("Could not parse DSN:  ", config.URL, " using regex ", r.String())
	}
}

func (config *Config) validate() error {
	if config.ColumnType != "integer" && config.ColumnType != "string" {
		return errors.New(`ColumnType must be "integer" or "string"`)
	}

	if config.Format != "unix" && config.Format != "datetime" {
		return errors.New(`Format must be "unix" or "datetime"`)
	}

	if config.Format == "datetime" && config.ColumnType != "string" {
		return errors.New(`ColumnType must be "string" if Format is "datetime"`)
	}

	if config.MigrationDriver != "pq" && config.MigrationDriver != "psql" {
		return errors.New("MigrationDriver must be one of: pq, psql")
	}

	return nil
}

func (config *Config) quotedMigrationTable() string {
	if !strings.Contains(config.MigrationTable, ".") {
		return pq.QuoteIdentifier(config.MigrationTable)
	}

	tokens := strings.SplitN(config.MigrationTable, ".", 2)
	return pq.QuoteIdentifier(tokens[0]) + "." + pq.QuoteIdentifier(tokens[1])
}

func (config *Config) versionColumnType() string {
	if config.ColumnType == "string" {
		return "CHARACTER VARYING (255)"
	}

	return "INTEGER"
}

func deprecatedDumpFieldWarning(oldField, newField, provision string) {
	fmt.Println(
		"WARN: Providing '"+oldField+"' as a "+provision+" is deprecated.",
		"Specify it as '"+newField+"' via the command line, or in your config",
		"file underneath the 'dump-options' key.",
	)
}
