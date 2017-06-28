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
	Username      string
	Superuser     string
	MigrationUser string `json:"migration-user"`
	Password      string
	Database      string
	Host          string
	Port          int
	URL           string
	SslMode       string

	// filepaths
	DumpFile        string `json:"dump-file"`
	MigrationFolder string `json:"migration-folder"`

	// options
	MigrationTable  string   `json:"migration-table"`
	SeedTables      []string `json:"seed-tables"`
	ExcludedTables  []string `json:"excluded-tables"`
	ExcludedSchemas []string `json:"excluded-schemas"`
	UserRoles       []string `json:"user-roles"`
	ColumnType      string   `json:"column-type"`
	Format          string
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
	if config.SslMode == "" {
		config.SslMode = "disable"
	}
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
	if ctx.String("dump-file") != "" {
		config.DumpFile = ctx.String("dump-file")
	}
	if ctx.String("migration-folder") != "" {
		config.MigrationFolder = ctx.String("migration-folder")
	}
	if ctx.StringSlice("seed-tables") != nil && len(ctx.StringSlice("seed-tables")) > 0 {
		config.SeedTables = ctx.StringSlice("seed-tables")
	}
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
