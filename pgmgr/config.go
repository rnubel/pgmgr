package pgmgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
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

	// filepaths
	DumpFile        string `json:"dump-file"`
	MigrationFolder string `json:"migration-folder"`

	// options
	SeedTables []string `json:"seed-tables"`
}

// LoadConfig reads the config file and applies CLI arguments as
// overrides.
func LoadConfig(config *Config, ctx argumentContext) {
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
}

func (config *Config) applyDefaults() {
	if config.Port == 0 {
		config.Port = 5432
	}
	if config.Host == "" {
		config.Host = "localhost"
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
	r := regexp.MustCompile("^postgres://(.*)@(.*):([0-9]+)/([a-zA-Z0-9_-]+)")
	m := r.FindStringSubmatch(config.URL)
	if len(m) > 0 {
		config.Username = m[1]
		config.Host = m[2]
		config.Port, _ = strconv.Atoi(m[3])
		config.Database = m[4]
	} else {
		println("Could not parse DSN:  ", config.URL, " using regex ", r.String())
	}
}
