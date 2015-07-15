package pgmgr

import (
	"os"
	"io/ioutil"
	"encoding/json"
	"github.com/codegangsta/cli"
	"regexp"
	"strconv"
	"fmt"
)

type Config struct {
	// connection
	Username string
	Password string
	Database string
	Host     string
	Port     int
	Url      string

	// filepaths
	DumpFile        string	`json:"dump-file"`
	MigrationFolder string	`json:"migration-folder"`

	// options
	SeedTables	[]string	`json:"seed-tables"`
}

func LoadConfig(config *Config, ctx *cli.Context) {
	// load configuration from file first; then override with
	// flags or env vars if they're present.
	configFile := ctx.String("config-file")
	contents, err := ioutil.ReadFile(configFile)
	if err == nil {
		json.Unmarshal(contents, &config)
	} else {
		fmt.Println("error reading config file: ", err)
	}

	// apply defaults from Postgres environment variables, but allow
	// them to be overridden in the next step
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

	// apply some other, sane defaults
	if config.Port == 0 {
		config.Port = 5432
	}
	if config.Host == "" {
		config.Host = "localhost"
	}

	// override if passed-in from the CLI or via environment variables
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
	if ctx.Int("port") != 0  {
		config.Port = ctx.Int("port")
	}
	if ctx.String("url") != "" {
		config.Url = ctx.String("url")
	}

	if config.Url != "" { // TODO: move this into pgmgr, probably?
		// parse the DSN and populate the other configuration values. Some of the pg commands
		// accept a DSN parameter, but not all, so this will help unify things.
		r := regexp.MustCompile("^postgres://(.*)@(.*):([0-9]+)/([a-zA-Z0-9_-]+)")
		m := r.FindStringSubmatch(config.Url)
		if len(m) > 0 {
			config.Username = m[1]
			config.Host = m[2]
			config.Port, _ = strconv.Atoi(m[3])
			config.Database = m[4]
		} else {
			println("Could not parse DSN:  ", config.Url, " using regex ", r.String())
		}
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
