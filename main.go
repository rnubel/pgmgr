package main

import (
	"os"
	"io/ioutil"
	"encoding/json"
	"github.com/rnubel/pgmgr/pgmgr"
	"github.com/codegangsta/cli"
	"regexp"
	"strconv"
	"fmt"
)

func displayErrorOrMessage(err error, args... interface{}) {
	if err != nil {
		fmt.Println(os.Stderr, "Error: ", err)
		os.Exit(1)
	} else {
		fmt.Println(args...)
	}
}

func main() {
	config := &pgmgr.Config{}
	app := cli.NewApp()

	app.Name  = "pgmgr"
	app.Usage = "manage your app's Postgres database"
	app.Version = "0.0.1"

	s := make([]string, 0)

	app.Flags = []cli.Flag {
		cli.StringFlag{
			Name:  "config-file, c",
			Value: ".pgmgr.json",
			Usage: "set the path to the JSON configuration file specifying your DB parameters",
			EnvVar: "PGMGR_CONFIG_FILE",
		},
		cli.StringFlag{
			Name:  "database, d",
			Value: "",
			Usage: "the database name which pgmgr will connect to or try to create",
			EnvVar: "PGMGR_DATABASE",
		},
		cli.StringFlag{
			Name:  "username, u",
			Value: "",
			Usage: "the username which pgmgr will connect with",
			EnvVar: "PGMGR_USERNAME",
		},
		cli.StringFlag{
			Name:  "password, P",
			Value: "",
			Usage: "the password which pgmgr will connect with",
			EnvVar: "PGMGR_PASSWORD",
		},
		cli.StringFlag{
			Name:  "host, H",
			Value: "",
			Usage: "the host which pgmgr will connect to",
			EnvVar: "PGMGR_HOST",
		},
		cli.IntFlag{
			Name:  "port, p",
			Value: 0,
			Usage: "the port which pgmgr will connect to",
			EnvVar: "PGMGR_PORT",
		},
		cli.StringFlag{
			Name: "url",
			Value: "",
			Usage: "connection URL or DSN containing connection info; will override the other params if given",
			EnvVar: "PGMGR_URL",
		},
		cli.StringFlag{
			Name:  "dump-file",
			Value: "",
			Usage: "where to dump or load the database structure and contents to or from",
			EnvVar: "PGMGR_DUMP_FILE",
		},
		cli.StringFlag{
			Name:  "migration-folder",
			Value: "",
			Usage: "folder containing the migrations to apply",
			EnvVar: "PGMGR_MIGRATION_FOLDER",
		},
		cli.StringSliceFlag{
			Name:  "seed-tables",
			Value: (*cli.StringSlice)(&s),
			Usage: "list of tables (or globs matching table names) to dump the data of",
			EnvVar: "PGMGR_SEED_TABLES",
		},
	}

	app.Before = func(c *cli.Context) error {
		// load configuration from file first; then override with
		// flags or env vars if they're present.
		configFile := c.String("config-file")
		contents, err := ioutil.ReadFile(configFile)
		if err == nil {
			json.Unmarshal(contents, &config)
		} else {
			fmt.Println("error reading config file: ", err)
		}

		// apply some defaults
		if config.Port == 0 {
			config.Port = 5432
		}

		if config.Host == "" {
			config.Host = "localhost"
		}

		// override if passed-in
		if c.String("username") != "" {
			config.Username = c.String("username")
		}
		if c.String("password") != "" {
			config.Password = c.String("password")
		}
		if c.String("database") != "" {
			config.Database = c.String("database")
		}
		if c.String("host") != "" {
			config.Host = c.String("host")
		}
		if c.Int("port") != 0  {
			config.Port = c.Int("port")
		}
		if c.String("url") != "" {
			config.Url = c.String("url")
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

		if c.String("dump-file") != "" {
			config.DumpFile = c.String("dump-file")
		}
		if c.String("migration-folder") != "" {
			config.MigrationFolder = c.String("migration-folder")
		}
		if c.StringSlice("seed-tables") != nil && len(c.StringSlice("seed-tables")) > 0 {
			config.SeedTables = c.StringSlice("seed-tables")
		}

		return nil
	}

	app.Commands = []cli.Command{
		{
			Name: "migration",
			Usage: "generates a new migration with the given name",
			Action: func(c *cli.Context) {
				if len(c.Args()) == 0 {
					println("migration name not given! try `pgmgr migration NameGoesHere`")
				} else {
					pgmgr.CreateMigration(config, c.Args()[0])
				}
			},
		},
		{
			Name: "config",
			Usage: "displays the current configuration as seen by pgmgr",
			Action: func(c *cli.Context) {
				fmt.Printf("%+v\n", config)
			},
		},
		{
			Name: "db",
			Usage: "manage your database. use 'pgmgr db help' for more info",
			Subcommands: []cli.Command{
				{
					Name: "create",
					Usage: "creates the database if it doesn't exist",
					Action: func(c *cli.Context) {
						displayErrorOrMessage(pgmgr.Create(config), "Database", config.Database, "created successfully.")
					},
				},
				{
					Name: "drop",
					Usage: "drops the database (all sessions must be disconnected first. this command does not force it)",
					Action: func(c *cli.Context) {
						displayErrorOrMessage(pgmgr.Drop(config), "Database", config.Database, "dropped successfully.")
					},
				},
				{
					Name: "dump",
					Usage: "dumps the database schema and contents to the dump file (see --dump-file)",
					Action: func(c *cli.Context) {
						err := pgmgr.Dump(config)
						displayErrorOrMessage(err, "Database dumped to", config.DumpFile, "successfully")
					},
				},
				{
					Name: "load",
					Usage: "loads the database schema and contents from the dump file (see --dump-file)",
					Action: func(c *cli.Context) {
						err := pgmgr.Load(config)
						displayErrorOrMessage(err, "Database loaded successfully.")

						v, err := pgmgr.Version(config)
						displayErrorOrMessage(err, "Latest migration version: ", v)
					},
				},
				{
					Name: "version",
					Usage: "returns the current schema version",
					Action: func(c *cli.Context) {
						v, err := pgmgr.Version(config)
						displayErrorOrMessage(err, "Latest migration version: ", v)
					},
				},
				{
					Name: "migrate",
					Usage: "applies any un-applied migrations in the migration folder (see --migration-folder)",
					Action: func(c *cli.Context) {
						pgmgr.Migrate(config)
					},
				},
				{
					Name: "rollback",
					Usage: "rolls back the latest migration",
					Action: func(c *cli.Context) {
						pgmgr.Rollback(config)
					},
				},
			},
		},
	}

	app.Action = func(c *cli.Context) {
		app.Command("help").Run(c)
	}

	app.Run(os.Args)
}
