package main

import (
	"os"
	"io/ioutil"
	"encoding/json"
	"github.com/rnubel/pgmgr/pgmgr"
	"github.com/codegangsta/cli"
)

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
			Value: "localhost",
			Usage: "the host which pgmgr will connect to",
			EnvVar: "PGMGR_HOST",
		},
		cli.IntFlag{
			Name:  "port, p",
			Value: 5432,
			Usage: "the port which pgmgr will connect to",
			EnvVar: "PGMGR_PORT",
		},
		cli.StringFlag{
			Name:  "dump-file",
			Value: "db/dump.sql",
			Usage: "where to dump or load the database structure and contents to or from",
			EnvVar: "PGMGR_DUMP_FILE",
		},
		cli.StringFlag{
			Name:  "migration-folder",
			Value: "db/migrate",
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
		}

		if c.String("username") != "" {
			config.Username = c.String("username")
		}
		if c.String("password") != "" {
			config.Username = c.String("password")
		}
		if c.String("database") != "" {
			config.Database = c.String("database")
		}
		if c.String("host") != "" {
			config.Host = c.String("host")
		}
		if c.Int("port") != 0 {
			config.Port = c.Int("port")
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
			Name: "db",
			Usage: "manage your database. use 'pgmgr db help' for more info",
			Subcommands: []cli.Command{
				{
					Name: "create",
					Usage: "creates the database if it doesn't exist",
					Action: func(c *cli.Context) {
						pgmgr.Create(config)
					},
				},
				{
					Name: "drop",
					Usage: "drops the database (all sessions must be disconnected first. this command does not force it)",
					Action: func(c *cli.Context) {
						pgmgr.Drop(config)
					},
				},
				{
					Name: "dump",
					Usage: "dumps the database schema and contents to the dump file (see --dump-file)",
					Action: func(c *cli.Context) {
						pgmgr.Dump(config)
					},
				},
				{
					Name: "load",
					Usage: "loads the database schema and contents from the dump file (see --dump-file)",
					Action: func(c *cli.Context) {
						pgmgr.Load(config)
					},
				},
				{
					Name: "version",
					Usage: "returns the current schema version",
					Action: func(c *cli.Context) {
						pgmgr.Version(config)
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
