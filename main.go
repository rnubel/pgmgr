package main

import (
	"fmt"
	"os"

	"github.com/rnubel/pgmgr/pgmgr"
	"github.com/urfave/cli"
)

func displayErrorOrMessage(err error, args ...interface{}) error {
	if err != nil {
		return cli.NewExitError(fmt.Sprintln("Error: ", err), 1)
	}

	fmt.Println(args...)
	return nil
}

func displayVersion(config *pgmgr.Config) error {
	v, err := pgmgr.Version(config)
	if v < 0 {
		return displayErrorOrMessage(err, "Database has no schema_migrations table; run `pgmgr db migrate` to create it.")
	}

	return displayErrorOrMessage(err, "Latest migration version:", v)
}

func main() {
	config := &pgmgr.Config{}
	app := cli.NewApp()

	app.Name = "pgmgr"
	app.Usage = "manage your app's Postgres database"
	app.Version = "0.0.2"

	var s []string

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "config-file, c",
			Value:  ".pgmgr.json",
			Usage:  "set the path to the JSON configuration file specifying your DB parameters",
			EnvVar: "PGMGR_CONFIG_FILE",
		},
		cli.StringFlag{
			Name:   "database, d",
			Value:  "",
			Usage:  "the database name which pgmgr will connect to or try to create",
			EnvVar: "PGMGR_DATABASE",
		},
		cli.StringFlag{
			Name:   "username, u",
			Value:  "",
			Usage:  "the username which pgmgr will connect with",
			EnvVar: "PGMGR_USERNAME",
		},
		cli.StringFlag{
			Name:   "password, P",
			Value:  "",
			Usage:  "the password which pgmgr will connect with",
			EnvVar: "PGMGR_PASSWORD",
		},
		cli.StringFlag{
			Name:   "host, H",
			Value:  "",
			Usage:  "the host which pgmgr will connect to",
			EnvVar: "PGMGR_HOST",
		},
		cli.IntFlag{
			Name:   "port, p",
			Value:  0,
			Usage:  "the port which pgmgr will connect to",
			EnvVar: "PGMGR_PORT",
		},
		cli.StringFlag{
			Name:   "sslmode",
			Value:  "",
			Usage:  "whether to verify SSL connection or not. See https://www.postgresql.org/docs/9.1/static/libpq-ssl.html",
			EnvVar: "PGMGR_SSLMODE",
		},
		cli.StringFlag{
			Name:   "url",
			Value:  "",
			Usage:  "connection URL or DSN containing connection info; will override the other params if given",
			EnvVar: "PGMGR_URL",
		},
		cli.StringFlag{
			Name:   "dump-file",
			Value:  "",
			Usage:  "where to dump or load the database structure and contents to or from",
			EnvVar: "PGMGR_DUMP_FILE",
		},
		cli.StringFlag{
			Name:   "column-type",
			Value:  "integer",
			Usage:  "column type to use in schema_migrations table; 'integer' or 'string'",
			EnvVar: "PGMGR_COLUMN_TYPE",
		},
		cli.StringFlag{
			Name:   "format",
			Value:  "unix",
			Usage:  "timestamp format for migrations; 'unix' or 'datetime'",
			EnvVar: "PGMGR_FORMAT",
		},
		cli.StringFlag{
			Name:   "migration-table",
			Value:  "schema_migrations",
			Usage:  "table to use for storing migration status; eg 'myschema.applied_migrations'",
			EnvVar: "PGMGR_MIGRATION_TABLE",
		},
		cli.StringFlag{
			Name:   "migration-folder",
			Value:  "",
			Usage:  "folder containing the migrations to apply",
			EnvVar: "PGMGR_MIGRATION_FOLDER",
		},
		cli.StringFlag{
			Name:   "migration-driver",
			Value:  "",
			Usage:  "how to apply the migrations. supported options are pq (which will execute the migration as one statement) or psql (which will use the psql binary on your system to execute each line)",
			EnvVar: "PGMGR_MIGRATION_DRIVER",
		},
		cli.StringSliceFlag{
			Name:   "seed-tables",
			Value:  (*cli.StringSlice)(&s),
			Usage:  "list of tables (or globs matching table names) to dump the data of",
			EnvVar: "PGMGR_SEED_TABLES",
		},
	}

	app.Before = func(c *cli.Context) error {
		return pgmgr.LoadConfig(config, c)
	}

	app.Commands = []cli.Command{
		{
			Name:  "migration",
			Usage: "generates a new migration with the given name",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "no-txn",
					Usage: "generate a migration that will not be wrapped in a transaction when run",
				},
			},
			Action: func(c *cli.Context) error {
				if len(c.Args()) == 0 {
					return cli.NewExitError("migration name not given! try `pgmgr migration NameGoesHere`", 1)
				}

				return displayErrorOrMessage(pgmgr.CreateMigration(config, c.Args()[0], c.Bool("no-txn")))
			},
		},
		{
			Name:  "config",
			Usage: "displays the current configuration as seen by pgmgr",
			Action: func(c *cli.Context) error {
				fmt.Printf("%+v\n", config)
				return nil
			},
		},
		{
			Name:  "db",
			Usage: "manage your database. use 'pgmgr db help' for more info",
			Subcommands: []cli.Command{
				{
					Name:  "create",
					Usage: "creates the database if it doesn't exist",
					Action: func(c *cli.Context) error {
						return displayErrorOrMessage(pgmgr.Create(config), "Database", config.Database, "created successfully.")
					},
				},
				{
					Name:  "drop",
					Usage: "drops the database (all sessions must be disconnected first. this command does not force it)",
					Action: func(c *cli.Context) error {
						return displayErrorOrMessage(pgmgr.Drop(config), "Database", config.Database, "dropped successfully.")
					},
				},
				{
					Name:  "dump",
					Usage: "dumps the database schema and contents to the dump file (see --dump-file)",
					Action: func(c *cli.Context) error {
						err := pgmgr.Dump(config)
						return displayErrorOrMessage(err, "Database dumped to", config.DumpFile, "successfully")
					},
				},
				{
					Name:  "load",
					Usage: "loads the database schema and contents from the dump file (see --dump-file)",
					Action: func(c *cli.Context) error {
						err := pgmgr.Load(config)
						err = displayErrorOrMessage(err, "Database loaded successfully.")
						if err != nil {
							return err
						}

						return displayVersion(config)
					},
				},
				{
					Name:  "version",
					Usage: "returns the current schema version",
					Action: func(c *cli.Context) error {
						return displayVersion(config)
					},
				},
				{
					Name:  "migrate",
					Usage: "applies any un-applied migrations in the migration folder (see --migration-folder)",
					Action: func(c *cli.Context) error {
						err := pgmgr.Migrate(config)
						if err != nil {
							return cli.NewExitError(fmt.Sprintln("Error during migration:", err), 1)
						}

						return nil
					},
				},
				{
					Name:  "rollback",
					Usage: "rolls back the latest migration",
					Action: func(c *cli.Context) error {
						pgmgr.Rollback(config)
						return nil
					},
				},
			},
		},
	}

	app.Action = func(c *cli.Context) error {
		app.Command("help").Run(c)
		return nil
	}

	app.Run(os.Args)
}
