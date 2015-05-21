package main

import (
	"os"
	"./pgmgr"
	"github.com/codegangsta/cli"
)

func main() {
	app := cli.NewApp()

	app.Name  = "pgmgr"
	app.Usage = "manage your app's Postgres database"

	app.Flags = []cli.Flag {
		cli.StringFlag{
			Name:  "config-file, c",
			Value: ".pgmgr.json",
			Usage: "set the path to the JSON configuration file specifying your DB parameters",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:      "db",
			Usage:     "manage your database. use 'pgmgr db help' for more info",
			Subcommands: []cli.Command{
				{
					Name: "create",
					Usage: "creates the database if it doesn't exist",
					Action: func(c *cli.Context) {
						pgmgr.Create("foodb")
					},
				},
			},
		},
	}

	app.Action = func(c *cli.Context) {
		println("boom! I say!")
	}
	app.Run(os.Args)
}
