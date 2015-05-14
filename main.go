package main

import (
  "fmt"
)

func main() {
  printHelpMessage()
}

func printHelpMessage() {
  fmt.Println("pgmgr - a Postgres-targeted database manager for your web app");
  fmt.Println("");
  fmt.Println("Usage: pgmgr [command]");
  fmt.Println("Available commands:");
  fmt.Println("  db create              Creates the database");
  fmt.Println("  db drop                Drops the database");
  fmt.Println("  help");
}
