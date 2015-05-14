# Postgres Manager (pgmgr)

Utility for web applications to manage their Postgres application in a
reliable, consistent manner. Inspired by [mattes/migrate]
(http://www.github.com/mattes/migrate), but with several benefits:

* Migration version numbers are timestamp-based, not sequential. This saves
  significant headaches when multiple developers are working on the same
  application in parallel. However, `pgmgr` is still compatible with your
  old migrations.
* `pgmgr` can generate database dumps, with seed data included, so that
  there's a single, authoritative source of your database structure. It's
  recommended you regularly check this file into source control.

## Configuration

`pgmgr` supports file-based configuration (useful for checking into your
source code) and environment-based configuration, which always overrides
the former (useful for production deploys, Docker usage, etc).

### .pgmgr.json

By default, `pgmgr` will look for a file named `.pgmgr.json` in your
working directory. You can override the file path with the environment
variable `PGMGR_CONFIG_FILE`. It should look something like:

```
{
  "host": "localhost",
  "port": 5432,
  "username": "test",
  "password": "test",
  "database": "testdb"
}
```


### Environment variables

The values above map to these environment variables:

* `PGMGR_HOST`
* `PGMGR_PORT`
* `PGMGR_USERNAME`
* `PGMGR_PASSWORD`
* `PGMGR_DATABASE`

If you prefer to use a connection string, you can set `PGMGR_URL`, e.g.:

```
PGMGR_URL='postgres://test@localhost/testdb?sslmode=false&password=test'
```

Also used:

* `PGMGR_DUMP_FILE`: the filepath to dump the database definition out to
* `PGMGR_SEED_TABLES`: tables to include data with when dumping the database

## Usage

```
pgmgr migration MigrationName   # generates files for a new migration
pgmgr db create                 # creates the database if it doesn't exist
pgmgr db drop                   # drop the database
pgmgr db migrate                # apply un-applied migrations
pgmgr db load                   # loads the schema dump file from PGMGR_DUMP_FILE
pgmgr db dump                   # dumps the database structure & seeds to PGMGR_DUMP_FILE
```
