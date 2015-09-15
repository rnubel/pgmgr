package pgmgr

import (
	"os"
	"testing"
)

// create a mock to replace cli.Context
type TestContext struct {
	StringVals      map[string]string
	IntVals         map[string]int
	StringSliceVals map[string][]string
}

func (t *TestContext) String(key string) string {
	return t.StringVals[key]
}
func (t *TestContext) Int(key string) int {
	return t.IntVals[key]
}
func (t *TestContext) StringSlice(key string) []string {
	return t.StringSliceVals[key]
}

func TestDefaults(t *testing.T) {
	c := &Config{}

	LoadConfig(c, &TestContext{})

	if c.Port != 5432 {
		t.Fatal("config's port should default to 5432")
	}

	if c.Host != "localhost" {
		t.Fatal("config's host should default to localhost, but was ", c.Host)
	}

	if c.MigrationTable != "schema_migrations" {
		t.Fatal("config's migration table should default to schema_migrations, but was ", c.MigrationTable)
	}

	if c.ColumnType != "integer" {
		t.Fatal("config's column type should default to integer, but was ", c.ColumnType)
	}

	if c.Format != "unix" {
		t.Fatal("config's format should default to unix, but was ", c.Format)
	}
}

func TestOverlays(t *testing.T) {
	c := &Config{}
	ctx := &TestContext{IntVals: make(map[string]int)}

	// should prefer the value from ctx, since
	// it was passed-in explictly at runtime
	c.Port = 123
	ctx.IntVals["port"] = 456
	os.Setenv("PGPORT", "789")

	LoadConfig(c, ctx)

	if c.Port != 456 {
		t.Fatal("config's port should come from the context, but was", c.Port)
	}

	// reset
	c = &Config{}
	ctx = &TestContext{IntVals: make(map[string]int)}

	// should prefer the value from PGPORT, since
	// nothing was passed-in at runtime
	c.Port = 123
	os.Setenv("PGPORT", "789")

	LoadConfig(c, ctx)

	if c.Port != 789 {
		t.Fatal("config's port should come from PGPORT, but was", c.Port)
	}

	// reset
	c = &Config{}
	ctx = &TestContext{IntVals: make(map[string]int)}

	// should prefer the value in the struct, since
	// nothing else is given
	c.Port = 123
	os.Setenv("PGPORT", "")

	LoadConfig(c, ctx)

	if c.Port != 123 {
		t.Fatal("config's port should not change, but was", c.Port)
	}
}

func TestURL(t *testing.T) {
	c := &Config{}
	c.URL = "postgres://foo@bar:5431/testdb"

	LoadConfig(c, &TestContext{})

	if c.Username != "foo" || c.Host != "bar" || c.Port != 5431 || c.Database != "testdb" {
		t.Fatal("config did not populate itself from the given URL:", c)
	}
}

func TestValidation(t *testing.T) {
	c := &Config{}
	c.Format = "wrong"

	if err := LoadConfig(c, &TestContext{}); err == nil {
		t.Fatal("LoadConfig should reject invalid Format value")
	}

	c.Format = ""
	c.ColumnType = "wrong"
	if err := LoadConfig(c, &TestContext{}); err == nil {
		t.Fatal("LoadConfig should reject invalid ColumnType value")
	}

	c.Format = "datetime"
	c.ColumnType = "integer"
	if err := LoadConfig(c, &TestContext{}); err == nil {
		t.Fatal("LoadConfig should prevent Format=datetime when ColumnType=integer")
	}
}

func TestQuotedMigrationTable(t *testing.T) {
	c := &Config{MigrationTable: "abc"}
	if c.quotedMigrationTable() != `"abc"` {
		t.Fatal(`Migration table should be "abc", got`, c.quotedMigrationTable())
	}

	c.MigrationTable = "abc.def"
	if c.quotedMigrationTable() != `"abc"."def"` {
		t.Fatal(`Schema-qualified migration table should be "abc"."def", got`, c.quotedMigrationTable())
	}
}
