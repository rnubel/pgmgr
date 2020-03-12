package pgmgr

import (
	"strings"
	"testing"
)

func TestDumpFlags(test *testing.T) {
	c := DumpConfig{
		IncludeTables:  []string{"iTable1", "iTable2"},
		ExcludeSchemas: []string{},
		Compress:       true,
	}

	flags := strings.Join(c.baseFlags(), " ")
	for _, t := range c.ExcludeSchemas {
		if !strings.Contains(flags, "-N "+t) {
			test.Fatal("Dump flags should flag each excluded schema with '-N', missing", t)
		}
	}
	if !strings.Contains(flags, "-Z 9") {
		test.Fatal("Dump flags should set compression level to 9 when compressed is 't'")
	}
	if !strings.Contains(flags, "-x") {
		test.Fatal("Dump flags should set -x when IncludePrivileges is 'f'")
	}

	c.Compress = false
	c.IncludePrivileges = true
	flags = strings.Join(c.baseFlags(), " ")
	if strings.Contains(flags, "-Z 9") {
		test.Fatal("Dump flags should not set compression level to 9 when compressed is 'f'")
	}

	if strings.Contains(flags, "-x") {
		test.Fatal("Dump flags should not set -x when IncludePrivileges is 't'")
	}

	flags = strings.Join(c.dataFlags(), " ")
	for _, t := range c.IncludeTables {
		if !strings.Contains(flags, "-t "+t) {
			test.Fatal("Data flags should flag each included table with '-t', missing", t)
		}
	}
	if !strings.Contains(flags, "--data-only") {
		test.Fatal("Data flags should mark --data-only")
	}
	if !strings.Contains(flags, "--disable-triggers") {
		test.Fatal("Data flags should set --disable-triggers when IncludeTriggers is 'f'")
	}

	c.IncludeTriggers = true
	flags = strings.Join(c.dataFlags(), " ")
	if strings.Contains(flags, "--disable-triggers") {
		test.Fatal("Data flags should not set --disable-triggers when IncludeTriggers is 't'")
	}

	flags = strings.Join(c.schemaFlags(), " ")
	if !strings.Contains(flags, "--schema-only") {
		test.Fatal("Schema flags should mark --schema-only")
	}
}

func TestDumpDefaults(t *testing.T) {
	c := &Config{}
	c.applyDefaults()

	if c.DumpConfig.DumpFile != "dump.sql" {
		t.Fatal("dump config's dump-file should default to 'dump.sql', but was ", c.DumpConfig.DumpFile)
	}

	if !c.DumpConfig.Compress {
		t.Fatal("dump config's compression should default to 't', but was ", c.DumpConfig.Compress)
	}

	if c.DumpConfig.IncludePrivileges {
		t.Fatal("dump config's include privileges should default to 'f', but was ", c.DumpConfig.IncludePrivileges)
	}

	if c.DumpConfig.IncludeTriggers {
		t.Fatal("dump config's include triggers should default to 'f', but was ", c.DumpConfig.IncludeTriggers)
	}

	dumpContext := TestContext{
		StringVals: map[string]string{
			"dump-file": "dump.file.sql.gz",
		},
	}
	LoadConfig(c, &dumpContext)

	if c.DumpConfig.DumpFile != "dump.file.sql" {
		t.Fatal("dump config should strip '.gz' suffix, but was ", c.DumpConfig.DumpFile)
	}
	if !c.DumpConfig.Compress {
		t.Fatal("dump config should set Compress='t' if '.gz' suffix is present, but was ", c.DumpConfig.Compress)
	}
}

func TestDumpOverlays(t *testing.T) {
	c := &Config{}
	ctx := &TestContext{StringVals: make(map[string]string)}

	// should prefer the value from ctx, since
	// it was passed-in explictly at runtime
	c.DumpConfig.DumpFile = "structval"
	ctx.StringVals["dump-file"] = "stringval"

	LoadConfig(c, ctx)

	if c.DumpConfig.DumpFile != "stringval" {
		t.Fatal("config's dump file should come from the context, but was", c.DumpConfig.DumpFile)
	}

	// reset
	c = &Config{}
	ctx = &TestContext{StringVals: make(map[string]string)}

	// should prefer the value in the struct, since
	// nothing else is given
	c.DumpConfig.DumpFile = "structval"
	LoadConfig(c, ctx)

	if c.DumpConfig.DumpFile != "structval" {
		t.Fatal("config's dump file should not change, but was", c.DumpConfig.DumpFile)
	}
}
