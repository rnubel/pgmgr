package pgmgr

import (
	"strings"
	"testing"
)

func TestDumpFlags(test *testing.T) {
	c := DumpConfig{
		IncludeTables:   []string{"iTable1", "iTable2"},
		ExcludeSchemas:  []string{},
		DumpCompression: "anyValueOtherThanLowerCaseF",
	}

	flags := strings.Join(c.dumpFlags(), " ")
	for _, t := range c.IncludeTables {
		if !strings.Contains(flags, "-t "+t) {
			test.Fatal("Dump flags should flag each included table with '-t', missing", t)
		}
	}
	for _, t := range c.ExcludeSchemas {
		if !strings.Contains(flags, "-N "+t) {
			test.Fatal("Dump flags should flag each excluded schema with '-N', missing", t)
		}
	}
	if !strings.Contains(flags, "-Z 9") {
		test.Fatal("Dump flags should set compression level to 9 when compressed is 't'")
	}
}

func TestIsCompressed(t *testing.T) {
	c := DumpConfig{}
	c.DumpCompression = ""
	if !c.IsCompressed() {
		t.Fatal("Dump config IsCompressed should be true for any value other than 'f'")
	}
	c.DumpCompression = "t"
	if !c.IsCompressed() {
		t.Fatal("Dump config IsCompressed should be true when value is 't'")
	}
	c.DumpCompression = "f"
	if c.IsCompressed() {
		t.Fatal("Dump config IsCompressed should be false when value is 'f'")
	}
}

func TestDumpDefaults(t *testing.T) {
	c := &Config{}
	LoadConfig(c, &TestContext{})

	if c.DumpConfig.DumpFile != "dump.sql" {
		t.Fatal("dump config's dump-file should default to 'dump.sql', but was ", c.DumpConfig.DumpFile)
	}

	if c.DumpConfig.DumpCompression != "t" {
		t.Fatal("dump config's dump-compression should default to 't', but was ", c.DumpConfig.DumpCompression)
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
	if c.DumpConfig.DumpCompression != "t" {
		t.Fatal("dump config should set DumpCompression='t' if '.gz' suffix is present, but was ", c.DumpConfig.DumpCompression)
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
