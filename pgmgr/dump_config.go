package pgmgr

import "strings"

// DumpConfig stores the options used by pgmgr's dump tool
// and defers connection-type options to the main config file
type DumpConfig struct {
	// exclusions
	ExcludeSchemas []string `json:"exclude-schemas"`

	// inclusions
	IncludeTables     []string `json:"seed-tables"`
	IncludePrivileges bool     `json:"include-privileges"`
	IncludeTriggers   bool     `json:"include-triggers"`

	// options
	NoCompress bool   `json:"no-compress"`
	DumpFile   string `json:"dump-file"`
}

// GetDumpFileRaw returns the literal dump file name as configured
func (config DumpConfig) GetDumpFileRaw() string {
	return config.DumpFile
}

// GetDumpFile returns the true dump file name
// with or without the specified compression suffix
func (config DumpConfig) GetDumpFile() string {
	if config.IsCompressed() {
		return config.DumpFile + ".gz"
	}
	return config.DumpFile
}

// IsCompressed returns whether compression is set
func (config DumpConfig) IsCompressed() bool {
	return !config.NoCompress
}

func (config *DumpConfig) applyArguments(ctx argumentContext) {
	if sliceValuesGiven(ctx, "exclude-schemas") {
		config.ExcludeSchemas = ctx.StringSlice("exclude-schemas")
	}
	if sliceValuesGiven(ctx, "seed-tables") {
		config.IncludeTables = ctx.StringSlice("seed-tables")
	}
	if ctx.String("dump-file") != "" {
		config.DumpFile = ctx.String("dump-file")
	}
	if ctx.Bool("no-compress") {
		config.NoCompress = true
	}
	if ctx.Bool("include-privileges") {
		config.IncludePrivileges = true
	}
	if ctx.Bool("include-triggers") {
		config.IncludeTriggers = true
	}
	if strings.HasSuffix(config.DumpFile, ".gz") {
		config.DumpFile = config.DumpFile[0 : len(config.DumpFile)-3]
		config.NoCompress = false
	}
}

func (config *DumpConfig) applyDefaults() {
	if config.DumpFile == "" {
		config.DumpFile = "dump.sql"
	}
}

func sliceValuesGiven(ctx argumentContext, key string) bool {
	return ctx.StringSlice(key) != nil && len(ctx.StringSlice(key)) > 0
}

func (config DumpConfig) baseFlags() []string {
	var args []string
	for _, schema := range config.ExcludeSchemas {
		args = append(args, "-N", schema)
	}

	if config.IsCompressed() {
		args = append(args, "-Z", "9")
	}

	if !config.IncludePrivileges {
		args = append(args, "-x")
	}

	return args
}

func (config DumpConfig) schemaFlags() []string {
	args := config.baseFlags()
	return append(args, "--schema-only")
}

func (config DumpConfig) dataFlags() []string {
	args := config.baseFlags()

	for _, table := range config.IncludeTables {
		args = append(args, "-t", table)
	}

	if !config.IncludeTriggers {
		args = append(args, "--disable-triggers")
	}

	args = append(args, "--data-only")
	return args
}
