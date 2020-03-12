package pgmgr

import "strings"

// DumpConfig stores the options used by pgmgr's dump tool
// and defers connection-type options to the main config file
type DumpConfig struct {
	// exclusions
	ExcludeSchemas []string `json:"exclude-schemas"`

	// inclusions
	IncludeTables []string `json:"include-tables"`

	// options
	DumpCompression string `json:"compress"`
	DumpFile        string `json:"dump-file"`
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

// IsCompressed returns the configured value of the DumpCompression flag.
// Since compression is really beneficial to apply, we greedily
// set to true for any string value other than "f"
func (config DumpConfig) IsCompressed() bool {
	return config.DumpCompression != "f"
}

func (config *DumpConfig) applyArguments(ctx argumentContext) {
	if sliceValuesGiven(ctx, "exclude-schemas") {
		config.ExcludeSchemas = ctx.StringSlice("exclude-schemas")
	}
	if sliceValuesGiven(ctx, "include-tables") {
		config.IncludeTables = ctx.StringSlice("include-tables")
	}
	if sliceValuesGiven(ctx, "seed-tables") {
		deprecatedDumpFieldWarning("seed-tables", "include-tables", "command line arg")
		config.IncludeTables = ctx.StringSlice("seed-tables")
	}
	if ctx.String("dump-file") != "" {
		config.DumpFile = ctx.String("dump-file")
	}
	if ctx.String("dump-compression") == "f" {
		config.DumpCompression = "f"
	}
	if strings.HasSuffix(config.DumpFile, ".gz") {
		config.DumpFile = config.DumpFile[0 : len(config.DumpFile)-3]
		config.DumpCompression = "t"
	}
}

func (config *DumpConfig) applyDefaults() {
	if config.DumpFile == "" {
		config.DumpFile = "dump.sql"
	}
	if config.DumpCompression == "" {
		config.DumpCompression = "t"
	}
}

func sliceValuesGiven(ctx argumentContext, key string) bool {
	return ctx.StringSlice(key) != nil && len(ctx.StringSlice(key)) > 0
}

func (config DumpConfig) dumpFlags() []string {
	var args []string
	for _, schema := range config.ExcludeSchemas {
		args = append(args, "-N", schema)
	}

	for _, table := range config.IncludeTables {
		args = append(args, "-t", table)
	}

	if config.IsCompressed() {
		args = append(args, "-Z", "9")
	}

	return args
}
