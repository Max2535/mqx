package cli

import (
	"cmp"
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// Release builds set these, e.g.
// -ldflags "-X github.com/Max2535/mqx/internal/cli.version=v0.1.0 -X ...cli.commit=<sha> -X ...cli.date=<rfc3339>".
var (
	version string
	commit  string
	date    string
)

type build struct{ version, commit, date string }

// currentBuild prefers ldflags values, then Go's embedded build info, then placeholders.
func currentBuild() build {
	b := build{version: version, commit: commit, date: date}
	if info, ok := debug.ReadBuildInfo(); ok {
		if b.version == "" && info.Main.Version != "(devel)" {
			b.version = info.Main.Version
		}
		for _, s := range info.Settings {
			switch {
			case s.Key == "vcs.revision" && b.commit == "":
				b.commit = s.Value
			case s.Key == "vcs.time" && b.date == "":
				b.date = s.Value
			}
		}
	}
	b.version = cmp.Or(b.version, "dev")
	b.commit = cmp.Or(b.commit, "unknown")
	b.date = cmp.Or(b.date, "unknown")
	return b
}

func (b build) String() string {
	return fmt.Sprintf("mqx %s (commit %s, built %s, %s/%s)", b.version, b.commit, b.date, runtime.GOOS, runtime.GOARCH)
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the mqx version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), currentBuild())
			return err
		},
	}
}
