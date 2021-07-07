package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/hirosassa/tblmonit/config"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(newFreshness())
}

func newFreshness() *cobra.Command {
	var showDetail bool
	cmd := &cobra.Command{
		Use:   "freshness",
		Short: "Check freshness for each table",
		Long: `Check freshness for each table.
The target tables and time thresholds should be listed on config file.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFreshnessCmd(args, showDetail)
		},
	}

	cmd.Flags().BoolVarP(&showDetail, "detail", "d", false, "show details of a specific reason of old tables")

	return cmd
}

func runFreshnessCmd(args []string, showDetail bool) error {
	var targetConfig config.Config
	_, err := toml.DecodeFile(args[0], &targetConfig)
	if err != nil {
		return xerrors.Errorf("failed to load target config file: %w", err)
	}

	current := time.Now()
	oldTables, err := config.CheckFreshness(targetConfig, current)
	if err != nil {
		return xerrors.Errorf("failed to check freshness: %w", err)
	}

	if len(oldTables) == 0 {
		log.Info().Msg("All tables are fresh enough!")
		return nil
	}

	var result strings.Builder
	for _, t := range oldTables {
		result.WriteString(t.Table)
		if showDetail {
			reason := fmt.Sprintf(" (%s)", strings.Join(t.Reason, ","))
			result.WriteString(reason)
		}
		result.WriteString("\n")
	}
	fmt.Print(result.String())

	return nil
}
