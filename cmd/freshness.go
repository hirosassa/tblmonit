package cmd

import (
	"fmt"
	"time"

	"github.com/hirosassa/tblmonit/config"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(newFreshness())
}

func newFreshness() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "freshness",
		Short: "Check freshness for each table",
		Long: `Check freshness for each table.
The target tables and time thresholds should be listed on config file.
`,
		RunE: runFreshnessCmd,
	}
	return cmd
}

func runFreshnessCmd(cmd *cobra.Command, args []string) error {
	current := time.Now()
	oldTables, err := config.CheckFreshness(cfg, current)
	if err != nil {
		return xerrors.Errorf("failed to check freshness: %w", err)
	}

	if len(oldTables) == 0 {
		log.Info().Msg("All tables are fresh enough!")
		return nil
	}

	for _, t := range oldTables {
		fmt.Printf("%s\n", t)
	}

	return nil
}
