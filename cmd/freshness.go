package cmd

import (
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(newFreshness())
}

func newFreshness() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "freshness",
		Short: "A brief description of your command",
		Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	cmd.AddCommand(
		newFreshness(),
	)

	return cmd
}

func runFreshnessCmd(cmd *cobra.Command, args []string) error {
	return nil
}
