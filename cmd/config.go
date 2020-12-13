package cmd

import (
	"bytes"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/hirosassa/tblmonit/flexconfig"
	"github.com/spf13/cobra"
	"golang.org/x/xerrors"
)

func init() {
	rootCmd.AddCommand(newConfig())
}

func newConfig() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Check and edit configuration",
		Long:  `Check and edit configuration file.`,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	cmd.AddCommand(
		newConfigExpandCmd(),
	)
	return cmd
}

func newConfigExpandCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "expand",
		Short: "Expand flexible representtation config file to raw expressions",
		Long: `Expand configuration file in flexible DSL to raw expressions and output
 on standard output
For example:

tablemonit config expand tblmonit.flex.conf >> .tblmonit.toml`,
		Args: cobra.ExactArgs(1),
		RunE: runConfigExpandCmd,
	}

	return cmd
}

func runConfigExpandCmd(cmd *cobra.Command, args []string) error {
	var targetConfig flexconfig.FlexConfig
	_, err := toml.DecodeFile(args[0], &targetConfig)
	if err != nil {
		return xerrors.Errorf("failed to load target config file: %w", err)
	}

	config, err := targetConfig.Expand()
	if err != nil {
		return xerrors.Errorf("failed to expand input config file: %w", err)
	}

	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return xerrors.Errorf("failed to encode expanded config: %w", err)
	}
	fmt.Println(buf.String())

	return nil
}
