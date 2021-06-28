package cmd

import (
	"fmt"
	"os"
	"time"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var cfg tmConfig

type tmConfig struct {
	TimeZone string
}

var verbose, debug bool // for verbose and debug output

// rootCmd represents the root command
var rootCmd = &cobra.Command{
	Use:   "tblmonit",
	Short: "Monitoring tool for Bigquery tables",
	Long:  `Monitoring BigQuery table metadata to ensure the data pipeline jobs are correctly worked.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".tblmonit" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".tblmonit")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is not found, skip to set timezone.
	if err := viper.ReadInConfig(); err == nil {
		err := viper.Unmarshal(&cfg)
		if err != nil {
			fmt.Println("Failed to read Config File", viper.ConfigFileUsed(), err)
			os.Exit(1)
		}
	}

	err := loadTimezone()
	if err != nil {
		fmt.Printf("Failed to load timezone: %v\n", err)
		os.Exit(1)
	}
	logOutput()
}

func loadTimezone() error {
	if cfg.TimeZone == "" {
		return nil
	}

	loc, err := time.LoadLocation(cfg.TimeZone)
	if err != nil {
		return fmt.Errorf("Failed to load location from config file: %s", cfg.TimeZone)
	}
	time.Local = loc
	return nil
}

func logOutput() {
	zerolog.SetGlobalLevel(zerolog.Disabled) // default: quiet mode
	switch {
	case verbose:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case debug:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.tblmonit.yaml)")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	// for log output
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "enable varbose log output")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "enable debug log output")
}
