package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var cfgFile string

var config Config

type Config struct {
	projects Projects
}

type Projects struct {
	project     string
	tableConfig TableConfig
}

type TableConfig struct {
	table         string
	timethreshold time.Time
}

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

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.tblmonit.yaml)")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
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

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Failed to read Config File:", viper.ConfigFileUsed())
		os.Exit(1)
	}

	if err := viper.Unmarshal(&config); err != nil {
		fmt.Println("Failed to read Config File:", viper.ConfigFileUsed())
		os.Exit(1)
	}
}
