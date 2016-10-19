package conf

import (
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/netlify/streamer/messaging"
)

type Config struct {
	NatsConf  messaging.NatsConfig `mapstructure:"nats_conf"    json:"nats_conf"`
	LogConf   LoggingConfig        `mapstructure:"log_conf"     json:"log_conf"`
	Prefix    string               `mapstructure:"prefix"       json:"prefix"`
	Paths     []PathConfig         `mapstructure:"paths"        json:"paths"`
	ReportSec int64                `mapstructure:"report_sec"   json:"report_sec"`
}

type PathConfig struct {
	Path   string `mapstructure:"path"   json:"path"`
	Prefix string `mapstructure:"prefix" json:"prefix"`
}

// LoadConfig loads the config from a file if specified, otherwise from the environment
func LoadConfig(cmd *cobra.Command) (*Config, error) {
	viper.SetConfigType("json")

	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return nil, err
	}

	viper.SetEnvPrefix("ELASTINATS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if configFile, _ := cmd.Flags().GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath("./")
		viper.AddConfigPath("$HOME/.netlify-subscriptions/")
	}

	if err := viper.ReadInConfig(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	config := new(Config)

	if err := viper.Unmarshal(config); err != nil {
		return nil, err
	}

	return config, nil
}
