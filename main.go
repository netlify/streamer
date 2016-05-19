package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/dleung/gotail"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"

	nconfig "github.com/netlify/util/config"
	nlog "github.com/netlify/util/logger"
	nnats "github.com/netlify/util/nats"
)

var logger *logrus.Entry
var host string

type configuration struct {
	NatsConf nnats.Configuration   `json:"nats_conf"`
	LogConf  nlog.LogConfiguration `json:"log_conf"`
	Paths    []string              `json:"paths"`
	Prefix   string                `json:"prefix"`
}

func main() {
	var configFile string

	rootCmd := cobra.Command{
		Short: "streamer",
		RunE: func(cmd *cobra.Command, args []string) error {
			if configFile == "" {
				return errors.New("Must provide a config file")
			}

			return run(configFile)
		},
	}

	rootCmd.Flags().StringVarP(&configFile, "config", "c", "config.json", "a configruation file to use")

	if err := rootCmd.Execute(); err != nil {
		logger.Fatalf("Failed to execute command: %v", err)
	}
}

func run(configFile string) error {
	conf := new(configuration)
	err := nconfig.LoadFromFile(configFile, conf)
	if err != nil {
		return err
	}

	logger, err = nlog.ConfigureLogging(&conf.LogConf)
	if err != nil {
		return err
	}

	logger.WithFields(conf.NatsConf.LogFields()).Debug("Connecting to nats")
	nc, err := nnats.Connect(&conf.NatsConf)
	if err != nil {
		return err
	}

	host, err = os.Hostname()
	if err != nil {
		return err
	}

	logger.Debugf("building tailers")

	wg := sync.WaitGroup{}
	for _, glob := range conf.Paths {
		matches, err := filepath.Glob(glob)
		if err != nil {
			return err
		}
		if matches == nil {
			return fmt.Errorf("'%s' didn't match any files", glob)
		}

		for _, path := range matches {
			subject := conf.Prefix + filepath.Base(path)
			log := logger.WithFields(logrus.Fields{
				"file":    path,
				"subject": subject,
			})
			path := path // intentional shadow

			wg.Add(1)
			go func() {
				log.Info("Starting to tail forever")
				err := tailForever(nc, subject, path)
				if err != nil {
					log.WithError(err).Warn("Problem tailing file")
				}
				wg.Done()
			}()
		}
	}

	logger.Debug("Waiting for routines to complete")
	wg.Wait()
	return nil
}

func tailForever(nc *nats.Conn, subject, path string) error {
	tail, err := gotail.NewTail(path, gotail.Config{})
	if err != nil {
		return err
	}
	payload := map[string]string{
		"@filepath": path,
		"@hostname": host,
	}

	for line := range tail.Lines {
		payload["@msg"] = line
		asBytes, err := json.Marshal(&payload)
		if err != nil {
			return err
		}
		err = nc.Publish(subject, asBytes)
		if err != nil {
			return err
		}
	}

	return nil
}
