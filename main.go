package main

import (
	"errors"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/dleung/gotail"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"

	nconfig "github.com/netlify/util/config"
	nnats "github.com/netlify/util/nats"
)

var logger = logrus.StandardLogger()
var debug = false

type configuration struct {
	NatsConf nnats.Configuration `json:"nats_conf"`
	Subjects map[string]string   `json:"subjects"`
}

func main() {
	rootCmd := cobra.Command{
		Short: "shipit",
		RunE:  run,
	}

	rootCmd.Flags().StringP("config", "c", "config.json", "a configruation file to use")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "enable debug logging")

	if err := rootCmd.Execute(); err != nil {
		logger.Fatalf("Failed to execute command: %v", err)
	}
}

func run(cmd *cobra.Command, args []string) error {
	configFile, err := cmd.Flags().GetString("config")
	if err != nil {
		return err
	}

	if configFile == "" {
		return errors.New("Must provide a config file")
	}

	if debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	return runFromFile(configFile)
}

func runFromFile(configFile string) error {
	conf := new(configuration)
	err := nconfig.LoadFromFile(configFile, conf)
	if err != nil {
		return err
	}

	logger.WithFields(conf.NatsConf.LogFields()).Debug("Connecting to nats")
	nc, err := nnats.Connect(&conf.NatsConf)
	if err != nil {
		return err
	}

	logger.Debugf("dispatching tailers: %+v", conf.Subjects)

	wg := sync.WaitGroup{}
	for subject, file := range conf.Subjects {
		wg.Add(1)
		go func(f, s string) {
			err := tailAndSend(nc, f, s)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"subject": s,
					"file":    f,
				}).WithError(err).Errorf("Error while tailing")
			}
			wg.Done()
		}(file, subject)
	}
	logger.Debug("Waiting for routines to complete")
	wg.Wait()
	return nil
}

func tailAndSend(nc *nats.Conn, file, subject string) error {
	log := logger.WithFields(logrus.Fields{
		"subject": subject,
		"file":    file,
	})
	log.Debug("Creating tail")
	tail, err := gotail.NewTail(file, gotail.Config{})
	if err != nil {
		return err
	}

	log.Debugf("Starting to tail file")
	for line := range tail.Lines {
		nc.Publish(subject, []byte(line))
	}

	// this should never happen
	log.Info("Finished tailing")
	return nil
}
