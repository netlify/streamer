package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"unicode"

	"github.com/Sirupsen/logrus"
	"github.com/dleung/gotail"
	"github.com/nats-io/nats"
	"github.com/netlify/messaging"
	"github.com/spf13/cobra"
)

var logger *logrus.Entry
var host string

func main() {
	var configFile string

	rootCmd := cobra.Command{
		Short: "streamer",
		Run: func(cmd *cobra.Command, args []string) {
			if configFile == "" {
				log.Fatal("Must provide a config file")
			}

			run(configFile)
		},
	}

	rootCmd.Flags().StringVarP(&configFile, "config", "c", "config.json", "a configruation file to use")

	if err := rootCmd.Execute(); err != nil {
		logger.Fatalf("Failed to execute command: %v", err)
	}
}

func run(configFile string) {
	conf := new(configuration)
	err := loadFromFile(configFile, conf)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	logger, err = configureLogging(&conf.LogConf)
	if err != nil {
		log.Fatalf("Failed to configure logging : %v", err)
	}

	logger.WithFields(conf.NatsConf.LogFields()).Debug("Connecting to nats")
	nc, err := messaging.ConnectToNats(&conf.NatsConf.NatsConfig)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to nats")
	}

	if conf.NatsConf.NatsHookSubject != "" {
		hook, err := messaging.NewNatsHook(nc, conf.NatsConf.NatsHookSubject)
		if err != nil {
			logger.WithError(err).Fatal("Failed to connect build nats hook")
		}
		logrus.AddHook(hook)
		logger.WithField("hook_subject", conf.NatsConf.NatsHookSubject).Debug("Added nats hook")
	}

	host, err = os.Hostname()
	if err != nil {
		logger.WithError(err).Fatal("Failed to get hostname")
	}

	logger.Debugf("building tailers")

	wg := sync.WaitGroup{}
	for _, glob := range conf.Paths {
		matches, err := filepath.Glob(glob)
		if err != nil {
			logger.WithError(err).Fatalf("Failed to parse glob %s", glob)
		}
		if matches == nil {
			logger.Fatalf("'%s' didn't match any files", glob)
		}

		for _, path := range matches {
			subject := getSubjectName(conf.Prefix, path)
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
	logger.Info("Shutting down")
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

func getSubjectName(prefix, path string) string {
	fname := filepath.Base(path)
	runes := make([]rune, len(prefix)+len(fname))
	for i, r := range prefix + fname {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' {
			runes[i] = r
		} else {
			runes[i] = '_'
		}
	}
	return string(runes)
}
