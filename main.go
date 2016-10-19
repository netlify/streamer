package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode"

	"github.com/Sirupsen/logrus"
	"github.com/hpcloud/tail"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"

	"time"

	"github.com/netlify/streamer/conf"
	"github.com/netlify/streamer/messaging"
)

var host string

var statLock sync.Mutex
var stats map[string]int64

func main() {
	rootCmd := cobra.Command{
		Short: "streamer",
		Run:   run,
	}

	rootCmd.Flags().StringP("config", "c", "config.json", "a configruation file to use")

	if err := rootCmd.Execute(); err != nil {
		logrus.Fatalf("Failed to execute command: %v", err)
	}
}

func run(cmd *cobra.Command, args []string) {
	config, err := conf.LoadConfig(cmd)
	if err != nil {
		logrus.Fatalf("Failed to load configuration: %v", err)
	}

	logger, err := conf.ConfigureLogging(&config.LogConf)
	if err != nil {
		log.Fatalf("Failed to configure logging : %v", err)
	}

	logger.WithFields(logrus.Fields{
		"servers":   config.NatsConf.Servers,
		"ca_files":  config.NatsConf.CAFiles,
		"key_file":  config.NatsConf.KeyFile,
		"cert_file": config.NatsConf.CertFile,
	}).Debug("Connecting to nats")
	nc, err := messaging.ConnectToNats(&config.NatsConf)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to nats")
	}

	stats = make(map[string]int64)
	reportStats(config.ReportSec, nc, logger)

	host, err = os.Hostname()
	if err != nil {
		logger.WithError(err).Fatal("Failed to get hostname")
	}

	logger.Debugf("building tailers")

	wg := sync.WaitGroup{}
	for _, pathConfig := range config.Paths {
		matches, err := filepath.Glob(pathConfig.Path)
		if err != nil {
			logger.WithError(err).Fatalf("Failed to parse glob %s", pathConfig.Path)
		}
		if matches == nil {
			logger.Fatalf("'%s' didn't match any files", pathConfig.Path)
		}

		for _, path := range matches {
			subject := getSubjectName(pathConfig.Prefix, path)
			log := logger.WithFields(logrus.Fields{
				"file":    path,
				"subject": subject,
			})
			path := path // intentional shadow

			wg.Add(1)
			go func() {
				defer wg.Done()

				log.Info("Starting to tail forever")
				err := tailForever(nc, subject, path, log)
				if err != nil {
					log.WithError(err).Warn("Problem tailing file")
				}
			}()
		}
	}

	logger.Debug("Waiting for routines to complete")
	wg.Wait()
	logger.Info("Shutting down")
}

func tailForever(nc *nats.Conn, subject, path string, logger *logrus.Entry) error {
	tailConfig := tail.Config{
		Logger:      logger,
		ReOpen:      true,
		MustExist:   true,
		Location:    &tail.SeekInfo{Offset: 0, Whence: os.SEEK_END},
		Follow:      true,
		MaxLineSize: 0, // infinite lines
	}
	t, err := tail.TailFile(path, tailConfig)
	if err != nil {
		return err
	}

	payload := map[string]string{
		"@filepath": path,
		"@hostname": host,
	}
	maxPayload := nc.MaxPayload()
	overageKey := fmt.Sprintf("%s.over_size", path)
	linesSeenKey := fmt.Sprintf("%s.lines_seen", path)
	blankSeenKey := fmt.Sprintf("%s.blanks_seen", path)
	for line := range t.Lines {
		text := strings.TrimSpace(line.Text)
		if text != "" {
			incrementStat(linesSeenKey)
			payload["@msg"] = text
			asBytes, err := json.Marshal(&payload)
			if err != nil {
				return err
			}
			length := len(asBytes)
			if int64(length) > maxPayload {
				logger.Warnf("Can't send line because it is over length: %d vs %d bytes", length, maxPayload)
				incrementStat(overageKey)
			} else {
				err = nc.Publish(subject, asBytes)
				if err != nil {
					return err
				}
			}
		} else {
			incrementStat(blankSeenKey)
		}
	}

	return nil
}

func getSubjectName(prefix, path string) string {
	fname := filepath.Base(path)
	runes := make([]rune, len(prefix)+len(fname)+1)
	for i, r := range prefix + "." + fname {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' {
			runes[i] = r
		} else {
			runes[i] = '_'
		}
	}
	return string(runes)
}

func reportStats(intervalSec int64, nc *nats.Conn, log *logrus.Entry) {
	if intervalSec == 0 {
		log.Info("Stat reporting disabled")
		return
	}

	go func() {
		ticks := time.Tick(time.Duration(intervalSec) * time.Second)
		for range ticks {
			go func() {
				fields := logrus.Fields{
					"in_bytes":  nc.Statistics.InBytes,
					"out_bytes": nc.Statistics.OutBytes,
					"in_msgs":   nc.Statistics.InMsgs,
					"out_msgs":  nc.Statistics.OutMsgs,
				}
				for k, v := range stats {
					fields[k] = v
				}

				logrus.WithFields(fields).Info("Stats report")
			}()
		}
	}()
}

func incrementStat(key string) {
	go func() {
		statLock.Lock()
		defer statLock.Unlock()
		val, ok := stats[key]
		if !ok {
			val = 0
		}
		stats[key] = val + 1
	}()
}
