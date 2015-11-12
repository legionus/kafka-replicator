/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-replicator as the file COPYING.
 */

package main

import (
	log "github.com/Sirupsen/logrus"

	"strings"
	"time"
)

type CfgLogLevel struct {
	log.Level
}

func (d *CfgLogLevel) UnmarshalText(data []byte) (err error) {
	d.Level, err = log.ParseLevel(strings.ToLower(string(data)))
	return
}

// CfgDuration is a Duration wrapper for Config.
type CfgDuration struct {
	time.Duration
}

// UnmarshalText is a wrapper.
func (d *CfgDuration) UnmarshalText(data []byte) (err error) {
	d.Duration, err = time.ParseDuration(string(data))
	return
}

type ConfigGlobal struct {
	Address string
	Logfile    string
	Pidfile    string
	GoMaxProcs int
}

type ConfigLogging struct {
	Level            CfgLogLevel
	DisableColors    bool
	DisableTimestamp bool
	FullTimestamp    bool
	DisableSorting   bool
}

type ConfigKafka struct {
	Brokers          []string
	Topics           []string
	DialTimeout      CfgDuration
	LeaderRetryLimit int
	LeaderRetryWait  CfgDuration
}

type ConfigConsumer struct {
	RequestTimeout   CfgDuration
	RetryLimit       int
	RetryWait        CfgDuration
	RetryErrLimit    int
	RetryErrWait     CfgDuration
	MinFetchSize     int32
	MaxFetchSize     int32
	DefaultFetchSize int32
}

type ConfigState struct {
	Period CfgDuration
	File   string
}

type ConfigEndpoint struct {
	URL string
}

// Config is a main config structure
type Config struct {
	Global   ConfigGlobal
	Logging  ConfigLogging
	Kafka    ConfigKafka
	State    ConfigState
	Endpoint ConfigEndpoint
	Consumer ConfigConsumer
}

// SetDefaults applies default values to config structure.
func (c *Config) SetDefaults() {
	c.Global.GoMaxProcs = 0
	c.Global.Logfile = "/var/log/kafka-replicator.log"
	c.Global.Pidfile = "/run/kafka-replicator.pid"

	c.State.Period.Duration = 3 * time.Second
	c.State.File = "/tmp/kafka-replicator.state"

	c.Kafka.DialTimeout.Duration = 500 * time.Millisecond
	c.Kafka.LeaderRetryLimit = 2
	c.Kafka.LeaderRetryWait.Duration = 500 * time.Millisecond

	c.Consumer.RequestTimeout.Duration = 50 * time.Millisecond
	c.Consumer.RetryLimit = 2
	c.Consumer.RetryWait.Duration = 50 * time.Millisecond
	c.Consumer.RetryErrLimit = 2
	c.Consumer.RetryErrWait.Duration = 50 * time.Millisecond
	c.Consumer.MinFetchSize = 1
	c.Consumer.MaxFetchSize = 4194304
	c.Consumer.DefaultFetchSize = 524288

	c.Logging.Level.Level = log.InfoLevel
	c.Logging.DisableColors = true
	c.Logging.DisableTimestamp = false
	c.Logging.FullTimestamp = true
	c.Logging.DisableSorting = true
}
