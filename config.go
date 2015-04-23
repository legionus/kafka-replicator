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

// Config is a main config structure
type Config struct {
	Global struct {
		Logfile    string
		Pidfile    string
		GoMaxProcs int
	}
	Kafka struct {
		Broker []string
		Topic  []string
	}
	State struct {
		Period CfgDuration
		File   string
	}
	Endpoint struct {
		URL     string
	}
	Broker struct {
		NumConns         int64
		DialTimeout      CfgDuration
		LeaderRetryLimit int
		LeaderRetryWait  CfgDuration
		ReconnectTimeout CfgDuration
	}
	Consumer struct {
		RequestTimeout   CfgDuration
		RetryLimit       int
		RetryWait        CfgDuration
		RetryErrLimit    int
		RetryErrWait     CfgDuration
		MinFetchSize     int32
		MaxFetchSize     int32
		DefaultFetchSize int32
	}
	Logging struct {
		Level            CfgLogLevel
		DisableColors    bool
		DisableTimestamp bool
		FullTimestamp    bool
		DisableSorting   bool
	}
}

// SetDefaults applies default values to config structure.
func (c *Config) SetDefaults() {
	c.Global.GoMaxProcs = 0
	c.Global.Logfile = "/var/log/kafka-replicator.log"
	c.Global.Pidfile = "/run/kafka-replicator.pid"

	c.State.Period.Duration = 3 * time.Second
	c.State.File = "/tmp/kafka-replicator.state"

	c.Broker.DialTimeout.Duration = 500 * time.Millisecond
	c.Broker.LeaderRetryLimit = 2
	c.Broker.LeaderRetryWait.Duration = 500 * time.Millisecond
	c.Broker.ReconnectTimeout.Duration = 15 * time.Second

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
