package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/optiopay/kafka"
	"github.com/BurntSushi/toml"

	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"time"
	"net/http"
	"runtime"
	"sync"
)

var (
	addr   = flag.String("addr", "", "The address to bind to")
	config = flag.String("config", "", "Path to configuration file")
)

type serverLogger struct {
	subsys string
}

func (l *serverLogger) Debug(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Debugf("[%s] %s", l.subsys, msg)
}

func (l *serverLogger) Info(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Infof("[%s] %s", l.subsys, msg)
}

func (l *serverLogger) Warn(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Warningf("[%s] %s", l.subsys, msg)
}

func (l *serverLogger) Error(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Errorf("[%s] %s", l.subsys, msg)
}

func (l *serverLogger) Fatal(msg string, args ...interface{}) {
	e := log.NewEntry(log.StandardLogger())

	for i := 0; i < len(args); i += 2 {
		k := fmt.Sprintf("%+v", args[i])
		e = e.WithField(k, args[i+1])
	}

	e.Fatalf("[%s] %s", l.subsys, msg)
}

type senderInfo struct {
	Project string
	Cluster string
	IP      string
}

type kafkaMessage struct {
	ID       string
	Accepted int64
	Sender   senderInfo
	Data     json.RawMessage
}

type partitionKey struct {
	Topic     string
	Partition int32
}

type lastOffsets struct {
	sync.RWMutex
	Last map[partitionKey]int64
}

// Server is a main structure.
type Server struct {
	Cfg     *Config
	Log     *serverLogger
	Offsets *lastOffsets
}

func (s *Server) run() error {
	brokerConf := kafka.NewBrokerConf("kafka-replicator")
	brokerConf.DialTimeout = s.Cfg.Kafka.DialTimeout.Duration
	brokerConf.LeaderRetryLimit = s.Cfg.Kafka.LeaderRetryLimit
	brokerConf.LeaderRetryWait = s.Cfg.Kafka.LeaderRetryWait.Duration

	broker, err := kafka.Dial(s.Cfg.Kafka.Brokers, brokerConf)
	if err != nil {
		s.Log.Fatal("Unable to connect to kafka", "err", err)
	}
	defer broker.Close()

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}

	client := &http.Client{
		Transport: transCfg,
	}

	for {
		var fetchers []kafka.Consumer

		s.Offsets.Lock()
		for k, offset := range s.Offsets.Last {
			conf := kafka.NewConsumerConf(k.Topic, k.Partition)

			conf.RequestTimeout = s.Cfg.Consumer.RequestTimeout.Duration
			conf.RetryLimit = s.Cfg.Consumer.RetryLimit
			conf.RetryWait = s.Cfg.Consumer.RetryWait.Duration
			conf.RetryErrLimit = s.Cfg.Consumer.RetryErrLimit
			conf.RetryErrWait = s.Cfg.Consumer.RetryErrWait.Duration
			conf.MinFetchSize = s.Cfg.Consumer.MinFetchSize
			conf.MaxFetchSize = s.Cfg.Consumer.MaxFetchSize

			conf.StartOffset = offset

			consumer, err := broker.Consumer(conf)
			if err != nil {
				s.Log.Fatal("Unable make consumer", "err", err)
			}

			fetchers = append(fetchers, consumer)
		}
		s.Offsets.Unlock()

		mx := kafka.Merge(fetchers...)

		for {
			msg, err := mx.Consume()
			if err != nil {
				s.Log.Error("Unable to consume", "err", err)
				break
			}

			key := partitionKey{
				Topic:     msg.Topic,
				Partition: msg.Partition,
			}

			s.Log.Info("Received message",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)

			var m kafkaMessage
			if err := json.Unmarshal(msg.Value, &m); err != nil {
				s.Log.Error("Unable to parse message", "err", err)
				continue
			}

			var endpointURL = s.Cfg.Endpoint.URL+"?queue="+msg.Topic
			for {
				resp, err := client.Post(
					endpointURL,
					"application/json",
					bytes.NewReader(m.Data),
				)
				if err == nil {
					resp.Body.Close()
					break
				}
				s.Log.Error("Unable to sent event to endpoint", "err", err)
			}

			s.Offsets.Lock()
			s.Offsets.Last[key] = msg.Offset + 1
			s.Offsets.Unlock()
		}

		mx.Close()
	}
}

func main() {
	flag.Parse()

	if *config == "" {
		*config = "/etc/kafka-replicator.cfg"
	}

	cfg := Config{}
	cfg.SetDefaults()

	if _, err := toml.DecodeFile(*config, &cfg); err != nil {
		log.Fatal(err)
	}

	log.SetLevel(cfg.Logging.Level.Level)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:    cfg.Logging.FullTimestamp,
		DisableTimestamp: cfg.Logging.DisableTimestamp,
		DisableColors:    cfg.Logging.DisableColors,
		DisableSorting:   cfg.Logging.DisableSorting,
	})

	pidfile, err := OpenPidfile(cfg.Global.Pidfile)
	if err != nil {
		log.Fatal("Unable to open pidfile: ", err.Error())
	}
	defer pidfile.Close()

	if err := pidfile.Check(); err != nil {
		log.Fatal("Check failed: ", err.Error())
	}

	if err := pidfile.Write(); err != nil {
		log.Fatal("Unable to write pidfile: ", err.Error())
	}

	logfile, err := OpenLogfile(cfg.Global.Logfile)
	if err != nil {
		log.Fatal("Unable to open log: ", err.Error())
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	if cfg.Global.GoMaxProcs == 0 {
		cfg.Global.GoMaxProcs = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(cfg.Global.GoMaxProcs)

	statefile := NewState(cfg.State.File)

	server := &Server{
		Cfg:     &cfg,
		Log:     &serverLogger{
			subsys: "server",
		},
		Offsets: statefile.Load(cfg.Kafka.Topics),
	}

	go func() {
		for {
			time.Sleep(cfg.State.Period.Duration)

			server.Offsets.Lock()
			statefile.Save(server.Offsets)
			server.Offsets.Unlock()
		}
	}()

	log.Fatal(server.run())
}
