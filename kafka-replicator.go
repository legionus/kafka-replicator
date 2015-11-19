package main

import (
	"github.com/BurntSushi/toml"
	"github.com/Sirupsen/logrus"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/satori/go.uuid"

	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	addr   = flag.String("addr", "", "The address to bind to")
	config = flag.String("config", "", "Path to configuration file")
)

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

type partitionStatus struct {
	Locked bool
	Offset int64
}

func toInt32(s string) int32 {
	if s == "" {
		return 0
	}
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(i)
}

type partitionInfo map[partitionKey]*partitionStatus

func NewOffsets(topics []string) (offsets partitionInfo) {
	offsets = make(partitionInfo)

	for _, topic := range topics {
		topicInfo := strings.SplitN(topic, ":", 2)
		if len(topicInfo) != 2 {
			logrus.Fatal("Wrong topics definition:", topic)
		}

		numPartitions := toInt32(topicInfo[1])
		if numPartitions == 0 {
			logrus.Fatal("Wrong number of partitions:", topicInfo[1])
		}

		for i := int32(0); i < numPartitions; i++ {
			key := partitionKey{
				Topic:     topicInfo[0],
				Partition: i,
			}
			offsets[key] = &partitionStatus{
				Locked: false,
				Offset: 0,
			}
		}
	}

	return
}

func (offsets partitionInfo) GetCoordinatorOffsets(coordinator kafka.OffsetCoordinator) {
	for k, v := range offsets {
		if !v.Locked {
			continue
		}

		offset, _, err := coordinator.Offset(k.Topic, k.Partition)

		if err == proto.ErrUnknownTopicOrPartition {
			offset = kafka.StartOffsetNewest
		} else {
			offset++
		}

		offsets[k].Offset = offset
	}
}

func (offsets partitionInfo) GetEarliestOffsets(conn *kafka.Broker) {
	var offset int64
	var err error

	for k, v := range offsets {
		if !v.Locked {
			continue
		}

		for {
			offset, err = conn.OffsetEarliest(k.Topic, k.Partition)
			if err == nil {
				break
			}
			logrus.Errorf("Unable to obtain earliest offset (%s, %d): %+v", k.Topic, k.Partition, err)
			time.Sleep(1 * time.Second)
		}

		if offsets[k].Offset >= 0 && offsets[k].Offset < offset {
			offsets[k].Offset = offset
		}
	}
}

func (offsets partitionInfo) SetOffset(topic string, partition int32, offset int64) {
	k := partitionKey{
		Topic:     topic,
		Partition: partition,
	}

	if offset < offsets[k].Offset {
		logrus.Fatal("Try to update newer offset")
	}

	offsets[k].Offset = offset
}

func main() {
	flag.Parse()

	if *config == "" {
		*config = "/etc/kafka-replicator.cfg"
	}

	cfg := Config{}
	cfg.SetDefaults()

	if _, err := toml.DecodeFile(*config, &cfg); err != nil {
		logrus.Fatal(err)
	}

	logrus.SetLevel(cfg.Logging.Level.Level)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:    cfg.Logging.FullTimestamp,
		DisableTimestamp: cfg.Logging.DisableTimestamp,
		DisableColors:    cfg.Logging.DisableColors,
		DisableSorting:   cfg.Logging.DisableSorting,
	})

	pidfile, err := OpenPidfile(cfg.Global.Pidfile)
	if err != nil {
		logrus.Fatal("Unable to open pidfile: ", err.Error())
	}
	defer pidfile.Close()

	if err := pidfile.Check(); err != nil {
		logrus.Fatal("Check failed: ", err.Error())
	}

	if err := pidfile.Write(); err != nil {
		logrus.Fatalf("Unable to write pidfile: %+v", err.Error())
	}

	logfile, err := OpenLogfile(cfg.Global.Logfile)
	if err != nil {
		logrus.Fatalf("Unable to open log: %+v", err.Error())
	}
	defer logfile.Close()
	logrus.SetOutput(logfile)

	clientID := uuid.NewV4().String()

	zki := ZkInitialize(&cfg.Zookeeper)
	ZkRegister(zki, clientID)

	brokerConf := kafka.NewBrokerConf("kafka-replicator")
	brokerConf.DialTimeout = cfg.Kafka.DialTimeout.Duration
	brokerConf.LeaderRetryLimit = cfg.Kafka.LeaderRetryLimit
	brokerConf.LeaderRetryWait = cfg.Kafka.LeaderRetryWait.Duration

	conns := make(map[string]*kafka.Broker)
	conns["offsets"] = nil
	conns["coordinator"] = nil

	for k := range conns {
		conns[k], err = kafka.Dial(cfg.Kafka.Brokers, brokerConf)
		if err != nil {
			logrus.Fatalf("Unable to connect to kafka: %+v", err)
		}
		defer conns[k].Close()
	}

	coordConf := kafka.NewOffsetCoordinatorConf("replicator-" + cfg.Zookeeper.Cluster)
	coordinator, err := conns["coordinator"].OffsetCoordinator(coordConf)

	if err != nil {
		logrus.Fatalf("Unable to create coordinator: %+v", err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
		},
	}

	offsets := NewOffsets(cfg.Kafka.Topics)
	numOffsets := len(offsets)

	numLocked := 0
	numNodes := 0
	numPartitions := 0
	prevNodes := 0

	var nodes []string
	var partitions []string

	for {
		if nodes, err = ZkNodes(zki); err != nil {
			logrus.Fatalf("Unable to get nodes: %+v", err)
		}
		numNodes = len(nodes)

		if numNodes == 0 {
			logrus.Fatal("Nodes not found")
		}

		if partitions, err = ZkPartitions(zki); err != nil {
			logrus.Fatalf("Unable to get partitions: %+v", err)
		}

		if prevNodes != numNodes || numOffsets != len(partitions) {
			// FIXME numOffsets < numNodes
			numPartitions = int(math.Ceil(float64(numOffsets) / float64(numNodes)))

			if prevNodes < numNodes {
				needUnlock := numLocked - numPartitions

				for k, v := range offsets {
					if needUnlock <= 0 {
						break
					}
					if !v.Locked {
						continue
					}
					if err := ZkDeletePartition(zki, k.Topic, k.Partition); err != nil {
						logrus.Fatalf("Unable to remove partition: %+v", err)
					}
					offsets[k].Locked = false
					offsets[k].Offset = 0

					needUnlock--
					numLocked--
				}
			}

			for {
				if partitions, err = ZkPartitions(zki); err != nil {
					logrus.Fatalf("Unable to get partitions: %+v", err)
				}

				if numOffsets == len(partitions) || numLocked >= numPartitions {
					break
				}

				for k, v := range offsets {
					if v.Locked {
						continue
					}
					err := ZkCreatePartition(zki, k.Topic, k.Partition, clientID)

					if err != nil {
						if err == zk.ErrNodeExists {
							continue
						}
						logrus.Fatalf("Unable to create partition: %+v", err)
					}

					offsets[k].Locked = true
					numLocked++
				}
			}

			offsets.GetCoordinatorOffsets(coordinator)

			prevNodes = numNodes
		}

		logrus.Infof("numNodes=%d numPartitions=%d", numNodes, numPartitions)

		conns["consumer"], err = kafka.Dial(cfg.Kafka.Brokers, brokerConf)
		if err != nil {
			logrus.Fatalf("Unable to connect to kafka: %+v", err)
		}

		var fetchers []kafka.Consumer

		for k, v := range offsets {
			if !v.Locked {
				continue
			}

			conf := kafka.NewConsumerConf(k.Topic, k.Partition)
			conf.StartOffset = v.Offset

			conf.RequestTimeout = cfg.Consumer.RequestTimeout.Duration
			conf.RetryLimit = cfg.Consumer.RetryLimit
			conf.RetryWait = cfg.Consumer.RetryWait.Duration
			conf.RetryErrLimit = cfg.Consumer.RetryErrLimit
			conf.RetryErrWait = cfg.Consumer.RetryErrWait.Duration
			conf.MinFetchSize = cfg.Consumer.MinFetchSize
			conf.MaxFetchSize = cfg.Consumer.MaxFetchSize

			consumer, err := conns["consumer"].Consumer(conf)
			if err != nil {
				logrus.Fatalf("Unable make consumer: %+v", err)
			}

			fetchers = append(fetchers, consumer)
		}

		if len(fetchers) == 0 {
			continue
		}

		mx := kafka.Merge(fetchers...)
		delay := time.Now().Add(3 * time.Second)

		for {
			if time.Now().After(delay) {
				if nodes, err = ZkNodes(zki); err != nil {
					logrus.Fatalf("Unable to get nodes: %+v", err)
				}

				if prevNodes != len(nodes) {
					break
				}

				delay = time.Now().Add(3 * time.Second)
			}

			result := make(chan struct{})
			timeout := make(chan struct{})

			timer := time.AfterFunc(4 * time.Second, func() { close(timeout) })

			var msg *proto.Message
			var err error

			go func() {
				msg, err = mx.Consume()
				close(result)
			}()

			select {
			case <-result:
			case <-timeout:
				continue
			}

			timer.Stop()

			if err != nil {
				if err == proto.ErrOffsetOutOfRange {
					offsets.GetEarliestOffsets(conns["offsets"])
					continue
				}
				if err != kafka.ErrMxClosed {
					logrus.Errorf("Unable to consume: %+v", err)
				}
				break
			}

			var m kafkaMessage
			if err = json.Unmarshal(msg.Value, &m); err != nil {
				logrus.Errorf("Unable to parse message: %+v", err)
				continue
			}

			endpointURL := cfg.Endpoint.URL + "?queue=" + msg.Topic
			for {
				resp, err := client.Post(endpointURL, "application/json", bytes.NewReader(m.Data))
				if err == nil {
					resp.Body.Close()
					break
				}
				logrus.Errorf("Unable to sent event to endpoint: %+v", err)
			}

			if err := coordinator.Commit(msg.Topic, msg.Partition, msg.Offset); err != nil {
				logrus.Fatalf("Unable to commit offset: %+v", err)
			}

			offsets.SetOffset(msg.Topic, msg.Partition, msg.Offset)

			logrus.NewEntry(logrus.StandardLogger()).
				WithField("topic", msg.Topic).
				WithField("partition", msg.Partition).
				WithField("offset", msg.Offset).
				Info("Received message")
		}

		mx.Close()
		conns["consumer"].Close()
	}
}
