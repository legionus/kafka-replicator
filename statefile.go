/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-replicator as the file COPYING.
 */

package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/optiopay/kafka"

	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Statefile struct {
	sync.RWMutex
	Name string
}

func NewState(name string) *Statefile {
	return &Statefile{
		Name: name,
	}
}

func (f *Statefile) Load(topics []string) *lastOffsets {
	f.Lock()
	defer f.Unlock()

	offsets := make(map[partitionKey]int64)

	for _, topic := range topics {
		topicInfo := strings.SplitN(topic, ":", 2)
		if len(topicInfo) != 2 {
			log.Fatal("Wrong topics definition:", topic)
		}

		numPartitions := toInt32(topicInfo[1])
		if numPartitions == 0 {
			log.Fatal("Wrong number of partitions:", topicInfo[1])
		}

		for i := int32(0); i < numPartitions; i++ {
			key := partitionKey{
				Topic:     topicInfo[0],
				Partition: i,
			}
			offsets[key] = kafka.StartOffsetNewest
		}
	}

	fd, err := os.OpenFile(f.Name, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("error opening file: ", err)
	}

	var topic string
	var partition int32
	var offset int64

	for {
		n, err := fmt.Fscanf(fd, "%s\t%d\t%d\n", &topic, &partition, &offset)
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		if n != 3 {
			log.Fatal("error parsing state file: ", f.Name)
		}

		key := partitionKey{
			Topic:     topic,
			Partition: partition,
		}

		if _, ok := offsets[key]; !ok {
			continue
		}

		offsets[key] = offset
	}

	fd.Close()

	return &lastOffsets{
		Last: offsets,
	}
}

func (f *Statefile) Save(state *lastOffsets) error {
	f.Lock()
	defer f.Unlock()

	fd, err := os.OpenFile(f.Name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	for k, offset := range state.Last {
		if _, err := fd.WriteString(fmt.Sprintf("%s\t%d\t%d\n", k.Topic, k.Partition, offset)); err != nil {
			return err
		}
	}

	if err := fd.Sync(); err != nil {
		return err
	}

	fd.Close()

	return nil
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
