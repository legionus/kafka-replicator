/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-replicator as the file COPYING.
 */

package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"

	"fmt"
)

type Zk struct {
	NodesPath      string
	PartitionsPath string
	Conn           *zk.Conn
}

func ZkInitialize(cfg *ConfigZookeeper) *Zk {
	zkConn, _, err := zk.Connect(cfg.Servers, cfg.SessionTimeout.Duration)
	if err != nil {
		logrus.Fatal("Unable to connect to zookeeper", err)
	}

	zki := &Zk{
		NodesPath:      "/replicator-" + cfg.Cluster + "/nodes",
		PartitionsPath: "/replicator-" + cfg.Cluster + "/partitions",
		Conn:           zkConn,
	}

	paths := []string{
		"/replicator-" + cfg.Cluster,
		zki.NodesPath,
		zki.PartitionsPath,
	}

	for _, path := range paths {
		exists, _, _ := zkConn.Exists(path)

		if exists {
			continue
		}

		_, err := zkConn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))

		if err != nil {
			logrus.Fatal(err)
		}
	}

	return zki
}

func ZkCreateEphemeral(conn *zk.Conn, path string, data string) error {
	_, err := conn.Create(path, []byte(data), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

func ZkRegister(zki *Zk, clientID string) {
	logrus.Info("ClientID " + clientID)
	if err := ZkCreateEphemeral(zki.Conn, zki.NodesPath+"/"+clientID, ""); err != nil {
		logrus.Fatal(err)
	}
}

func ZkNodes(zki *Zk) ([]string, error) {
	children, _, err := zki.Conn.Children(zki.NodesPath)
	return children, err
}

func ZkPartitions(zki *Zk) ([]string, error) {
	children, _, err := zki.Conn.Children(zki.PartitionsPath)
	return children, err
}

func ZkCreatePartition(zki *Zk, topic string, partition int32) error {
	path := fmt.Sprintf("%s/%s-%d", zki.PartitionsPath, topic, partition)
	return ZkCreateEphemeral(zki.Conn, path, "")
}

func ZkDeletePartition(zki *Zk, topic string, partition int32) error {
	path := fmt.Sprintf("%s/%s-%d", zki.PartitionsPath, topic, partition)
	return zki.Conn.Delete(path, 0)
}
