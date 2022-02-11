// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package base

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/cubefs/blobstore/common/kafka"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/util/log"
)

// KafkaTopicMonitor kafka monitor
type KafkaTopicMonitor struct {
	offsetAccessor   IOffsetAccessor
	topic            string
	partitions       []int32
	monitor          *kafka.KafkaMonitor
	monitorIntervalS int
}

// NewKafkaTopicMonitor returns kafka topic monitor
func NewKafkaTopicMonitor(cfg *KafkaConfig, access IOffsetAccessor, monitorIntervalS int) (*KafkaTopicMonitor, error) {
	consumer, err := sarama.NewConsumer(cfg.BrokerList, defaultKafkaCfg())
	if err != nil {
		return nil, err
	}

	partitions, err := consumer.Partitions(cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("get partitions: err[%w]", err)
	}

	// create kafka monitor
	monitor, err := kafka.NewKafkaMonitor(
		proto.ServiceNameTinker,
		cfg.BrokerList,
		cfg.Topic,
		partitions,
		kafka.DefauleintervalSecs)
	if err != nil {
		return nil, fmt.Errorf("new kafka monitor: broker list[%v], topic[%v], parts[%v], error[%w]",
			cfg.BrokerList, cfg.Topic, partitions, err)
	}
	return &KafkaTopicMonitor{
		monitor:          monitor,
		topic:            cfg.Topic,
		partitions:       partitions,
		offsetAccessor:   access,
		monitorIntervalS: monitorIntervalS,
	}, nil
}

// Run run kafka monitor
func (m *KafkaTopicMonitor) Run() {
	for {
		for _, pid := range m.partitions {
			off, err := m.offsetAccessor.Get(m.topic, pid)
			if err != nil {
				if err != mongo.ErrNoDocuments {
					log.Errorf("get consume offset failed: err[%v]", err)
				}
				continue
			}
			m.monitor.SetConsumeOffset(off, pid)
		}

		time.Sleep(time.Duration(m.monitorIntervalS) * time.Second)
	}
}
