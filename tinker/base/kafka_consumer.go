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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/cubefs/blobstore/common/kafka"
	"github.com/cubefs/blobstore/common/trace"
)

// MinConsumeWaitTime default min consume wait time
const MinConsumeWaitTime = time.Millisecond * 500

// IConsumer define the interface of consumer for message consume
type IConsumer interface {
	ConsumeMessages(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage)
	CommitOffset(ctx context.Context) error
}

// KafkaConfig kafka config
type KafkaConfig struct {
	Topic      string   `json:"topic"`
	Partitions []int32  `json:"partitions"`
	BrokerList []string `json:"-"`
}

// ConsumeInfo consume info
type ConsumeInfo struct {
	Offset int64
	Commit int64
}

// TopicConsumer rotate consume msg among partition consumers
type TopicConsumer struct {
	topic               string
	partitionsConsumers []IConsumer

	curIdx int
}

// NewTopicConsumer returns topic consumer
func NewTopicConsumer(cfg *KafkaConfig, offAccessor IOffsetAccessor) (IConsumer, error) {
	consumers, err := NewKafkaPartitionConsumers(cfg, offAccessor)
	if err != nil {
		return nil, err
	}

	topicConsumer := &TopicConsumer{
		partitionsConsumers: consumers,
		topic:               cfg.Topic,
	}

	return topicConsumer, err
}

// ConsumeMessages consumer messages
func (c *TopicConsumer) ConsumeMessages(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
	msgs = c.partitionsConsumers[c.curIdx].ConsumeMessages(ctx, msgCnt)
	c.curIdx = (c.curIdx + 1) % len(c.partitionsConsumers)
	return
}

// CommitOffset commit offset
func (c *TopicConsumer) CommitOffset(ctx context.Context) error {
	for _, pc := range c.partitionsConsumers {
		err := pc.CommitOffset(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// PartitionConsumer partition consumer
type PartitionConsumer struct {
	topic       string
	ptID        int32
	ptConsumer  sarama.PartitionConsumer
	consumeInfo ConsumeInfo

	// consume offset persist
	offset IOffsetAccessor
}

// NewKafkaPartitionConsumers returns kafka partition consumers
func NewKafkaPartitionConsumers(cfg *KafkaConfig, offAccessor IOffsetAccessor) ([]IConsumer, error) {
	if len(cfg.Partitions) == 0 {
		return nil, errors.New("empty partition id")
	}

	var consumers []IConsumer
	consumer, err := sarama.NewConsumer(cfg.BrokerList, defaultKafkaCfg())
	if err != nil {
		return nil, fmt.Errorf("new consumer: err[%w]", err)
	}

	for _, ptID := range cfg.Partitions {
		partitionConsumer, err := newKafkaPartitionConsumer(consumer, cfg.Topic, ptID, offAccessor)
		if err != nil {
			return nil, fmt.Errorf("new kafka partition consumer: err[%w]", err)
		}
		consumers = append(consumers, partitionConsumer)
	}

	return consumers, nil
}

func newKafkaPartitionConsumer(consumer sarama.Consumer, topic string, ptID int32, offset IOffsetAccessor) (*PartitionConsumer, error) {
	kafkaConsumer := PartitionConsumer{
		topic:  topic,
		offset: offset,
	}

	ptConsumeInfo, err := kafkaConsumer.loadConsumeInfo(kafkaConsumer.topic, ptID)
	if err != nil {
		return nil, fmt.Errorf("loadConsumeInfo: topic[%s], err[%w]", kafkaConsumer.topic, err)
	}

	pc, err := consumer.ConsumePartition(kafkaConsumer.topic, ptID, ptConsumeInfo.Commit)
	if err != nil {
		return nil, fmt.Errorf("consume partition: topic[%s], ptID[%d], ptConsumeInfo[%+v], err[%w]", topic, ptID, ptConsumeInfo, err)
	}

	kafkaConsumer.ptID = ptID
	kafkaConsumer.ptConsumer = pc
	kafkaConsumer.consumeInfo = ptConsumeInfo

	return &kafkaConsumer, nil
}

// ConsumeMessages consume messages
func (c *PartitionConsumer) ConsumeMessages(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
	span := trace.SpanFromContextSafe(ctx)

	d := time.Millisecond / 2 * time.Duration(msgCnt) // assume each message cost 0.5 ms
	if d < MinConsumeWaitTime {
		d = MinConsumeWaitTime
	}

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	start := time.Now()

	var err error
	for {
		var msg *sarama.ConsumerMessage
		select {
		case msg = <-c.ptConsumer.Messages():
		case err = <-c.ptConsumer.Errors():
		case <-ticker.C:
			break
		}
		if err != nil {
			span.Errorf("acquire msg failed: topic[%s], pid[%d], err[%+v]", c.topic, c.ptID, err)
			break
		}

		if msg == nil {
			span.Debugf("no message for consume and return")
			break // consume finish,return
		}

		c.consumeInfo.Offset = msg.Offset
		msgs = append(msgs, msg)
		if len(msgs) >= msgCnt {
			break
		}
	}

	span.Debugf("consume info: topic[%s], pid[%d], time cost[%+v], consumer msg numbers[%d], offset[%d], batch msg cnt[%d]",
		c.topic,
		c.ptID,
		time.Since(start),
		len(msgs),
		c.consumeInfo.Offset,
		msgCnt)
	return
}

// CommitOffset commit offset
func (c *PartitionConsumer) CommitOffset(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("start commit offset: offset[%d], topic[%s], pid[%d]", c.consumeInfo.Offset, c.topic, c.ptID)
	err := c.offset.Put(c.topic, c.ptID, c.consumeInfo.Offset)
	if err != nil {
		span.Errorf("commit offset failed: [%+v]", err)
		return err
	}
	c.consumeInfo.Commit = c.consumeInfo.Offset
	return nil
}

func (c *PartitionConsumer) loadConsumeInfo(topic string, pt int32) (consumeInfo ConsumeInfo, err error) {
	commitOffset, err := c.offset.Get(topic, pt)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return ConsumeInfo{Commit: sarama.OffsetOldest, Offset: sarama.OffsetOldest}, nil
		}
		return
	}

	return ConsumeInfo{Commit: commitOffset + 1, Offset: commitOffset}, err
}

func defaultKafkaCfg() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = kafka.DefaultKafkaVersion
	cfg.Consumer.Return.Errors = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = sarama.CompressionSnappy
	return cfg
}
