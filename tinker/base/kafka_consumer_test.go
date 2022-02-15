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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/cubefs/blobstore/util/errors"
)

const (
	testTopic = "test_topic"
)

var ErrMock = errors.New("mock err")

type mockPartitionConsumer struct {
	topic  string
	pid    int32
	offset int64

	ch    chan *sarama.ConsumerMessage
	errCh chan *sarama.ConsumerError
}

func newMockPartitionConsumer(topic string, pid int32) *mockPartitionConsumer {
	return &mockPartitionConsumer{
		topic:  topic,
		pid:    pid,
		offset: 0,
		ch:     make(chan *sarama.ConsumerMessage),
		errCh:  make(chan *sarama.ConsumerError),
	}
}

func (m *mockPartitionConsumer) sendMsg(key, val string) {
	msg := sarama.ConsumerMessage{
		Key:       []byte(key),
		Value:     []byte(val),
		Topic:     m.topic,
		Partition: m.pid,
		Offset:    m.offset,
	}
	m.offset++
	m.ch <- &msg
}

func (m *mockPartitionConsumer) sendErr(err error) {
	CErr := sarama.ConsumerError{
		Topic:     m.topic,
		Partition: m.pid,
		Err:       err,
	}
	m.errCh <- &CErr
}

func (m *mockPartitionConsumer) AsyncClose() {}

func (m *mockPartitionConsumer) Close() error {
	return nil
}

func (m *mockPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return m.ch
}

func (m *mockPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return m.errCh
}

func (m *mockPartitionConsumer) HighWaterMarkOffset() int64 {
	return 0
}

type mockConsumer struct {
	topics map[string][]*mockPartitionConsumer
}

func newMockConsumer() *mockConsumer {
	return &mockConsumer{
		topics: map[string][]*mockPartitionConsumer{
			"topic1": newBatchpc("topic1", []int32{1, 2, 3, 4}),
			"topic2": newBatchpc("topic2", []int32{1, 2, 3, 4}),
		},
	}
}

func newBatchpc(topic string, pids []int32) (ret []*mockPartitionConsumer) {
	for _, pid := range pids {
		pc := newMockPartitionConsumer(topic, pid)
		ret = append(ret, pc)
	}
	return ret
}

func (m *mockConsumer) Run(msgCnt int) {
	for _, pcs := range m.topics {
		for _, pc := range pcs {
			tmpPc := pc
			go func() {
				for i := 1; i <= msgCnt; i++ {
					tmpPc.sendMsg(fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i))
				}
			}()
		}
	}
	fmt.Printf("run end\n")
}

func (m *mockConsumer) getPc(topic string, pid int32) *mockPartitionConsumer {
	pcs := m.topics[topic]
	for _, pc := range pcs {
		if pc.pid == pid {
			return pc
		}
	}
	return nil
}

func (m *mockConsumer) Topics() ([]string, error) {
	var ret []string
	for topic := range m.topics {
		ret = append(ret, topic)
	}
	return ret, nil
}

func (m *mockConsumer) Partitions(topic string) ([]int32, error) {
	var pids []int32
	for _, pc := range m.topics[topic] {
		pids = append(pids, pc.pid)
	}
	return pids, nil
}

func (m *mockConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	for _, pc := range m.topics[topic] {
		if pc.pid == partition {
			return pc, nil
		}
	}
	return nil, errors.New("not found")
}

func (m *mockConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

func (m *mockConsumer) Close() error {
	return nil
}

type mockAccess struct {
	offsets map[string]int64
	retErr  error
}

func newMockAccess(err error) *mockAccess {
	return &mockAccess{
		offsets: make(map[string]int64),
		retErr:  err,
	}
}

func (m *mockAccess) Set(topic string, partition int32, off int64) error {
	key := fmt.Sprintf("%s_%d", topic, partition)
	m.offsets[key] = off
	return m.retErr
}

func (m *mockAccess) Get(topic string, partition int32) (int64, error) {
	key := fmt.Sprintf("%s_%d", topic, partition)
	return m.offsets[key], m.retErr
}

type mockKafkaOffsetTable struct {
	mockErr error
	offset  int64
}

func (m *mockKafkaOffsetTable) Set(topic string, partition int32, off int64) error {
	if m.mockErr != nil {
		return m.mockErr
	}
	atomic.AddInt64(&m.offset, off)
	return nil
}

func (m *mockKafkaOffsetTable) Get(topic string, partition int32) (int64, error) {
	if m.mockErr != nil {
		return 0, m.mockErr
	}
	return atomic.LoadInt64(&m.offset), nil
}

func TestPtConsumer(t *testing.T) {
	defaultKafkaCfg()
	mockConsume := newMockConsumer()
	access := newMockAccess(nil)
	pc, err := newKafkaPartitionConsumer(mockConsume, "topic1", 1, access)
	require.NoError(t, err)
	mockConsume.Run(100)
	msgs := pc.ConsumeMessages(context.Background(), 5)
	require.Equal(t, 5, len(msgs))

	err = pc.CommitOffset(context.Background())
	require.NoError(t, err)
	fmt.Printf("msgs %s %s\n", msgs[0].Key, msgs[0].Value)
	fmt.Printf("msgs %s %s\n", msgs[1].Key, msgs[1].Value)
	fmt.Printf("access %+v", *access)
}

func TestTopicConsume(t *testing.T) {
	var cs []IConsumer
	mockConsume := newMockConsumer()
	access := newMockAccess(nil)
	pc, _ := newKafkaPartitionConsumer(mockConsume, "topic1", 1, access)
	cs = append(cs, pc)
	pc, _ = newKafkaPartitionConsumer(mockConsume, "topic1", 2, access)
	cs = append(cs, pc)
	pc, _ = newKafkaPartitionConsumer(mockConsume, "topic1", 3, access)
	cs = append(cs, pc)

	mockConsume.Run(100)

	topicConsumer := &TopicConsumer{
		topic:               "topic1",
		partitionsConsumers: cs,
	}
	topicConsumer.ConsumeMessages(context.Background(), 1)
	err := topicConsumer.CommitOffset(context.Background())
	require.NoError(t, err)
	off, err := access.Get("topic1", 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	topicConsumer.ConsumeMessages(context.Background(), 1)
	err = topicConsumer.CommitOffset(context.Background())
	require.NoError(t, err)
	off, err = access.Get("topic1", 2)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	topicConsumer.ConsumeMessages(context.Background(), 1)
	err = topicConsumer.CommitOffset(context.Background())
	require.NoError(t, err)
	off, err = access.Get("topic1", 3)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	topicConsumer.ConsumeMessages(context.Background(), 1)
	err = topicConsumer.CommitOffset(context.Background())
	require.NoError(t, err)
	off, err = access.Get("topic1", 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), off)
}

func TestConsumerErr(t *testing.T) {
	mockConsume := newMockConsumer()
	access := newMockAccess(nil)
	pc, err := newKafkaPartitionConsumer(mockConsume, "topic1", 1, access)
	require.NoError(t, err)
	pcTmp := mockConsume.getPc("topic1", 1)
	go func() { pcTmp.sendErr(errors.New("fake error")) }()

	msgs := pc.ConsumeMessages(context.Background(), 1)
	require.Equal(t, 0, len(msgs))
}

func TestLoadConsumeInfo(t *testing.T) {
	mockConsume := newMockConsumer()
	access := newMockAccess(mongo.ErrNoDocuments)
	pc, _ := newKafkaPartitionConsumer(mockConsume, "topic1", 1, access)
	off, _ := pc.loadConsumeInfo("topic1", 1)
	require.Equal(t, sarama.OffsetOldest, off.Offset)
}

func TestNewTopicConsumer(t *testing.T) {
	// Given
	broker0 := sarama.NewMockBrokerAddr(t, 0, "127.0.0.1:0")

	var msg sarama.ByteEncoder = []byte("FOO")

	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	mockFetchResponse.SetVersion(1)
	for i := 0; i < 1000; i++ {
		mockFetchResponse.SetMessage(testTopic, 0, int64(i), msg)
	}

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader(testTopic, 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(testTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(testTopic, 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})

	defer broker0.Close()
	cfg := &KafkaConfig{
		Topic:      testTopic,
		BrokerList: []string{broker0.Addr()},
		Partitions: []int32{0},
	}

	tbl := &mockKafkaOffsetTable{}
	consumer, err := NewTopicConsumer(cfg, tbl)
	require.NoError(t, err)

	msgs := consumer.ConsumeMessages(context.Background(), 1)
	require.Equal(t, 1, len(msgs))

	tbl.mockErr = ErrMock
	err = consumer.CommitOffset(context.Background())
	require.Error(t, err)

	cfg.BrokerList = []string{}
	_, err = NewTopicConsumer(cfg, tbl)
	require.Error(t, err)

	cfg.Partitions = nil
	cfg.BrokerList = []string{broker0.Addr()}
	_, err = NewTopicConsumer(cfg, tbl)
	require.Error(t, err)
}

func TestNewCounter(t *testing.T) {
	counter := NewCounter(0, "", "")
	require.NotNil(t, counter)
}
