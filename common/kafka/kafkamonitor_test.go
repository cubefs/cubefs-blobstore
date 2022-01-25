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

package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
)

type MockKafkaClient struct{}

func (self *MockKafkaClient) Config() *sarama.Config {
	return nil
}

func (self *MockKafkaClient) Controller() (*sarama.Broker, error) {
	return nil, nil
}

func (self *MockKafkaClient) Brokers() []*sarama.Broker {
	return nil
}

func (self *MockKafkaClient) Topics() ([]string, error) {
	return nil, nil
}

func (self *MockKafkaClient) Partitions(topic string) ([]int32, error) {
	return nil, nil
}

func (self *MockKafkaClient) WritablePartitions(topic string) ([]int32, error) {
	return nil, nil
}

func (self *MockKafkaClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	return nil, nil
}

func (self *MockKafkaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	return nil, nil
}

func (self *MockKafkaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	return nil, nil
}

func (self *MockKafkaClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	return nil, nil
}

func (self *MockKafkaClient) RefreshMetadata(topics ...string) error {
	return nil
}

func (self *MockKafkaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	if time == sarama.OffsetNewest {
		return 100, nil
	}
	return 1, nil
}

func (self *MockKafkaClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	return nil, nil
}

func (self *MockKafkaClient) RefreshCoordinator(consumerGroup string) error {
	return nil
}

func (self *MockKafkaClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	return nil, nil
}

func (self *MockKafkaClient) Close() error {
	return nil
}

func (self *MockKafkaClient) Closed() bool {
	return true
}

func TestSetConsumeOffset(t *testing.T) {
	brokens := []string{"127.0.01:9092"}
	mockTestKafkaClient = &MockKafkaClient{}
	monitor, _ := NewKafkaMonitor("TestKafkaMonitor", brokens, "Test_monitor", []int32{0, 1, 2}, 10)
	monitor.SetConsumeOffset(1, 1)
}
