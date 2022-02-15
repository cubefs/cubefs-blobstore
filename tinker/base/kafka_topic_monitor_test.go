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
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

func TestNewKafkaTopicMonitor(t *testing.T) {
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
	monitor, err := NewKafkaTopicMonitor(cfg, tbl, 0)
	go func() {
		monitor.Run()
	}()
	time.Sleep(time.Second * 3)
	require.NoError(t, err)

	cfg.BrokerList = []string{}
	monitor, err = NewKafkaTopicMonitor(cfg, tbl, 0)
	require.Error(t, err)
}
