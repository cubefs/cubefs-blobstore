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

package tinker

// github.com/cubefs/blobstore/tinker/... module tinker interfaces
//go:generate mockgen -destination=./base_mock_test.go -package=tinker -mock_names IConsumer=MockConsumer,IProducer=MockProducer,IVolumeCache=MockVolumeCache,IBaseMgr=MockBaseMgr github.com/cubefs/blobstore/tinker/base IConsumer,IProducer,IVolumeCache,IBaseMgr
//go:generate mockgen -destination=./client_mock_test.go -package=tinker -mock_names ClusterMgrAPI=MockClusterMgrAPI,IScheduler=MockScheduler,BlobnodeAPI=MockBlobnodeAPI,IWorker=MockWorkerCli github.com/cubefs/blobstore/tinker/client ClusterMgrAPI,IScheduler,BlobnodeAPI,IWorker
//go:generate mockgen -destination=./db_mock_test.go -package=tinker -mock_names IDatabase=MockDatabase github.com/cubefs/blobstore/tinker/db IDatabase

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"

	"github.com/cubefs/blobstore/util/log"
)

const (
	testTopic = "test_topic"
)

var (
	any     = gomock.Any()
	errMock = errors.New("fake error")
)

type mockEncoder struct{}

func (m *mockEncoder) Encode(v interface{}) error { return nil }
func (m *mockEncoder) Close() error               { return nil }

func init() {
	log.SetOutputLevel(log.Lfatal)
}

func NewBroker(t *testing.T) *sarama.MockBroker {
	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	mockFetchResponse.SetVersion(1)
	var msg sarama.ByteEncoder = []byte("FOO")
	for i := 0; i < 1000; i++ {
		mockFetchResponse.SetMessage(testTopic, 0, int64(i), msg)
	}

	broker0 := sarama.NewMockBrokerAddr(t, 0, "127.0.0.1:0")
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader(testTopic, 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(testTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(testTopic, 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	return broker0
}
