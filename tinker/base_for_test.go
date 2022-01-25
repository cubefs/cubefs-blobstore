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
//go:generate mockgen -destination=./base_mock_test.go -package=tinker -mock_names IConsumer=MockConsumer,IProducer=MockProducer,IVolumeCache=MockVolumeCache,IOffsetAccessor=MockOffsetAccessor,IBaseMgr=MockBaseMgr github.com/cubefs/blobstore/tinker/base IConsumer,IProducer,IVolumeCache,IOffsetAccessor,IBaseMgr
//go:generate mockgen -destination=./client_mock_test.go -package=tinker -mock_names ClusterMgrAPI=MockClusterMgrAPI,IScheduler=MockScheduler,BlobNodeAPI=MockBlobNodeAPI,IWorker=MockWorkerCli github.com/cubefs/blobstore/tinker/client ClusterMgrAPI,IScheduler,BlobNodeAPI,IWorker
//go:generate mockgen -destination=./db_mock_test.go -package=tinker -mock_names IOrphanedShardTbl=MockOrphanedShardTbl github.com/cubefs/blobstore/tinker/db IOrphanedShardTbl

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
)

var errMock = errors.New("fake failed")

type MockEncoder struct {
	M []DelDoc
}

func (m *MockEncoder) Encode(v interface{}) error {
	v2 := v.(DelDoc)
	m.M = append(m.M, v2)
	return nil
}

func (m *MockEncoder) Close() error {
	return nil
}

func (m *MockEncoder) Clean() {
	m.M = nil
}

func NewBroker(t *testing.T) *sarama.MockBroker {
	broker0 := sarama.NewMockBrokerAddr(t, 0, "127.0.0.1:0")

	var msg sarama.ByteEncoder = []byte("FOO")

	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	mockFetchResponse.SetVersion(1)
	for i := 0; i < 1000; i++ {
		mockFetchResponse.SetMessage("my_topic", 0, int64(i), msg)
	}

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})
	return broker0
}
