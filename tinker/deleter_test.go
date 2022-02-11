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

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/common/counter"
	errcode "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/kafka"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/tinker/base"
	"github.com/cubefs/blobstore/tinker/client"
	"github.com/cubefs/blobstore/util/taskpool"
)

func newDeleteTopicConsumer(t *testing.T) *deleteTopicConsumer {
	ctr := gomock.NewController(t)
	mockCmClient := NewMockClusterMgrAPI(ctr)
	mockCmClient.EXPECT().GetConfig(gomock.Any(), gomock.Any()).AnyTimes().Return("", nil)

	volCache := NewMockVolumeCache(ctr)
	volCache.EXPECT().Get(gomock.Any()).AnyTimes().DoAndReturn(
		func(vid proto.Vid) (*client.VolInfo, error) {
			return &client.VolInfo{Vid: vid}, nil
		},
	)

	switchMgr := taskswitch.NewSwitchMgr(mockCmClient)
	taskSwitch, err := switchMgr.AddSwitch(taskswitch.BlobDeleteSwitchName)
	require.NoError(t, err)

	mockBlobnode := NewMockBlobnodeAPI(ctr)
	mockBlobnode.EXPECT().MarkDelete(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	mockBlobnode.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	mockProducer := NewMockProducer(ctr)
	mockProducer.EXPECT().SendMessage(gomock.Any()).AnyTimes().Return(nil)
	mockConsumer := NewMockConsumer(ctr)

	mockDelLogger := &mockEncoder{}
	tp := taskpool.New(2, 2)

	return &deleteTopicConsumer{
		taskSwitch:     taskSwitch,
		topicConsumers: []base.IConsumer{mockConsumer},
		taskPool:       &tp,

		consumeIntervalMs: time.Duration(0),
		safeDelayTime:     time.Hour,
		volCache:          volCache,
		blobnodeCli:       mockBlobnode,
		failMsgSender:     mockProducer,

		delSuccessCounter:      base.NewCounter(1, "delete", base.KindSuccess),
		delSuccessCounterByMin: &counter.CounterByMin{},
		delFailCounter:         base.NewCounter(1, "delete", base.KindFailed),
		delFailCounterByMin:    &counter.CounterByMin{},
		errStatsDistribution:   base.NewErrorStats(),
		delLogger:              mockDelLogger,
	}
}

func TestDeleteTopicConsumer(t *testing.T) {
	ctr := gomock.NewController(t)
	mockTopicConsumeDelete := newDeleteTopicConsumer(t)

	consumer := mockTopicConsumeDelete.topicConsumers[0].(*MockConsumer)
	consumer.EXPECT().CommitOffset(gomock.Any()).AnyTimes().Return(nil)

	{
		// nothing todo
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).Return([]*sarama.ConsumerMessage{})
		mockTopicConsumeDelete.consumeAndDelete(consumer, 0)
	}
	{
		// return one invalid message
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 1)
	}
	{
		// return 2 same messages and consume one time
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "123456"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs, kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
	}
	{
		// return 2 diff messages adn consume success
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "msg1"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}

				msg2 := proto.DeleteMsg{Bid: 1, Vid: 1, ReqId: "msg2"}
				msgByte2, _ := json.Marshal(msg2)
				kafkaMgs2 := &sarama.ConsumerMessage{
					Value: msgByte2,
				}
				return []*sarama.ConsumerMessage{kafkaMgs, kafkaMgs2}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
	}
	{
		// return one message and delete protected
		oldCache := mockTopicConsumeDelete.volCache
		volCache := NewMockVolumeCache(ctr)
		volCache.EXPECT().Get(gomock.Any()).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		mockTopicConsumeDelete.volCache = volCache

		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{
					Bid:   2,
					Vid:   2,
					ReqId: "msg with volume return",
					Time:  time.Now().Unix() - 1,
				}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.safeDelayTime = 2 * time.Second
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
		mockTopicConsumeDelete.volCache = oldCache
	}
	{
		// return one message and blobnode delete failed
		oldCache := mockTopicConsumeDelete.volCache
		volCache := NewMockVolumeCache(ctr)
		volCache.EXPECT().Get(gomock.Any()).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		mockTopicConsumeDelete.volCache = volCache

		oldBlobNode := mockTopicConsumeDelete.blobnodeCli
		mockBlobnode := NewMockBlobnodeAPI(ctr)
		mockBlobnode.EXPECT().MarkDelete(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(errMock)
		mockTopicConsumeDelete.blobnodeCli = mockBlobnode

		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
		mockTopicConsumeDelete.volCache = oldCache
		mockTopicConsumeDelete.blobnodeCli = oldBlobNode
	}
	{
		// return one message and blobnode return ErrDiskBroken
		oldCache := mockTopicConsumeDelete.volCache
		volCache := NewMockVolumeCache(ctr)
		volCache.EXPECT().Get(gomock.Any()).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		volCache.EXPECT().Update(gomock.Any()).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		mockTopicConsumeDelete.volCache = volCache

		oldBlobNode := mockTopicConsumeDelete.blobnodeCli
		mockBlobnode := NewMockBlobnodeAPI(ctr)
		mockBlobnode.EXPECT().MarkDelete(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(errcode.ErrDiskBroken)
		mockTopicConsumeDelete.blobnodeCli = mockBlobnode

		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)
		mockTopicConsumeDelete.volCache = oldCache
		mockTopicConsumeDelete.blobnodeCli = oldBlobNode
	}
	{
		// return one message, blobnode return ErrDiskBroken, and volCache update not eql
		oldCache := mockTopicConsumeDelete.volCache
		volCache := NewMockVolumeCache(ctr)
		volCache.EXPECT().Get(gomock.Any()).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}},
				}, nil
			},
		)
		volCache.EXPECT().Update(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 1}, {Vuid: 2}},
				}, nil
			},
		)
		mockTopicConsumeDelete.volCache = volCache

		oldBlobNode := mockTopicConsumeDelete.blobnodeCli
		mockBlobnode := NewMockBlobnodeAPI(ctr)
		mockBlobnode.EXPECT().MarkDelete(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(errcode.ErrDiskBroken)
		mockTopicConsumeDelete.blobnodeCli = mockBlobnode

		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.DeleteMsg{Bid: 2, Vid: 2, ReqId: "delete failed"}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)

		volCache.EXPECT().Update(gomock.Any()).AnyTimes().DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{
					Vid:            vid,
					VunitLocations: []proto.VunitLocation{{Vuid: 2}},
				}, nil
			},
		)
		mockTopicConsumeDelete.volCache = volCache
		mockTopicConsumeDelete.consumeAndDelete(consumer, 2)

		mockTopicConsumeDelete.volCache = oldCache
		mockTopicConsumeDelete.blobnodeCli = oldBlobNode
	}
}

// comment temporary
func TestNewDeleteMgr(t *testing.T) {
	ctr := gomock.NewController(t)
	broker0 := NewBroker(t)
	defer broker0.Close()

	consumerCfg := base.KafkaConfig{
		Topic:      testTopic,
		BrokerList: []string{broker0.Addr()},
		Partitions: []int32{0},
	}

	producerCfg := kafka.ProducerCfg{
		BrokerList: []string{broker0.Addr()},
	}

	testDir, err := ioutil.TempDir(os.TempDir(), "delete_log")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	blobCfg := &BlobDeleteConfig{
		ClusterID:            0,
		TaskPoolSize:         2,
		NormalTopic:          consumerCfg,
		FailTopic:            consumerCfg,
		FailMsgSender:        producerCfg,
		NormalHandleBatchCnt: 10,
		FailHandleBatchCnt:   10,
		DelLog: recordlog.Config{
			Dir:       testDir,
			ChunkBits: 22,
		},
	}

	mockCmClient := NewMockClusterMgrAPI(ctr)
	volCache := NewMockVolumeCache(ctr)
	mockBlobnode := NewMockBlobnodeAPI(ctr)
	accessor := NewMockOffsetAccessor(ctr)
	accessor.EXPECT().Get(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	accessor.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	switchMgr := taskswitch.NewSwitchMgr(mockCmClient)

	service, err := NewDeleteMgr(blobCfg, volCache, accessor, mockBlobnode, switchMgr)
	require.NoError(t, err)

	// run task
	service.RunTask()

	// get stats
	service.GetTaskStats()
	service.GetErrorStats()
}
