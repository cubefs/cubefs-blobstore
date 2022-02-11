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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	errcode "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/kafka"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/blobstore/tinker/base"
	"github.com/cubefs/blobstore/tinker/client"
	"github.com/cubefs/blobstore/util/taskpool"
)

func newShardRepairMgr(t *testing.T) *ShardRepairMgr {
	ctr := gomock.NewController(t)

	volCache := NewMockVolumeCache(ctr)
	volCache.EXPECT().Get(gomock.Any()).AnyTimes().Return(&client.VolInfo{}, nil)
	volCache.EXPECT().Update(gomock.Any()).AnyTimes().Return(&client.VolInfo{}, nil)

	selector := mocks.NewMockSelector(ctr)
	selector.EXPECT().GetRandomN(gomock.Any()).AnyTimes().Return([]string{"http://127.0.0.1:9600"})

	worker := NewMockWorkerCli(ctr)
	worker.EXPECT().RepairShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	sender := NewMockProducer(ctr)
	sender.EXPECT().SendMessage(gomock.Any()).AnyTimes().Return(nil)

	orphanedShardTable := NewMockOrphanedShardTbl(ctr)
	orphanedShardTable.EXPECT().SaveOrphanedShard(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	cmClient := NewMockClusterMgrAPI(ctr)
	cmClient.EXPECT().GetConfig(gomock.Any(), gomock.Any()).AnyTimes().Return("", nil)
	switchMgr := taskswitch.NewSwitchMgr(cmClient)
	taskSwitch, _ := switchMgr.AddSwitch(taskswitch.BlobDeleteSwitchName)

	consumer := NewMockConsumer(ctr)

	return &ShardRepairMgr{
		volCache:             volCache,
		workerSelector:       selector,
		workerCli:            worker,
		failMsgSender:        sender,
		orphanedShardTable:   orphanedShardTable,
		taskSwitch:           taskSwitch,
		failTopicConsumers:   []base.IConsumer{consumer},
		taskPool:             taskpool.New(1, 1),
		repairSuccessCounter: base.NewCounter(1, ShardRepair, base.KindSuccess),
		repairFailedCounter:  base.NewCounter(1, ShardRepair, base.KindFailed),
		errStatsDistribution: base.NewErrorStats(),
	}
}

func TestConsumerShardRepairMsg(t *testing.T) {
	ctr := gomock.NewController(t)
	service := newShardRepairMgr(t)
	consumer := NewMockConsumer(ctr)
	consumer.EXPECT().CommitOffset(gomock.Any()).AnyTimes().Return(nil)
	{
		// no messages
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				return []*sarama.ConsumerMessage{}
			},
		)
		service.consumerAndRepair(consumer, 0)
	}
	{
		// one message: message is invalid
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := struct{}{}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		service.consumerAndRepair(consumer, 1)
	}
	{
		// return one message and repair success
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		service.consumerAndRepair(consumer, 2)
	}
	{
		// return one message and repair failed because worker err
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		oldWorker := service.workerCli
		worker := NewMockWorkerCli(ctr)
		worker.EXPECT().RepairShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(errMock)
		service.workerCli = worker
		service.consumerAndRepair(consumer, 2)
		service.workerCli = oldWorker
	}
	{
		// return one message and repair failed because worker err(should update volume map)
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		oldWorker := service.workerCli
		worker := NewMockWorkerCli(ctr)
		worker.EXPECT().RepairShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(errcode.ErrDestReplicaBad)
		service.workerCli = worker
		service.consumerAndRepair(consumer, 2)
		service.workerCli = oldWorker
	}
	{

		// return one message and repair failed because worker return ErrOrphanShard err
		consumer.EXPECT().ConsumeMessages(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, msgCnt int) (msgs []*sarama.ConsumerMessage) {
				msg := proto.ShardRepairMsg{Bid: 1, Vid: 1, ReqId: "123456", BadIdx: []uint8{0, 1}}
				msgByte, _ := json.Marshal(msg)
				kafkaMgs := &sarama.ConsumerMessage{
					Value: msgByte,
				}
				return []*sarama.ConsumerMessage{kafkaMgs}
			},
		)
		oldWorker := service.workerCli
		worker := NewMockWorkerCli(ctr)
		worker.EXPECT().RepairShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(errcode.ErrOrphanShard)
		service.workerCli = worker
		service.consumerAndRepair(consumer, 2)
		service.workerCli = oldWorker
	}
	{
		// get stats
		service.GetErrorStats()
		service.GetTaskStats()
	}
	{
		// run task
		service.RunTask()
	}
}

func TestNewShardRepairMgr(t *testing.T) {
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
	cfg := &ShardRepairConfig{
		FailMsgSender: producerCfg,
		FailTopic:     consumerCfg,
		PriorityTopics: []base.PriorityConsumerConfig{
			{KafkaConfig: consumerCfg},
		},
	}

	volCache := NewMockVolumeCache(ctr)
	volCache.EXPECT().Get(gomock.Any()).AnyTimes().Return(&client.VolInfo{}, nil)
	volCache.EXPECT().Update(gomock.Any()).AnyTimes().Return(&client.VolInfo{}, nil)

	cmClient := NewMockClusterMgrAPI(ctr)
	switchMgr := taskswitch.NewSwitchMgr(cmClient)

	accessor := NewMockOffsetAccessor(ctr)
	accessor.EXPECT().Get(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	accessor.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	scheduler := NewMockScheduler(ctr)
	scheduler.EXPECT().ListService(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, clusterID proto.ClusterID, idc string) ([]string, error) {
			return []string{"http://127.0.0.1:9600"}, nil
		},
	)

	orphanedShardTable := NewMockOrphanedShardTbl(ctr)
	orphanedShardTable.EXPECT().SaveOrphanedShard(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	worker := NewMockWorkerCli(ctr)
	worker.EXPECT().RepairShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	_, err := NewShardRepairMgr(cfg, volCache, switchMgr, accessor, scheduler, orphanedShardTable, worker)
	require.NoError(t, err)

	_, err = NewShardRepairMgr(cfg, volCache, switchMgr, accessor, scheduler, orphanedShardTable, worker)
	require.Error(t, err)
}
