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

package mqproxy

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/api/mqproxy"
	"github.com/cubefs/blobstore/common/kafka"
	"github.com/cubefs/blobstore/common/rpc"
	c "github.com/cubefs/blobstore/mqproxy/client"
	"github.com/cubefs/blobstore/util/errors"
)

var (
	ctx = context.Background()

	mqproxyServer *httptest.Server
	once          sync.Once
)

func runMockService(s *Service) string {
	once.Do(func() {
		mqproxyServer = httptest.NewServer(NewHandler(s))
	})
	return mqproxyServer.URL
}

func newMockService(t *testing.T) *Service {
	ctr := gomock.NewController(t)

	register := NewMockRegister(ctr)
	register.EXPECT().Register(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, info c.RegisterInfo) error {
			return nil
		})

	blobDeleteMgr := NewMockBlobDeleteHandler(ctr)
	blobDeleteMgr.EXPECT().SendDeleteMsg(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, info *mqproxy.DeleteArgs) error {
			if len(info.Blobs) > 1 {
				return errors.New("fake send delete message failed")
			}
			return nil
		},
	)

	shardRepairMgr := NewMockShardRepairHandler(ctr)
	shardRepairMgr.EXPECT().SendShardRepairMsg(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, info *mqproxy.ShardRepairArgs) error {
			if info.Vid == 100 {
				return errors.New("fake send shard repair message failed")
			}
			return nil
		})

	return &Service{
		clusterMgrClient: register,
		blobDeleteMgr:    blobDeleteMgr,
		shardRepairMgr:   shardRepairMgr,
		Config: Config{
			ClusterID: 1,
		},
	}
}

func newClient() rpc.Client {
	return rpc.NewClient(&rpc.Config{})
}

func TestService(t *testing.T) {
	runMockService(newMockService(t))
	cli := newClient()

	deleteCases := []struct {
		args mqproxy.DeleteArgs
		code int
	}{
		{
			args: mqproxy.DeleteArgs{
				ClusterID: 1,
				Blobs:     []mqproxy.BlobDelete{{Bid: 0, Vid: 0}},
			},
			code: 200,
		},
		{
			args: mqproxy.DeleteArgs{
				ClusterID: 2,
				Blobs:     []mqproxy.BlobDelete{{Bid: 0, Vid: 0}},
			},
			code: 706,
		},
		{
			args: mqproxy.DeleteArgs{
				ClusterID: 1,
				Blobs:     []mqproxy.BlobDelete{{Bid: 0, Vid: 0}, {Bid: 1, Vid: 1}},
			},
			code: 500,
		},
	}
	for _, tc := range deleteCases {
		err := cli.PostWith(ctx, mqproxyServer.URL+"/deletemsg", nil, tc.args)
		require.Equal(t, tc.code, rpc.DetectStatusCode(err))
	}

	shardRepairCases := []struct {
		args mqproxy.ShardRepairArgs
		code int
	}{
		{
			args: mqproxy.ShardRepairArgs{
				ClusterID: 1,
				Bid:       1,
				Vid:       1,
				BadIdxes:  nil,
				Reason:    "",
			},
			code: 200,
		},
		{
			args: mqproxy.ShardRepairArgs{
				ClusterID: 2,
				Bid:       1,
				Vid:       1,
				BadIdxes:  nil,
				Reason:    "",
			},
			code: 706,
		},
		{
			args: mqproxy.ShardRepairArgs{
				ClusterID: 1,
				Bid:       1,
				Vid:       100,
				BadIdxes:  nil,
				Reason:    "",
			},
			code: 500,
		},
	}
	for _, tc := range shardRepairCases {
		err := cli.PostWith(ctx, mqproxyServer.URL+"/repairmsg", nil, tc.args)
		require.Equal(t, tc.code, rpc.DetectStatusCode(err))
	}
}

func TestConfigFix(t *testing.T) {
	testCases := []struct {
		cfg *Config
		err error
	}{
		{cfg: &Config{}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test"}}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test", ShardRepairTopic: "test1"}}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test", ShardRepairTopic: "test", ShardRepairPriorityTopic: "test3"}}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test", ShardRepairTopic: "test1", ShardRepairPriorityTopic: "test"}}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test", ShardRepairTopic: "test1", ShardRepairPriorityTopic: "test3"}}, err: nil},
	}

	for _, tc := range testCases {
		err := tc.cfg.checkAndFix()
		require.Equal(t, true, errors.Is(err, tc.err))
		tc.cfg.shardRepairCfg()
		tc.cfg.blobDeleteCfg()
	}
}

func TestRegister(t *testing.T) {
	service := newMockService(t)
	err := service.register()
	require.NoError(t, err)
}

func TestNewMqService(t *testing.T) {
	seedBroker, leader := NewBrokersWith2Responses(t)
	defer seedBroker.Close()
	defer leader.Close()

	testCases := []struct {
		cfg Config
	}{
		{
			cfg: Config{},
		},
		// todo wait cm chang rpc
		{
			cfg: Config{
				MQ: MQConfig{
					BlobDeleteTopic:          "test1",
					ShardRepairTopic:         "test2",
					ShardRepairPriorityTopic: "test3",
					MsgSender: kafka.ProducerCfg{
						BrokerList: []string{seedBroker.Addr()},
						TimeoutMs:  1,
					},
				},
				Clustermgr: clustermgr.Config{
					LbConfig: rpc.LbConfig{Hosts: []string{"http://127.0.0.1:9321"}},
				},
			},
		},
		{
			cfg: Config{
				MQ: MQConfig{
					BlobDeleteTopic:          "test1",
					ShardRepairTopic:         "test2",
					ShardRepairPriorityTopic: "test3",
				},
				Clustermgr: clustermgr.Config{
					LbConfig: rpc.LbConfig{Hosts: []string{"http://127.0.0.1:9321"}},
				},
			},
		},
	}
	for _, tc := range testCases {
		_, err := NewService(tc.cfg)
		require.Error(t, err)
	}
}
