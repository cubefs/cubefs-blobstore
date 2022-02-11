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
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/api/tinker"
	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	cli "github.com/cubefs/blobstore/tinker/client"
)

var (
	ctx = context.Background()

	tinkerServer *httptest.Server
	once         sync.Once
)

func runMockService(s *Service) string {
	once.Do(func() {
		tinkerServer = httptest.NewServer(NewHandler(s))
	})
	return tinkerServer.URL
}

func newMockService(t *testing.T) *Service {
	ctr := gomock.NewController(t)

	cmClient := NewMockClusterMgrAPI(ctr)
	volCache := NewMockVolumeCache(ctr)
	volCache.EXPECT().Update(gomock.Any()).AnyTimes().DoAndReturn(
		func(vid proto.Vid) (*cli.VolInfo, error) {
			if vid == proto.Vid(100) {
				return nil, errMock
			}
			return nil, nil
		},
	)
	volCache.EXPECT().Load().AnyTimes().Return(nil)

	deleteMgr := NewMockBaseMgr(ctr)
	deleteMgr.EXPECT().GetTaskStats().AnyTimes().DoAndReturn(
		func() (success [counter.SLOT]int, failed [counter.SLOT]int) {
			return [counter.SLOT]int{1, 1, 0, 0}, [counter.SLOT]int{0, 0, 1, 1}
		},
	)
	deleteMgr.EXPECT().GetErrorStats().AnyTimes().DoAndReturn(
		func() (errStats []string, totalErrCnt uint64) {
			return []string{"time out"}, 64
		},
	)
	deleteMgr.EXPECT().RunTask().AnyTimes().Return()
	deleteMgr.EXPECT().Enabled().AnyTimes().Return(true)

	shardRepairMgr := NewMockBaseMgr(ctr)
	shardRepairMgr.EXPECT().GetTaskStats().AnyTimes().DoAndReturn(
		func() (success [counter.SLOT]int, failed [counter.SLOT]int) {
			return [counter.SLOT]int{1, 1, 0, 0}, [counter.SLOT]int{0, 0, 1, 1}
		},
	)
	shardRepairMgr.EXPECT().GetErrorStats().AnyTimes().DoAndReturn(
		func() (errStats []string, totalErrCnt uint64) {
			return []string{"time out"}, 64
		},
	)
	shardRepairMgr.EXPECT().RunTask().AnyTimes().Return()
	shardRepairMgr.EXPECT().Enabled().AnyTimes().Return(true)

	return &Service{
		clusterMgrClient: cmClient,
		volCache:         volCache,
		deleteMgr:        deleteMgr,
		shardRepairMgr:   shardRepairMgr,
	}
}

func TestService(t *testing.T) {
	runMockService(newMockService(t))
	tinkerCli := tinker.New(&tinker.Config{})

	updateVolCases := []struct {
		vid  proto.Vid
		code int
	}{
		{
			vid:  proto.Vid(1),
			code: 200,
		},
		{
			vid:  proto.Vid(100),
			code: 500,
		},
	}
	for _, tc := range updateVolCases {
		err := tinkerCli.UpdateVolume(ctx, tinkerServer.URL, tc.vid)
		require.Equal(t, tc.code, rpc.DetectStatusCode(err))
	}

	for i := 0; i < 2; i++ {
		_, err := tinkerCli.Stats(ctx, tinkerServer.URL)
		require.NoError(t, err)
	}
}

func TestRunTask(t *testing.T) {
	service := newMockService(t)
	service.RunTask()

	require.Panics(t, func() {
		volCache := NewMockVolumeCache(gomock.NewController(t))
		volCache.EXPECT().Load().AnyTimes().Return(errMock)
		service.volCache = volCache
		service.RunTask()
	})
}

func TestConfigCheckAndFix(t *testing.T) {
	cfg := &Config{}
	err := cfg.checkAndFix()
	require.NoError(t, err)
	require.Equal(t, defaultUpdateDurationS, cfg.VolCacheUpdateDurationS)
}

func TestRegister(t *testing.T) {
	ctr := gomock.NewController(t)
	service := newMockService(t)

	schedulerCli := NewMockScheduler(ctr)
	schedulerCli.EXPECT().Register(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	err := service.Register(schedulerCli)
	require.NoError(t, err)

	schedulerCli.EXPECT().Register(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(3).Return(errMock)
	err = service.Register(schedulerCli)
	require.Error(t, err)
}

func TestRunKafkaMonitor(t *testing.T) {
	ctr := gomock.NewController(t)
	service := newMockService(t)

	accessor := NewMockOffsetAccessor(ctr)
	accessor.EXPECT().Get(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(1), nil)

	err := service.runKafkaMonitor(accessor)
	require.Error(t, err)
}
