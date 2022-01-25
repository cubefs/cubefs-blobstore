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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/proto"
	api "github.com/cubefs/blobstore/scheduler/client"
)

type mockStats struct{}

func (m *mockStats) StatQueueTaskCnt() (preparing, workerDoing, finishing int) {
	return 0, 0, 0
}

func TestTaskStatisticsMgr(t *testing.T) {
	mgr := NewTaskStatsMgrAndRun(1, proto.RepairTaskType, &mockStats{})
	mgr.ReportWorkerTaskStats("repair_task_1", proto.TaskStatistics{}, 10, 10)

	task, err := mgr.QueryTaskDetail("repair_task_1")
	require.NoError(t, err)
	fmt.Println(task.Statistics)

	increaseDataSize, increaseShardCnt := mgr.Counters()
	var increaseDataSizeVec [counter.SLOT]int
	increaseDataSizeVec[counter.SLOT-1] = 10

	var increaseShardCntVec [counter.SLOT]int
	increaseShardCntVec[counter.SLOT-1] = 10
	require.Equal(t, increaseDataSizeVec, increaseDataSize)
	require.Equal(t, increaseShardCntVec, increaseShardCnt)
}

func TestNewClusterTopoStatisticsMgr(t *testing.T) {
	mgr := NewClusterTopologyStatisticsMgr(1, []float64{})
	disk := &api.DiskInfoSimple{
		Idc:          "z0",
		Rack:         "test_rack",
		FreeChunkCnt: 100,
	}
	mgr.ReportFreeChunk(disk)
}
