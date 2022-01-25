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

package scheduler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/scheduler/client"
)

var (
	MocManualMigrateInfoMap = map[proto.Vid]*client.VolumeInfoSimple{
		20001: MockMigrateGenVolInfo(20001, codemode.EC6P3L3, proto.VolumeStatusIdle),
		20002: MockMigrateGenVolInfo(20002, codemode.EC6P3L3, proto.VolumeStatusActive),
	}
	mdisk1 = &client.DiskInfoSimple{
		ClusterID: 1,
		DiskID:    1,
		Idc:       "z0",
	}
	mockManualDisksmap = map[proto.DiskID]*client.DiskInfoSimple{
		1: mdisk1,
	}
)

func TestManualMigrateMgr(t *testing.T) {
	ctx := context.Background()
	cmCli := NewMigrateMockCmClient(nil, nil, MocManualMigrateInfoMap, mockManualDisksmap)
	cmCli.(*mockMigrateCmClient).disableCommRespErr()

	mockTinkerCli := NewTinkerMockClient(nil)

	mockRegisterTbl := NewMockRegisterTbl(nil)
	mockMigrateTbl := NewMockMigrateTbl(nil, nil)
	mgr := NewManualMigrateMgr(
		cmCli,
		mockTinkerCli,
		mockRegisterTbl,
		mockMigrateTbl,
		1,
	)

	inited, prepared, completed := mgr.migrate.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 0, completed)

	vid1 := MocManualMigrateInfoMap[20001]
	err := mgr.AddTask(ctx, vid1.VunitLocations[0].Vuid, false)
	require.NoError(t, err)
	inited, prepared, completed = mgr.migrate.StatQueueTaskCnt()
	require.Equal(t, 1, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 0, completed)
	err = mgr.migrate.prepareTask()
	require.NoError(t, err)

	inited, prepared, completed = mgr.migrate.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 1, prepared)
	require.Equal(t, 0, completed)

	tasks, err := mgr.migrate.GetAllTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	for _, task := range tasks {
		if task.State == proto.MigrateStatePrepared {
			args := genCompleteArgs(task)
			err = mgr.CompleteTask(context.Background(), args)
			require.NoError(t, err)
		}
	}

	inited, prepared, completed = mgr.migrate.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 1, completed)

	err = mgr.migrate.finishTask()
	require.NoError(t, err)

	tasks, _ = mgr.migrate.GetAllTasks(context.Background())
	require.Equal(t, 1, len(tasks))
	fmt.Printf("tasks %+v\n", tasks[0])

	vid2 := MocManualMigrateInfoMap[20002]
	err = mgr.AddTask(ctx, vid2.VunitLocations[0].Vuid, false)
	require.NoError(t, err)
	inited, prepared, completed = mgr.migrate.StatQueueTaskCnt()
	require.Equal(t, 1, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 0, completed)

	err = mgr.migrate.prepareTask()
	require.NoError(t, err)

	inited, prepared, completed = mgr.migrate.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 0, completed)

	tasks, err = mgr.migrate.GetAllTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, len(tasks))
	for _, task := range tasks {
		mgr.QueryTask(context.Background(), task.TaskID)
		fmt.Printf("task %+v\n", task)
	}
}

func TestManualMigrateMgrSvr(t *testing.T) {
	cmCli := NewMigrateMockCmClient(nil, nil, MocManualMigrateInfoMap, mockManualDisksmap)
	cmCli.(*mockMigrateCmClient).disableCommRespErr()

	mockTinkerCli := NewTinkerMockClient(nil)

	mockRegisterTbl := NewMockRegisterTbl(nil)
	mockMigrateTbl := NewMockMigrateTbl(nil, nil)
	mgr := NewManualMigrateMgr(
		cmCli,
		mockTinkerCli,
		mockRegisterTbl,
		mockMigrateTbl,
		1,
	)
	mgr.Run()
	mgr.RenewalTask(context.Background(), "", "")
	mgr.CompleteTask(context.Background(), &scheduler.CompleteTaskArgs{})
	mgr.CancelTask(context.Background(), &scheduler.CancelTaskArgs{})
	mgr.AcquireTask(context.Background(), "")
	mgr.ReclaimTask(context.Background(), "", "", nil, proto.VunitLocation{}, &client.AllocVunitInfo{})
	mgr.QueryTask(context.Background(), "")
	mgr.ReportWorkerTaskStats("", proto.TaskStatistics{}, 0, 0)
}
