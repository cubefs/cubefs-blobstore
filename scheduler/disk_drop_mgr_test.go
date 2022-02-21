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
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
)

var MockDropMigrateInfoMap = map[proto.Vid]*client.VolumeInfoSimple{
	10001: MockMigrateGenVolInfo(10001, codemode.EC6P3L3, proto.VolumeStatusIdle),
	10002: MockMigrateGenVolInfo(10002, codemode.EC6P3L3, proto.VolumeStatusIdle),
	10003: MockMigrateGenVolInfo(10003, codemode.EC6P3L3, proto.VolumeStatusIdle),
	10004: MockMigrateGenVolInfo(10004, codemode.EC6P3L3, proto.VolumeStatusIdle),
	10005: MockMigrateGenVolInfo(10005, codemode.EC6P3L3, proto.VolumeStatusIdle),
}

func initDiskDropMgr(
	respErr error,
	taskInDB map[string]*proto.MigrateTask,
	disksMap map[proto.DiskID]*client.DiskInfoSimple) (mgr *DiskDropMgr, err error) {
	mockCmCli := NewMigrateMockCmClient(respErr, nil, MockDropMigrateInfoMap, disksMap)
	switchMgr := taskswitch.NewSwitchMgr(mockCmCli)
	mockTinkerCli := NewTinkerMockClient(respErr)

	mockRegisterTbl := NewMockRegisterTbl(respErr)
	mockMigrateTbl := NewMockMigrateTbl(nil, taskInDB)
	migrateConf := MigrateConfig{
		TaskCommonConfig: base.TaskCommonConfig{
			PrepareQueueRetryDelayS: 1,
			FinishQueueRetryDelayS:  1,
			CancelPunishDurationS:   1,
			WorkQueueSize:           3,
			CheckTaskIntervalS:      1,
			CollectTaskIntervalS:    1,
		},
	}
	conf := &DiskDropMgrConfig{
		MigrateConfig: migrateConf,
	}

	return NewDiskDropMgr(mockCmCli, mockTinkerCli, switchMgr, mockRegisterTbl, mockMigrateTbl, conf)
}

func TestDiskDropMgrAll(t *testing.T) {
	testDropTaskLoad(t)
	testCollectDropTask(t)
	testCheckDropped(t)

	testDiskDropMgr(t)
}

func testDiskDropMgr(t *testing.T) {
	diskDropMgr, err := initDiskDropMgr(nil, nil, MockDisksMap)
	require.NoError(t, err)
	MockEmptyVolTaskLocker()
	diskDropMgr.Load()
	diskDropMgr.Run()
	diskDropMgr.RenewalTask(context.Background(), "", "")
	diskDropMgr.CompleteTask(context.Background(), &scheduler.CompleteTaskArgs{})
	diskDropMgr.CancelTask(context.Background(), &scheduler.CancelTaskArgs{})
	diskDropMgr.AcquireTask(context.Background(), "")
	diskDropMgr.ReclaimTask(context.Background(), "", "", nil, proto.VunitLocation{}, &client.AllocVunitInfo{})
	diskDropMgr.ReportWorkerTaskStats("", proto.TaskStatistics{}, 0, 0)
	diskDropMgr.Close()
}

func MockDiskDropTasks() map[string]*proto.MigrateTask {
	m := make(map[string]*proto.MigrateTask)
	taskIDs := []string{"task1", "task2", "task3", "task4", "task5"}
	states := []proto.MigrateSate{
		proto.MigrateStateInited,
		proto.MigrateStatePrepared,
		proto.MigrateStateWorkCompleted,
		proto.MigrateStateFinished,
		proto.MigrateStateFinishedInAdvance,
	}
	vids := []proto.Vid{10001, 10002, 10003, 10006, 10007}

	for i := 0; i < len(taskIDs); i++ {
		m[taskIDs[i]] = newMockDropTask(taskIDs[i], states[i], vids[i])
	}
	return m
}

func newMockDropTask(taskID string, state proto.MigrateSate, vid proto.Vid) *proto.MigrateTask {
	vol := MockMigrateGenVolInfo(vid, codemode.EC6P3L3, 1)
	return &proto.MigrateTask{
		TaskID:       taskID,
		State:        state,
		SourceIdc:    "z0",
		SourceDiskID: 4,
		SourceVuid:   vol.VunitLocations[0].Vuid,

		Sources:  vol.VunitLocations,
		CodeMode: vol.CodeMode,
	}
}

func testDropTaskLoad(t *testing.T) {
	mgr, err := initDiskDropMgr(nil, MockDiskDropTasks(), MockDisksMap)
	require.NoError(t, err)
	MockEmptyVolTaskLocker()
	err = mgr.Load()
	require.NoError(t, err)

	inited, prepared, completed := mgr.StatQueueTaskCnt()
	require.Equal(t, 1, inited)
	require.Equal(t, 1, prepared)
	require.Equal(t, 1, completed)
}

func testCollectDropTask(t *testing.T) {
	fmt.Printf("-----------------TestCollectDropTask---------------\n")
	mgr, err := initDiskDropMgr(nil, MockDiskDropTasks(), MockDisksMap)
	require.NoError(t, err)
	MockEmptyVolTaskLocker()
	err = mgr.Load()
	require.NoError(t, err)
	mgr.collectTask()

	// collect task again
	mgr.collectTask()

	tasks, err := mgr.migrateMgr.GetAllTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, 7, len(tasks))

	mgr2, err := initDiskDropMgr(nil, nil, MockDisksMap)
	require.NoError(t, err)
	mgr2.collectTask()
	tasks, err = mgr2.migrateMgr.GetAllTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, 5, len(tasks))

	for _, task := range tasks {
		mgr2.QueryTask(context.Background(), task.TaskID)
	}
	mgr2.Progress(context.Background())

	fmt.Printf("-----------------end TestCollectDropTask---------------\n")

	MockEmptyVolTaskLocker()
	mgr1, err := initDiskDropMgr(nil, MockDiskDropTasks(), nil)
	require.NoError(t, err)
	mgr1.hasRevised = true
	mgr1.collectTask()
}

func testCheckDropped(t *testing.T) {
	mgr, err := initDiskDropMgr(nil, MockDiskDropTasks(), MockDisksMap)
	require.NoError(t, err)
	MockEmptyVolTaskLocker()
	err = mgr.Load()
	require.NoError(t, err)
	mgr.collectTask()
	tasks, err := mgr.migrateMgr.GetAllTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, 7, len(tasks))

	inited, prepared, completed := mgr.StatQueueTaskCnt()
	fmt.Printf("inited %d, prepared %d, completed %d\n", inited, prepared, completed)
	mgr.migrateMgr.prepareTask()
	mgr.migrateMgr.prepareTask()
	mgr.migrateMgr.prepareTask()
	mgr.migrateMgr.prepareTask()
	inited, prepared, completed = mgr.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 4, prepared)
	require.Equal(t, 1, completed)

	tasks, err = mgr.migrateMgr.GetAllTasks(context.Background())
	require.NoError(t, err)
	for _, task := range tasks {
		if task.State == proto.MigrateStatePrepared {
			args := genCompleteArgs(task)
			err = mgr.CompleteTask(context.Background(), args)
			require.NoError(t, err)
		}
	}
	inited, prepared, completed = mgr.StatQueueTaskCnt()
	require.Equal(t, 0, inited)
	require.Equal(t, 0, prepared)
	require.Equal(t, 5, completed)

	err = mgr.migrateMgr.finishTask()
	require.NoError(t, err)
	err = mgr.migrateMgr.finishTask()
	require.NoError(t, err)
	err = mgr.migrateMgr.finishTask()
	require.NoError(t, err)
	err = mgr.migrateMgr.finishTask()
	require.NoError(t, err)
	err = mgr.migrateMgr.finishTask()
	require.NoError(t, err)
	inited, prepared, completed = mgr.StatQueueTaskCnt()
	fmt.Printf("inited %d, prepared %d, completed %d\n", inited, prepared, completed)
	mgr.cmCli.(*mockMigrateCmClient).volInfoMap = nil
	dropped := mgr.checkDropped(context.Background(), 4)
	require.Equal(t, true, dropped)

	mgr.checkDroppedAndClear()

	_, ok := mgr.cmCli.(*mockMigrateCmClient).droppedDisks[4]
	require.Equal(t, true, ok)

	tasks, err = mgr.migrateMgr.GetAllTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, len(tasks))
}
