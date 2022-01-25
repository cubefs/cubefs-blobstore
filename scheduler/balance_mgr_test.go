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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
)

var (
	MockBalanceMigrateInfoMap = map[proto.Vid]*client.VolumeInfoSimple{
		200: MockMigrateGenVolInfo(200, codemode.EC6P6, proto.VolumeStatusIdle),
		201: MockMigrateGenVolInfo(201, codemode.EC6P10L2, proto.VolumeStatusIdle),
		202: MockMigrateGenVolInfo(202, codemode.EC6P10L2, proto.VolumeStatusActive),
		203: MockMigrateGenVolInfo(203, codemode.EC6P6, proto.VolumeStatusLock),
		204: MockMigrateGenVolInfo(204, codemode.EC6P6, proto.VolumeStatusLock),
		205: MockMigrateGenVolInfo(205, codemode.EC6P6, proto.VolumeStatusIdle),
		206: MockMigrateGenVolInfo(206, codemode.EC6P10L2, proto.VolumeStatusIdle),
		207: MockMigrateGenVolInfo(207, codemode.EC6P10L2, proto.VolumeStatusActive),
		208: MockMigrateGenVolInfo(208, codemode.EC6P6, proto.VolumeStatusLock),
		209: MockMigrateGenVolInfo(209, codemode.EC6P6, proto.VolumeStatusLock),
	}
	migrateDisk4 = client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "r0",
		Host:         "127.0.0.1:8000",
		DiskID:       4,
		FreeChunkCnt: 10,
		MaxChunkCnt:  700,
	}
	migrateDisk5 = client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z0",
		Rack:         "r0",
		Host:         "127.0.0.2:8000",
		DiskID:       5,
		FreeChunkCnt: 100,
		MaxChunkCnt:  700,
		Status:       proto.DiskStatusNormal,
	}
	migrateDisk6 = client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z1",
		Rack:         "r1",
		Host:         "127.0.0.3:8000",
		DiskID:       6,
		FreeChunkCnt: 20,
		MaxChunkCnt:  700,
		Status:       proto.DiskStatusNormal,
	}
	migrateDisk7 = client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z2",
		Rack:         "r2",
		Host:         "127.0.0.4:8000",
		DiskID:       7,
		FreeChunkCnt: 5,
		MaxChunkCnt:  700,
		Status:       proto.DiskStatusNormal,
	}
	migrateDisk8 = client.DiskInfoSimple{
		ClusterID:    1,
		Idc:          "z2",
		Rack:         "r2",
		Host:         "127.0.0.4:8000",
		DiskID:       8,
		FreeChunkCnt: 200,
		MaxChunkCnt:  700,
		Status:       proto.DiskStatusNormal,
	}
	MockDisksMap = map[proto.DiskID]*client.DiskInfoSimple{
		4: &migrateDisk4,
		5: &migrateDisk5,
		6: &migrateDisk6,
		7: &migrateDisk8,
	}

	MockClusterDisks = []*client.DiskInfoSimple{&migrateDisk4, &migrateDisk5, &migrateDisk6, &migrateDisk7, &migrateDisk8}
)

func initBalanceMgr(respErr error, baseVid proto.Vid) (mgr *BalanceMgr, err error) {
	mockCmCli := NewMigrateMockCmClient(respErr, nil, MockBalanceMigrateInfoMap, MockDisksMap)
	switchMgr := taskswitch.NewSwitchMgr(mockCmCli)
	mockTinkerCli := NewTinkerMockClient(respErr)
	topoMgr := &ClusterTopologyMgr{
		done:         make(chan struct{}, 1),
		closeOnce:    &sync.Once{},
		taskStatsMgr: base.NewClusterTopologyStatisticsMgr(1, []float64{}),
	}

	topoMgr.buildClusterTopo(MockClusterDisks, 1)

	mockRegisterTbl := NewMockRegisterTbl(respErr)
	mockMigrateTbl := NewMockMigrateTbl(nil, MockBalanceMigrateTasks(MockBalanceMigrateInfoMap, baseVid))
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
	conf := &BalanceMgrConfig{
		MinDiskFreeChunkCnt: 20,
		MigrateConfig:       migrateConf,
	}

	return NewBalanceMgr(mockCmCli, mockTinkerCli, switchMgr, topoMgr, mockRegisterTbl, mockMigrateTbl, conf)
}

func TestBalanceMgr(t *testing.T) {
	mgr, err := initBalanceMgr(nil, 200)
	require.NoError(t, err)
	err = mgr.Load()
	require.NoError(t, err)

	// test stats
	tasks, _ := mgr.migrateMgr.taskTbl.FindAll(context.Background())
	require.NotEqual(t, 0, len(tasks))
	_, _, err = mgr.QueryTask(context.Background(), tasks[0].TaskID)
	require.NoError(t, err)
	mgr.GetTaskStats()
	mgr.StatQueueTaskCnt()
	mgr.Close()

	mgrErr, err := initBalanceMgr(ErrMockResponse, 205)
	require.NoError(t, err)
	err = mgrErr.Load()
	require.NoError(t, err)

	testBalanceWithErr(t, mgr, nil)
	testBalanceWithErr(t, mgrErr, ErrMockResponse)
	mgrErr.Close()
}

func testBalanceWithErr(t *testing.T, mgr *BalanceMgr, respErr error) {
	mgr.taskSwitch.Enable()

	testBalanceCollectionTask(t, mgr, respErr)
	testCheckAndClear(t, mgr, respErr)
	testAcquireAndCancel(t, mgr)
	testAcquireAndReclaim(t, mgr, respErr)
	testAcquireAndRenewal(t, mgr)
	testAcquireAndComplete(t, mgr)
	testTaskLoop(t, mgr)
}

func testBalanceCollectionTask(t *testing.T, mgr *BalanceMgr, respErr error) {
	mgr.cfg.BalanceDiskCntLimit = 3
	err := mgr.collectionTask()
	require.Error(t, err)
	require.EqualError(t, ErrTooManyBalancingTasks, err.Error())
	require.Equal(t, 3, mgr.migrateMgr.diskMigratingVuids.getCurrMigratingDisksCnt())
	todo, doing := mgr.migrateMgr.prepareQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)

	mgr.cfg.BalanceDiskCntLimit = 4
	err = mgr.collectionTask()
	if respErr != nil {
		require.Error(t, err)
		require.Equal(t, 3, mgr.migrateMgr.diskMigratingVuids.getCurrMigratingDisksCnt())
		todo, doing := mgr.migrateMgr.prepareQueue.StatsTasks()
		require.Equal(t, 1, todo+doing)
	} else {
		require.Equal(t, 4, mgr.migrateMgr.diskMigratingVuids.getCurrMigratingDisksCnt())
		todo, doing := mgr.migrateMgr.prepareQueue.StatsTasks()
		require.Equal(t, 2, todo+doing)
	}

	mgr.cfg.BalanceDiskCntLimit = 5
	mgr.collectionTask()
}

func testCheckAndClear(t *testing.T, mgr *BalanceMgr, respErr error) {
	tasks, err := mgr.migrateMgr.taskTbl.FindAll(context.TODO())
	if respErr != nil {
		require.NoError(t, err)
		require.Equal(t, 5, len(tasks))
	} else {
		require.NoError(t, err)
		require.Equal(t, 6, len(tasks))
	}

	mgr.ClearFinishedTask()

	tasks, err = mgr.migrateMgr.taskTbl.FindAll(context.TODO())
	require.NoError(t, err)
	if respErr != nil {
		require.Equal(t, 3, len(tasks))
	} else {
		require.Equal(t, 4, len(tasks))
	}
}

func testAcquireAndCancel(t *testing.T, mgr *BalanceMgr) {
	// acquire task
	task, err := mgr.AcquireTask(context.TODO(), "z0")
	require.NoError(t, err)
	_, err = mgr.AcquireTask(context.TODO(), "z1")
	require.EqualError(t, proto.ErrTaskEmpty, err.Error())

	_, err = mgr.AcquireTask(context.TODO(), "z2")
	require.EqualError(t, proto.ErrTaskEmpty, err.Error())
	// cancel task
	args := api.CancelTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.SourceIdc,
		TaskType: proto.BalanceTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CancelTask(context.TODO(), &args)
	require.NoError(t, err)
}

func testAcquireAndReclaim(t *testing.T, mgr *BalanceMgr, respErr error) {
	// acquire task
	_, err := mgr.AcquireTask(context.TODO(), "z0")
	require.EqualError(t, proto.ErrTaskEmpty, err.Error())

	// sleep punish time and get task
	time.Sleep(time.Duration(mgr.migrateMgr.CancelPunishDurationS) * time.Second)
	task, err := mgr.AcquireTask(context.TODO(), "z0")
	require.NoError(t, err)

	newDst, err := mgr.migrateMgr.clusterMgrClient.AllocVolumeUnit(context.TODO(), task.Destination.Vuid)
	testRespWithErr(t, respErr, err)
	err = mgr.ReclaimTask(context.TODO(), task.SourceIdc, task.TaskID, task.Sources, task.Destination, newDst)
	require.NoError(t, err)
}

func testAcquireAndRenewal(t *testing.T, mgr *BalanceMgr) {
	// acquire task
	task, err := mgr.AcquireTask(context.TODO(), "z0")
	require.NoError(t, err)

	// renewal task
	err = mgr.RenewalTask(context.TODO(), "z0", task.TaskID)
	require.NoError(t, err)

	// reclaim task
	newDst, _ := mgr.migrateMgr.clusterMgrClient.AllocVolumeUnit(context.TODO(), task.Destination.Vuid)
	err = mgr.ReclaimTask(context.TODO(), task.SourceIdc, task.TaskID, task.Sources, task.Destination, newDst)
	require.NoError(t, err)
}

func testAcquireAndComplete(t *testing.T, mgr *BalanceMgr) {
	// acquire task
	task, err := mgr.AcquireTask(context.TODO(), "z0")
	require.NoError(t, err)
	// complete task
	completeArgs := api.CompleteTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.SourceIdc,
		TaskType: proto.BalanceTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CompleteTask(context.TODO(), &completeArgs)
	require.NoError(t, err)
}

func testTaskLoop(t *testing.T, mgr *BalanceMgr) {
	mgr.Run()
	time.Sleep(time.Duration(mgr.cfg.CollectTaskIntervalS) * time.Second)
	time.Sleep(time.Duration(mgr.cfg.CheckTaskIntervalS) * time.Second)
}

func testRespWithErr(t *testing.T, expected, err error) {
	if expected != nil {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
}

func MockBalanceMigrateTasks(volInfos map[proto.Vid]*client.VolumeInfoSimple, baseVid proto.Vid) map[string]*proto.MigrateTask {
	m := make(map[string]*proto.MigrateTask)
	t1 := mockGenMigrateTask("z0", 4, baseVid, proto.MigrateStateInited, volInfos)

	baseVid++
	t2 := mockGenMigrateTask("z0", 5, baseVid, proto.MigrateStatePrepared, volInfos)

	baseVid++
	t3 := mockGenMigrateTask("z1", 6, baseVid, proto.MigrateStateWorkCompleted, volInfos)

	baseVid++
	t4 := mockGenMigrateTask("z2", 7, baseVid, proto.MigrateStateFinishedInAdvance, volInfos)

	baseVid++
	t5 := mockGenMigrateTask("z2", 8, baseVid, proto.MigrateStateFinished, volInfos)

	m[t1.TaskID] = t1
	m[t2.TaskID] = t2
	m[t3.TaskID] = t3
	m[t4.TaskID] = t4
	m[t5.TaskID] = t5
	return m
}
