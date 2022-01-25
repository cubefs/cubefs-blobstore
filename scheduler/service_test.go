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
	baseErr "errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/blobstore/util/log"
)

var (
	schedulerHost = ""
	cancelS       = 1
)

var (
	schedulerServer *httptest.Server
	once            sync.Once
)

func runMockService(s *Service) string {
	once.Do(func() {
		schedulerServer = httptest.NewServer(NewHandler(s))
	})
	return schedulerServer.URL
}

func init() {
	// init client
	clusterMgrCli := NewMockClusterManagerClient()
	tinkerCli := NewMockTinkerClient()
	mqProxyCli := &mockMqProxy{}

	// init task switch
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)

	topologyMgr := &ClusterTopologyMgr{
		taskStatsMgr: base.NewClusterTopologyStatisticsMgr(1, []float64{}),
	}

	clusterID := proto.ClusterID(1)

	serviceRegisterTbl := newServiceRegisterTbl()
	balanceTbl := newBalanceTbl()
	taskCommonConfig := base.TaskCommonConfig{
		CancelPunishDurationS: cancelS,
	}
	// init balance manager
	migrateConf := MigrateConfig{
		TaskCommonConfig: taskCommonConfig,
	}
	balanceConf := &BalanceMgrConfig{
		MigrateConfig: migrateConf,
	}
	balanceMgr, err := NewBalanceMgr(clusterMgrCli, tinkerCli, switchMgr, topologyMgr, serviceRegisterTbl,
		balanceTbl, balanceConf)
	if err != nil {
		log.Errorf("new balance mgr failed, err:%v", err)
		return
	}
	balanceMgr.taskSwitch.Enable()

	// init disk drop manager
	diskDropTbl := newDiskDropTbl()
	diskDropConf := &DiskDropMgrConfig{}
	diskDropMgr, err := NewDiskDropMgr(clusterMgrCli, tinkerCli, switchMgr, serviceRegisterTbl,
		diskDropTbl, diskDropConf)
	if err != nil {
		log.Errorf("new disk drop mgr failed,err:%v", err)
		return
	}
	diskDropMgr.taskSwitch.Enable()
	manualMigTbl := newManualMigTbl()
	manualMigMgr := NewManualMigrateMgr(
		clusterMgrCli,
		tinkerCli,
		serviceRegisterTbl,
		manualMigTbl, clusterID)

	// init disk repair manager
	repairConf := &RepairMgrCfg{
		TaskCommonConfig: taskCommonConfig,
	}
	repairTbl := newVolRepairTbl()
	repairMgr, err := NewRepairMgr(repairConf, switchMgr, repairTbl, clusterMgrCli)
	if err != nil {
		log.Errorf("new RepairMgr failed, err:%+v", err)
		return
	}
	repairMgr.taskSwitch.Enable()

	inspectCfg := &InspectMgrCfg{
		InspectBatch:      3,
		ListVolIntervalMs: 100,
		ListVolStep:       1,
		TimeoutMs:         100,
	}
	ckTbl := newMockInspectCheckPointTbl()

	volsGetter := NewMockVolsList()
	initAllocMockVol(volsGetter)

	volsGetter.allocVolume(codemode.EC6P10L2, proto.VolumeStatusActive)
	inspectMgr, _ := NewInspectMgr(inspectCfg, ckTbl, volsGetter, mqProxyCli, switchMgr)
	inspectMgr.taskSwitch.Enable()
	inspectMgr.Run()

	svr := &Service{
		ClusterID:      clusterID,
		clusterTopoMgr: topologyMgr,
		balanceMgr:     balanceMgr,
		diskDropMgr:    diskDropMgr,
		manualMigMgr:   manualMigMgr,
		repairMgr:      repairMgr,
		inspectMgr:     inspectMgr,
		svrTbl:         serviceRegisterTbl,
		cmCli:          clusterMgrCli,
	}

	svr.load()

	schedulerHost = runMockService(svr)
}

func TestServiceAPI(t *testing.T) {
	schedulerConf := &scheduler.Config{
		Host: schedulerHost,
	}
	schedulerCli := scheduler.New(schedulerConf)
	svrs, err := schedulerCli.ListServices(context.Background(), &scheduler.ListServicesArgs{
		Module: proto.TinkerModule,
		IDC:    "z0",
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(svrs))

	svrHost := "127.0.0.1:9999"

	_, err = schedulerCli.GetService(context.Background(), svrHost)
	fmt.Printf("err %+v\n", err)
	require.EqualError(t, err, errors.ErrNoSuchService.Error())

	err = schedulerCli.RegisterService(context.Background(), &scheduler.RegisterServiceArgs{})
	require.EqualError(t, err, errors.ErrIllegalArguments.Error())

	svrInfo := &scheduler.RegisterServiceArgs{
		ClusterID: proto.ClusterID(1),
		Module:    proto.TinkerModule,
		Host:      svrHost,
		Idc:       "z0",
	}
	err = schedulerCli.RegisterService(context.Background(), svrInfo)
	require.NoError(t, err)

	svr, err := schedulerCli.GetService(context.Background(), "127.0.0.1:9999")
	require.NoError(t, err)
	require.Equal(t, svrInfo.Host, svr.Host)
	require.Equal(t, svrInfo.ClusterID, svr.ClusterID)
	require.Equal(t, svrInfo.Module, svr.Module)
	require.Equal(t, svrInfo.Idc, svr.IDC)

	svrs, err = schedulerCli.ListServices(context.Background(), &scheduler.ListServicesArgs{
		Module: proto.TinkerModule,
		IDC:    "z0",
	})
	require.NoError(t, err)
	require.Equal(t, 4, len(svrs))

	err = schedulerCli.DeleteService(context.Background(), &scheduler.DeleteServiceArgs{
		Host: "",
	})
	require.EqualError(t, err, errors.ErrIllegalArguments.Error())

	err = schedulerCli.DeleteService(context.Background(), &scheduler.DeleteServiceArgs{
		Host: svrHost,
	})
	require.NoError(t, err)
	svrs, err = schedulerCli.ListServices(context.Background(), &scheduler.ListServicesArgs{
		Module: proto.TinkerModule,
		IDC:    "z0",
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(svrs))

	err = schedulerCli.ReportTask(context.Background(), &scheduler.TaskReportArgs{
		TaskType: proto.RepairTaskType,
		TaskId:   "repair_task_1",
	})
	require.NoError(t, err)

	err = schedulerCli.ReportTask(context.Background(), &scheduler.TaskReportArgs{
		TaskType: proto.BalanceTaskType,
		TaskId:   "balance_task_1",
	})
	require.NoError(t, err)

	err = schedulerCli.ReportTask(context.Background(), &scheduler.TaskReportArgs{
		TaskType: proto.DiskDropTaskType,
		TaskId:   "disk_drop_task_1",
	})
	require.NoError(t, err)
}

func TestTaskAPI(t *testing.T) {
	schedulerConf := &scheduler.Config{
		Host: schedulerHost,
	}
	schedulerCli := scheduler.New(schedulerConf)

	// acquire task
	repairTask, err := schedulerCli.AcquireTask(context.Background(), &scheduler.AcquireArgs{IDC: "z0"})

	require.NoError(t, err)
	require.Equal(t, proto.RepairTaskType, repairTask.TaskType)
	require.Equal(t, "z0", repairTask.Repair.BrokenDiskIDC)

	balanceTask, err := schedulerCli.AcquireTask(context.Background(), &scheduler.AcquireArgs{IDC: "z0"})
	require.NoError(t, err)
	require.Equal(t, proto.BalanceTaskType, balanceTask.TaskType)
	require.Equal(t, "z0", balanceTask.Balance.SourceIdc)

	// reclaim task
	err = schedulerCli.ReclaimTask(context.Background(), &scheduler.ReclaimTaskArgs{
		TaskId:   repairTask.Repair.TaskID,
		TaskType: repairTask.TaskType,
		IDC:      repairTask.Repair.BrokenDiskIDC,
		Src:      repairTask.Repair.Sources,
		Dest:     repairTask.Repair.Destination,
	})
	require.NoError(t, err)

	err = schedulerCli.ReclaimTask(context.Background(), &scheduler.ReclaimTaskArgs{
		TaskId:   balanceTask.Balance.TaskID,
		TaskType: balanceTask.TaskType,
		IDC:      balanceTask.Balance.SourceIdc,
		Src:      balanceTask.Balance.Sources,
		Dest:     balanceTask.Balance.Destination,
	})
	require.NoError(t, err)

	// acquire task again
	repairTask, err = schedulerCli.AcquireTask(context.Background(), &scheduler.AcquireArgs{IDC: "z0"})
	require.NoError(t, err)
	require.Equal(t, proto.RepairTaskType, repairTask.TaskType)
	require.Equal(t, "z0", repairTask.Repair.BrokenDiskIDC)
	repairTaskID := repairTask.Repair.TaskID
	fmt.Printf("====>acquire repairTask %+v\n", *repairTask.Repair)
	balanceTask, err = schedulerCli.AcquireTask(context.Background(), &scheduler.AcquireArgs{IDC: "z0"})
	require.NoError(t, err)
	require.Equal(t, proto.BalanceTaskType, balanceTask.TaskType)
	require.Equal(t, "z0", balanceTask.Balance.SourceIdc)
	balanceTaskID := balanceTask.Balance.TaskID

	_, err = schedulerCli.RepairTaskDetail(context.Background(), &scheduler.TaskStatArgs{TaskId: repairTaskID})
	require.NoError(t, err)
	_, err = schedulerCli.BalanceTaskDetail(context.Background(), &scheduler.TaskStatArgs{TaskId: balanceTaskID})
	require.NoError(t, err)

	// cancel task
	err = schedulerCli.CancelTask(context.Background(), &scheduler.CancelTaskArgs{
		TaskId:   repairTask.Repair.TaskID,
		TaskType: repairTask.TaskType,
		IDC:      repairTask.Repair.BrokenDiskIDC,
		Src:      repairTask.Repair.Sources,
		Dest:     repairTask.Repair.Destination,
	})
	require.NoError(t, err)

	err = schedulerCli.CancelTask(context.Background(), &scheduler.CancelTaskArgs{
		TaskId:   balanceTask.Balance.TaskID,
		TaskType: balanceTask.TaskType,
		IDC:      balanceTask.Balance.SourceIdc,
		Src:      balanceTask.Balance.Sources,
		Dest:     balanceTask.Balance.Destination,
	})
	require.NoError(t, err)

	_, err = schedulerCli.AcquireTask(context.Background(), &scheduler.AcquireArgs{IDC: "z0"})
	require.EqualError(t, err, errors.ErrNothingTodo.Error())

	// wait task punish time
	time.Sleep(time.Duration(cancelS) * time.Second)
	// acquire task again
	repairTask, err = schedulerCli.AcquireTask(context.Background(), &scheduler.AcquireArgs{IDC: "z0"})
	require.NoError(t, err)
	require.Equal(t, proto.RepairTaskType, repairTask.TaskType)
	require.Equal(t, "z0", repairTask.Repair.BrokenDiskIDC)

	balanceTask, err = schedulerCli.AcquireTask(context.Background(), &scheduler.AcquireArgs{IDC: "z0"})
	require.NoError(t, err)
	require.Equal(t, proto.BalanceTaskType, balanceTask.TaskType)
	require.Equal(t, "z0", balanceTask.Balance.SourceIdc)

	// complete task
	err = schedulerCli.CompleteTask(context.Background(), &scheduler.CompleteTaskArgs{
		TaskId:   repairTask.Repair.TaskID,
		TaskType: repairTask.TaskType,
		IDC:      repairTask.Repair.BrokenDiskIDC,
		Src:      repairTask.Repair.Sources,
		Dest:     repairTask.Repair.Destination,
	})
	require.NoError(t, err)

	err = schedulerCli.CompleteTask(context.Background(), &scheduler.CompleteTaskArgs{
		TaskId:   balanceTask.Balance.TaskID,
		TaskType: balanceTask.TaskType,
		IDC:      balanceTask.Balance.SourceIdc,
		Src:      balanceTask.Balance.Sources,
		Dest:     balanceTask.Balance.Destination,
	})
	require.NoError(t, err)

	inspectTask, err := schedulerCli.AcquireInspectTask(context.Background())
	fmt.Printf("inspectTask %+v\n", inspectTask)
	require.NoError(t, err)
	require.NotEqual(t, 0, len(inspectTask.Task.TaskId))

	err = schedulerCli.CompleteInspect(context.Background(), &scheduler.CompleteInspectArgs{
		InspectRet: &proto.InspectRet{
			TaskID: inspectTask.Task.TaskId,
		},
	})
	require.NoError(t, err)

	aliveRepairTasks := make(map[string]struct{}, 1)
	aliveRepairTasks[repairTask.Repair.TaskID] = struct{}{}

	aliveBalanceTasks := make(map[string]struct{}, 1)
	aliveBalanceTasks[balanceTask.Balance.TaskID] = struct{}{}

	alive := &scheduler.TaskRenewalArgs{
		IDC:     "z0",
		Repair:  aliveRepairTasks,
		Balance: aliveBalanceTasks,
	}
	_, err = schedulerCli.RenewalTask(context.Background(), alive)
	require.NoError(t, err)

	// test err task type
	err = schedulerCli.ReclaimTask(context.Background(), &scheduler.ReclaimTaskArgs{
		TaskType: "err_task_type",
	})
	require.EqualError(t, err, errors.ErrIllegalTaskType.Error())
	err = schedulerCli.CancelTask(context.Background(), &scheduler.CancelTaskArgs{
		TaskType: "err_task_type",
	})
	require.EqualError(t, err, errors.ErrIllegalTaskType.Error())
	err = schedulerCli.CompleteTask(context.Background(), &scheduler.CompleteTaskArgs{
		TaskType: "err_task_type",
	})
	require.EqualError(t, err, errors.ErrIllegalTaskType.Error())

	task, err := schedulerCli.DropTaskDetail(context.Background(), &scheduler.TaskStatArgs{TaskId: ""})
	require.Error(t, err)
	require.EqualError(t, baseErr.New("task not found"), err.Error())
	fmt.Printf("DropTaskDetail task %+v\n", task)

	_, err = schedulerCli.ManualMigrateTaskDetail(context.Background(), &scheduler.TaskStatArgs{TaskId: ""})
	require.Error(t, err)

	// stats
	_, err = schedulerCli.Stats(context.Background())
	require.NoError(t, err)

	// add manual migrate task
	err = schedulerCli.AddManualMigrateTask(context.Background(), &scheduler.AddManualMigrateArgs{Vuid: 0})
	require.Error(t, err)
	require.EqualError(t, errors.ErrIllegalArguments, err.Error())
}

func newServiceRegisterTbl() db.ISvrRegisterTbl {
	serviceRegisterMap := make(map[string]*proto.SvrInfo)
	svr1 := &proto.SvrInfo{
		ClusterID: proto.ClusterID(1),
		Host:      "127.0.0.1:xxx",
		Module:    proto.TinkerModule,
		IDC:       "z0",
		Ctime:     time.Now().String(),
	}
	svr2 := &proto.SvrInfo{
		ClusterID: proto.ClusterID(1),
		Host:      "127.0.0.2:xxx",
		Module:    proto.TinkerModule,
		IDC:       "z1",
		Ctime:     time.Now().String(),
	}
	svr3 := &proto.SvrInfo{
		ClusterID: proto.ClusterID(1),
		Host:      "127.0.0.3:xxx",
		Module:    proto.TinkerModule,
		IDC:       "z2",
		Ctime:     time.Now().String(),
	}
	serviceRegisterMap[svr1.Host] = svr1
	serviceRegisterMap[svr2.Host] = svr2
	serviceRegisterMap[svr3.Host] = svr3

	return NewMockServiceRegisterTbl(nil, serviceRegisterMap)
}

func newBalanceTbl() db.IMigrateTaskTbl {
	taskMap := make(map[string]*proto.MigrateTask)
	volInfos := make(map[proto.Vid]*client.VolumeInfoSimple)
	volInfos[500] = MockMigrateGenVolInfo(500, codemode.EC6P6, proto.VolumeStatusIdle)
	volInfos[501] = MockMigrateGenVolInfo(501, codemode.EC6P10L2, proto.VolumeStatusIdle)
	volInfos[502] = MockMigrateGenVolInfo(502, codemode.EC6P10L2, proto.VolumeStatusActive)
	volInfos[503] = MockMigrateGenVolInfo(503, codemode.EC6P6, proto.VolumeStatusLock)
	volInfos[504] = MockMigrateGenVolInfo(504, codemode.EC6P6, proto.VolumeStatusLock)

	t1 := mockGenMigrateTask("z0", 4, 500, proto.MigrateStateInited, volInfos)
	t2 := mockGenMigrateTask("z0", 5, 501, proto.MigrateStatePrepared, volInfos)
	t3 := mockGenMigrateTask("z1", 6, 502, proto.MigrateStateWorkCompleted, volInfos)
	t4 := mockGenMigrateTask("z2", 7, 503, proto.MigrateStateFinishedInAdvance, volInfos)
	t5 := mockGenMigrateTask("z2", 8, 504, proto.MigrateStateFinished, volInfos)

	taskMap[t1.TaskID] = t1
	taskMap[t2.TaskID] = t2
	taskMap[t3.TaskID] = t3
	taskMap[t4.TaskID] = t4
	taskMap[t5.TaskID] = t5
	return NewMockMigrateTbl(nil, taskMap)
}

func newDiskDropTbl() db.IMigrateTaskTbl {
	taskMap := make(map[string]*proto.MigrateTask)
	return NewMockMigrateTbl(nil, taskMap)
}

func newManualMigTbl() db.IMigrateTaskTbl {
	taskMap := make(map[string]*proto.MigrateTask)
	return NewMockMigrateTbl(nil, taskMap)
}

func newVolRepairTbl() db.IRepairTaskTbl {
	taskMap := make(map[string]*proto.VolRepairTask)
	volInfos := make(map[proto.Vid]*client.VolumeInfoSimple)
	volInfos[601] = MockGenVolInfo(601, codemode.EC6P6, proto.VolumeStatusIdle)
	volInfos[602] = MockGenVolInfo(602, codemode.EC6P10L2, proto.VolumeStatusIdle)
	volInfos[603] = MockGenVolInfo(603, codemode.EC6P10L2, proto.VolumeStatusActive)
	volInfos[604] = MockGenVolInfo(604, codemode.EC6P6, proto.VolumeStatusLock)
	volInfos[605] = MockGenVolInfo(605, codemode.EC6P6, proto.VolumeStatusLock)
	t1 := mockGenVolRepairTask(601, proto.RepairStateInited, 1, volInfos)
	t2 := mockGenVolRepairTask(602, proto.RepairStatePrepared, 1, volInfos)
	t3 := mockGenVolRepairTask(603, proto.RepairStateFinishedInAdvance, 1, volInfos)
	t4 := mockGenVolRepairTask(604, proto.RepairStateWorkCompleted, 1, volInfos)
	t5 := mockGenVolRepairTask(605, proto.RepairStateFinished, 1, volInfos)

	taskMap[t1.TaskID] = t1
	taskMap[t2.TaskID] = t2
	taskMap[t3.TaskID] = t3
	taskMap[t4.TaskID] = t4
	taskMap[t5.TaskID] = t5

	for _, t := range taskMap {
		fmt.Printf("%+v \n", t)
	}

	return NewMockRepairTbl(nil, taskMap)
}

//----------------------------------------------------------------------
func newMockInspectCheckPointTbl() db.IInspectCheckPointTbl {
	return newMockCheckpointTbl()
}
