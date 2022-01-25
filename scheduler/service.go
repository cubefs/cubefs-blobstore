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
	"errors"
	"fmt"
	"net/http"

	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/counter"
	comerrs "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
)

// Service rpc service
type Service struct {
	ClusterID proto.ClusterID

	clusterTopoMgr *ClusterTopologyMgr
	balanceMgr     *BalanceMgr
	diskDropMgr    *DiskDropMgr
	manualMigMgr   *ManualMigrateMgr
	repairMgr      *RepairMgr
	inspectMgr     *InspectMgr

	svrTbl db.ISvrRegisterTbl

	cmCli client.IClusterMgr
}

// HTTPTaskAcquire acquire task
func (svr *Service) HTTPTaskAcquire(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.AcquireArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	manualMigTask, err := svr.manualMigMgr.AcquireTask(ctx, args.IDC)
	if err == nil {
		ret := &api.WorkerTask{
			TaskType:      proto.ManualMigrateType,
			ManualMigrate: manualMigTask,
		}
		c.RespondJSON(ret)
		return
	}

	repairTask, err := svr.repairMgr.AcquireTask(ctx, args.IDC)
	if err == nil {
		ret := &api.WorkerTask{
			TaskType: proto.RepairTaskType,
			Repair:   repairTask,
		}
		c.RespondJSON(ret)
		return
	}

	diskDropTask, err := svr.diskDropMgr.AcquireTask(ctx, args.IDC)
	if err == nil {
		ret := &api.WorkerTask{
			TaskType: proto.DiskDropTaskType,
			DiskDrop: diskDropTask,
		}
		c.RespondJSON(ret)
		return
	}

	balanceTask, err := svr.balanceMgr.AcquireTask(ctx, args.IDC)
	if err == nil {
		ret := &api.WorkerTask{
			TaskType: proto.BalanceTaskType,
			Balance:  balanceTask,
		}
		c.RespondJSON(ret)
		return
	}

	c.RespondError(comerrs.ErrNothingTodo)
}

// HTTPTaskReclaim reclaim task
func (svr *Service) HTTPTaskReclaim(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	args := new(api.ReclaimTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("reclaim task args==>%+v", args)

	newDst, err := base.AllocVunitSafe(ctx, svr.cmCli, args.Dest.Vuid, args.Src)
	if err != nil {
		c.RespondError(err)
		return
	}

	switch args.TaskType {
	case proto.RepairTaskType:
		err = svr.repairMgr.ReclaimTask(ctx, args.IDC, args.TaskId, args.Src, args.Dest, newDst)
	case proto.BalanceTaskType:
		err = svr.balanceMgr.ReclaimTask(ctx, args.IDC, args.TaskId, args.Src, args.Dest, newDst)
	case proto.DiskDropTaskType:
		err = svr.diskDropMgr.ReclaimTask(ctx, args.IDC, args.TaskId, args.Src, args.Dest, newDst)
	case proto.ManualMigrateType:
		err = svr.manualMigMgr.ReclaimTask(ctx, args.IDC, args.TaskId, args.Src, args.Dest, newDst)
	default:
		c.RespondError(rpc.NewError(http.StatusBadRequest, "illegal_type", comerrs.ErrIllegalTaskType))
		return
	}
	c.RespondError(err)
}

// HTTPTaskCancel cancel task
func (svr *Service) HTTPTaskCancel(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	args := new(api.CancelTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("cancel task args==>%+v", args)

	var err error
	switch args.TaskType {
	case proto.RepairTaskType:
		err = svr.repairMgr.CancelTask(ctx, args)
	case proto.BalanceTaskType:
		err = svr.balanceMgr.CancelTask(ctx, args)
	case proto.DiskDropTaskType:
		err = svr.diskDropMgr.CancelTask(ctx, args)
	case proto.ManualMigrateType:
		err = svr.manualMigMgr.CancelTask(ctx, args)
	default:
		c.RespondError(rpc.NewError(http.StatusBadRequest, "illegal_type", comerrs.ErrIllegalTaskType))
		return
	}
	c.RespondError(err)
}

// HTTPTaskComplete complete task
func (svr *Service) HTTPTaskComplete(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	args := new(api.CompleteTaskArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("complete task args==>%+v", args)

	var err error
	switch args.TaskType {
	case proto.RepairTaskType:
		err = svr.repairMgr.CompleteTask(ctx, args)
	case proto.BalanceTaskType:
		err = svr.balanceMgr.CompleteTask(ctx, args)
	case proto.DiskDropTaskType:
		err = svr.diskDropMgr.CompleteTask(ctx, args)
	case proto.ManualMigrateType:
		err = svr.manualMigMgr.CompleteTask(ctx, args)
	default:
		c.RespondError(rpc.NewError(http.StatusBadRequest, "illegal_type", comerrs.ErrIllegalTaskType))
		return
	}

	c.RespondError(err)
}

// HTTPInspectAcquire acquire inspect task
func (svr *Service) HTTPInspectAcquire(c *rpc.Context) {
	ctx := c.Request.Context()

	if svr.inspectMgr != nil {
		task, _ := svr.inspectMgr.AcquireInspect(ctx)
		if task != nil {
			c.RespondJSON(api.WorkerInspectTask{Task: task})
			return
		}
	}

	c.RespondError(comerrs.ErrNothingTodo)
}

// HTTPInspectComplete complete inspect task
func (svr *Service) HTTPInspectComplete(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.CompleteInspectArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if svr.inspectMgr != nil {
		svr.inspectMgr.CompleteInspect(ctx, args.InspectRet)
		c.Respond()
		return
	}
	c.RespondError(comerrs.ErrNothingTodo)
}

// HTTPTaskRenewal renewal task
func (svr *Service) HTTPTaskRenewal(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.TaskRenewalArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	idc := args.IDC
	ret := &api.TaskRenewalRet{
		Repair:        make(map[string]string),
		Balance:       make(map[string]string),
		DiskDrop:      make(map[string]string),
		ManualMigrate: make(map[string]string),
	}

	for taskID := range args.Repair {
		err := svr.repairMgr.RenewalTask(ctx, idc, taskID)
		ret.Repair[taskID] = getErrMsg(err)
	}

	for taskID := range args.Balance {
		err := svr.balanceMgr.RenewalTask(ctx, idc, taskID)
		ret.Balance[taskID] = getErrMsg(err)
	}

	for taskID := range args.DiskDrop {
		err := svr.diskDropMgr.RenewalTask(ctx, idc, taskID)
		ret.DiskDrop[taskID] = getErrMsg(err)
	}

	for taskID := range args.ManualMigrate {
		err := svr.manualMigMgr.RenewalTask(ctx, idc, taskID)
		ret.ManualMigrate[taskID] = getErrMsg(err)
	}

	c.RespondJSON(ret)
}

func getErrMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// HTTPTaskReport reports task stats
func (svr *Service) HTTPTaskReport(c *rpc.Context) {
	args := new(api.TaskReportArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	switch args.TaskType {
	case proto.RepairTaskType:
		svr.repairMgr.ReportWorkerTaskStats(
			args.TaskId,
			args.TaskStats,
			args.IncreaseDataSizeByte,
			args.IncreaseShardCnt)
	case proto.BalanceTaskType:
		svr.balanceMgr.ReportWorkerTaskStats(
			args.TaskId,
			args.TaskStats,
			args.IncreaseDataSizeByte,
			args.IncreaseShardCnt)
	case proto.DiskDropTaskType:
		svr.diskDropMgr.ReportWorkerTaskStats(
			args.TaskId,
			args.TaskStats,
			args.IncreaseDataSizeByte,
			args.IncreaseShardCnt)

	case proto.ManualMigrateType:
		svr.manualMigMgr.ReportWorkerTaskStats(
			args.TaskId,
			args.TaskStats,
			args.IncreaseDataSizeByte,
			args.IncreaseShardCnt)
	}

	c.Respond()
}

// HTTPServiceRegister register service
func (svr *Service) HTTPServiceRegister(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.RegisterServiceArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if !args.Valid(svr.ClusterID) {
		c.RespondError(comerrs.ErrIllegalArguments)
		return
	}

	svrInfo := &proto.SvrInfo{
		ClusterID: args.ClusterID,
		Host:      args.Host,
		Module:    args.Module,
		IDC:       args.Idc,
	}
	err := svr.svrTbl.Register(ctx, svrInfo)
	c.RespondError(err)
}

// HTTPServiceDelete deletes service
func (svr *Service) HTTPServiceDelete(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.DeleteServiceArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if args.Host == "" {
		c.RespondError(comerrs.ErrIllegalArguments)
		return
	}

	err := svr.svrTbl.Delete(ctx, args.Host)
	c.RespondError(err)
}

// HTTPServiceGet returns service
func (svr *Service) HTTPServiceGet(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.FindServiceArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	service, err := svr.svrTbl.Find(ctx, args.Host)
	if err == base.ErrNoDocuments {
		c.RespondError(comerrs.ErrNoSuchService)
		return
	}
	if err != nil {
		c.RespondError(rpc.Error2HTTPError(err))
		return
	}

	c.RespondJSON(service)
}

// HTTPServiceList returns service list
func (svr *Service) HTTPServiceList(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.ListServicesArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	services, err := svr.svrTbl.FindAll(ctx, args.Module, args.IDC)
	if err != nil {
		c.RespondStatusData(rpc.Error2HTTPError(err).StatusCode(), services)
	}
	c.RespondStatusData(200, services)
}

// HTTPBalanceTaskDetail returns balance task detail stats
func (svr *Service) HTTPBalanceTaskDetail(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.TaskStatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	taskInfo, runStats, err := svr.balanceMgr.QueryTask(ctx, args.TaskId)
	if err != nil {
		c.RespondStatusData(404, []byte(`{"error":"task not found"}`))
		return
	}

	taskDetail := api.MigrateTaskDetail{
		TaskInfo: taskInfo,
		RunStats: runStats,
	}
	c.RespondJSON(taskDetail)
}

// HTTPDropTaskDetail returns disk drop task detail stats
func (svr *Service) HTTPDropTaskDetail(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.TaskStatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	taskInfo, runStats, err := svr.diskDropMgr.QueryTask(ctx, args.TaskId)
	if err != nil {
		c.RespondError(rpc.NewError(http.StatusNotFound, "not found", errors.New("task not found")))
		return
	}

	taskDetail := api.MigrateTaskDetail{
		TaskInfo: taskInfo,
		RunStats: runStats,
	}
	c.RespondJSON(taskDetail)
}

// HTTPManualMigrateTaskDetail returns manual migrate task detail stats
func (svr *Service) HTTPManualMigrateTaskDetail(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.TaskStatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	taskInfo, runStats, err := svr.manualMigMgr.QueryTask(ctx, args.TaskId)
	if err != nil {
		c.RespondError(rpc.NewError(http.StatusNotFound, "not found", errors.New("task not found")))
		return
	}

	taskDetail := api.MigrateTaskDetail{
		TaskInfo: taskInfo,
		RunStats: runStats,
	}
	c.RespondJSON(taskDetail)
}

// HTTPRepairTaskDetail returns repair task detail stats
func (svr *Service) HTTPRepairTaskDetail(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.TaskStatArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	taskInfo, runStats, err := svr.repairMgr.QueryTask(ctx, args.TaskId)
	if err != nil {
		c.RespondError(rpc.NewError(http.StatusNotFound, "not found", errors.New("task not found")))
		return
	}

	taskDetail := api.RepairTaskDetail{
		TaskInfo: taskInfo,
		RunStats: runStats,
	}
	c.RespondJSON(taskDetail)
}

// HTTPStats returns service stats
func (svr *Service) HTTPStats(c *rpc.Context) {
	ctx := c.Request.Context()

	// stats repair tasks
	finishedCnt, dataSizeByte, shardCnt := svr.repairMgr.GetTaskStats()
	preparing, workerDoing, finishing := svr.repairMgr.StatQueueTaskCnt()
	repairDiskID, totalTasksCnt, repairedTasksCnt := svr.repairMgr.Progress(ctx)

	var switchStatus string
	if svr.repairMgr.taskSwitch.Enabled() {
		switchStatus = taskswitch.SwitchOpen
	} else {
		switchStatus = taskswitch.SwitchClose
	}

	repair := api.RepairTasksStat{
		Switch: switchStatus,

		RepairingDiskId:  repairDiskID,
		TotalTasksCnt:    totalTasksCnt,
		RepairedTasksCnt: repairedTasksCnt,

		PreparingCnt:   preparing,
		WorkerDoingCnt: workerDoing,
		FinishingCnt:   finishing,
		StatsPerMin: api.PerMinStats{
			FinishedCnt:    fmt.Sprint(finishedCnt),
			DataAmountByte: base.DataMountFormat(dataSizeByte),
			ShardCnt:       fmt.Sprint(shardCnt),
		},
	}
	// stats drop tasks
	finishedCnt, dataSizeByte, shardCnt = svr.diskDropMgr.GetTaskStats()
	preparing, workerDoing, finishing = svr.diskDropMgr.StatQueueTaskCnt()
	dropDiskID, totalTasksCnt, droppedTasksCnt := svr.diskDropMgr.Progress(ctx)
	if svr.diskDropMgr.taskSwitch.Enabled() {
		switchStatus = taskswitch.SwitchOpen
	} else {
		switchStatus = taskswitch.SwitchClose
	}

	drop := api.DiskDropTasksStat{
		Switch: switchStatus,

		DroppingDiskId:  dropDiskID,
		TotalTasksCnt:   totalTasksCnt,
		DroppedTasksCnt: droppedTasksCnt,

		MigrateTasksStat: api.MigrateTasksStat{
			PreparingCnt:   preparing,
			WorkerDoingCnt: workerDoing,
			FinishingCnt:   finishing,
			StatsPerMin: api.PerMinStats{
				FinishedCnt:    fmt.Sprint(finishedCnt),
				DataAmountByte: base.DataMountFormat(dataSizeByte),
				ShardCnt:       fmt.Sprint(shardCnt),
			},
		},
	}

	// stats balance tasks
	finishedCnt, dataSizeByte, shardCnt = svr.balanceMgr.GetTaskStats()
	preparing, workerDoing, finishing = svr.balanceMgr.StatQueueTaskCnt()
	if svr.balanceMgr.taskSwitch.Enabled() {
		switchStatus = taskswitch.SwitchOpen
	} else {
		switchStatus = taskswitch.SwitchClose
	}

	balance := api.BalanceTasksStat{
		Switch: switchStatus,
		MigrateTasksStat: api.MigrateTasksStat{
			PreparingCnt:   preparing,
			WorkerDoingCnt: workerDoing,
			FinishingCnt:   finishing,
			StatsPerMin: api.PerMinStats{
				FinishedCnt:    fmt.Sprint(finishedCnt),
				DataAmountByte: base.DataMountFormat(dataSizeByte),
				ShardCnt:       fmt.Sprint(shardCnt),
			},
		},
	}

	// stats manual migrate tasks
	finishedCnt, dataSizeByte, shardCnt = svr.manualMigMgr.GetTaskStats()
	preparing, workerDoing, finishing = svr.manualMigMgr.StatQueueTaskCnt()

	manualMigrate := api.ManualMigrateTasksStat{
		MigrateTasksStat: api.MigrateTasksStat{
			PreparingCnt:   preparing,
			WorkerDoingCnt: workerDoing,
			FinishingCnt:   finishing,
			StatsPerMin: api.PerMinStats{
				FinishedCnt:    fmt.Sprint(finishedCnt),
				DataAmountByte: base.DataMountFormat(dataSizeByte),
				ShardCnt:       fmt.Sprint(shardCnt),
			},
		},
	}

	// stats inspect tasks
	var finished, timeout [counter.SLOT]int
	if svr.inspectMgr != nil {
		if svr.inspectMgr.taskSwitch.Enabled() {
			switchStatus = taskswitch.SwitchOpen
		} else {
			switchStatus = taskswitch.SwitchClose
		}
		finished, timeout = svr.inspectMgr.GetTaskStats()
	} else {
		switchStatus = "inspectMgr nil"
	}

	inspect := api.InspectTasksStats{
		Switch:         switchStatus,
		FinishedPerMin: fmt.Sprint(finished),
		TimeOutPerMin:  fmt.Sprint(timeout),
	}
	//////
	taskStats := api.TasksStat{
		Repair:        repair,
		Drop:          drop,
		Balance:       balance,
		ManualMigrate: manualMigrate,
		Inspect:       inspect,
	}

	c.RespondJSON(taskStats)
}

// HTTPManualMigrateTaskAdd adds manual migrate task
func (svr *Service) HTTPManualMigrateTaskAdd(c *rpc.Context) {
	ctx := c.Request.Context()

	args := new(api.AddManualMigrateArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if !args.Valid() {
		c.RespondError(comerrs.ErrIllegalArguments)
		return
	}

	err := svr.manualMigMgr.AddTask(ctx, args.Vuid, !args.DirectDownload)
	c.RespondError(rpc.Error2HTTPError(err))
}
