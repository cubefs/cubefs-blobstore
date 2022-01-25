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

	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
)

// IMigrateCmCliEx define the interface of clustermgr used by manual migrate
type IMigrateCmCliEx interface {
	GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *client.DiskInfoSimple, err error)
	IMigrateCmCli
}

// ManualMigrateMgr manual migrate manager
type ManualMigrateMgr struct {
	migrate      *MigrateMgr
	cmCli        IMigrateCmCliEx
	taskStatsMgr *base.TaskStatsMgr
}

// NewManualMigrateMgr returns manual migrate manager
func NewManualMigrateMgr(
	cmCli IMigrateCmCliEx,
	tinkerCli ITinkerCli,
	svrTbl db.ISvrRegisterTbl,
	taskTbl db.IMigrateTaskTbl,
	clusterID proto.ClusterID) *ManualMigrateMgr {
	mgr := &ManualMigrateMgr{
		cmCli: cmCli,
	}
	cfg := defaultMigrateConfig(clusterID)

	mgr.migrate = NewMigrateMgr(
		cmCli,
		tinkerCli,
		taskswitch.NewEnabledTaskSwitch(),
		svrTbl,
		taskTbl,
		&cfg,
		proto.ManualMigrateType)

	mgr.migrate.SetLockFailHandleFunc(mgr.migrate.FinishTaskInAdvanceWhenLockFail)
	// stats
	mgr.taskStatsMgr = base.NewTaskStatsMgr(clusterID, proto.ManualMigrateType)
	return mgr
}

// Load load manual migrate task
func (mgr *ManualMigrateMgr) Load() error {
	return mgr.migrate.Load()
}

// Run run manual migrate task
func (mgr *ManualMigrateMgr) Run() {
	mgr.migrate.Run()
}

// AddTask add manual migrate task
func (mgr *ManualMigrateMgr) AddTask(ctx context.Context, vuid proto.Vuid, forbiddenDirectDownload bool) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	volume, err := mgr.cmCli.GetVolumeInfo(ctx, vuid.Vid())
	if err != nil {
		span.Errorf("get vid %d volume fail err:%+v", vuid.Vid(), err)
		return err
	}
	diskID := volume.VunitLocations[vuid.Index()].DiskID
	disk, err := mgr.cmCli.GetDiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("get disk info diskID %d fail err:%+v", err)
		return err
	}

	task := &proto.MigrateTask{
		TaskID:                  mgr.genUniqTaskID(vuid.Vid()),
		State:                   proto.MigrateStateInited,
		SourceIdc:               disk.Idc,
		SourceDiskID:            disk.DiskID,
		SourceVuid:              vuid,
		ForbiddenDirectDownload: forbiddenDirectDownload,
	}
	mgr.migrate.AddTask(ctx, task)

	span.Infof("add manual migrate task success! task_info:%+v", task)
	return nil
}

func (mgr *ManualMigrateMgr) genUniqTaskID(vid proto.Vid) string {
	return base.GenTaskID("manual_migrate", vid)
}

// AcquireTask acquire manual migrate task
func (mgr *ManualMigrateMgr) AcquireTask(ctx context.Context, idc string) (task *proto.MigrateTask, err error) {
	return mgr.migrate.AcquireTask(ctx, idc)
}

// CancelTask cancel manual migrate task
func (mgr *ManualMigrateMgr) CancelTask(ctx context.Context, args *api.CancelTaskArgs) (err error) {
	mgr.taskStatsMgr.CancelTask()
	return mgr.migrate.CancelTask(ctx, args)
}

// ReclaimTask reclaim manual migrate task
func (mgr *ManualMigrateMgr) ReclaimTask(
	ctx context.Context,
	idc, taskID string,
	src []proto.VunitLocation,
	oldDst proto.VunitLocation,
	newDst *client.AllocVunitInfo) (err error) {
	mgr.taskStatsMgr.ReclaimTask()
	return mgr.migrate.ReclaimTask(ctx, idc, taskID, src, oldDst, newDst)
}

// RenewalTask renewal manual migrate task
func (mgr *ManualMigrateMgr) RenewalTask(ctx context.Context, idc, taskID string) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("Renewal manual migrate taskID %s", taskID)

	err = mgr.migrate.RenewalTask(ctx, idc, taskID)
	if err != nil {
		span.Warnf("Renewal manual migrate taskID %s fail error:%v", taskID, err)
	}
	return
}

// CompleteTask complete manual migrate task
func (mgr *ManualMigrateMgr) CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	err = mgr.migrate.CompleteTask(ctx, args)
	if err != nil {
		span.Errorf("ManualMigrateMgr complete task fail err %+v", err)
		return
	}
	return
}

// ReportWorkerTaskStats report task stats
func (mgr *ManualMigrateMgr) ReportWorkerTaskStats(
	taskID string,
	s proto.TaskStatistics,
	increaseDataSize,
	increaseShardCnt int) {
	mgr.taskStatsMgr.ReportWorkerTaskStats(taskID, s, increaseDataSize, increaseShardCnt)
}

// QueryTask gets task statistics
func (mgr *ManualMigrateMgr) QueryTask(ctx context.Context, taskID string) (proto.MigrateTask, proto.TaskStatistics, error) {
	taskInfo, err := mgr.migrate.taskTbl.Find(ctx, taskID)
	if err != nil {
		return proto.MigrateTask{}, proto.TaskStatistics{}, err
	}
	detailRunInfo, err := mgr.taskStatsMgr.QueryTaskDetail(taskID)
	if err != nil {
		return *taskInfo, proto.TaskStatistics{}, nil
	}
	return *taskInfo, detailRunInfo.Statistics, nil
}

// GetTaskStats returns task stats
func (mgr *ManualMigrateMgr) GetTaskStats() (finish, dataSize, shardCnt [counter.SLOT]int) {
	increaseDataSize, increaseShardCnt := mgr.taskStatsMgr.Counters()
	return mgr.migrate.finishTaskCounter.Show(), increaseDataSize, increaseShardCnt
}

// StatQueueTaskCnt returns queue stats
func (mgr *ManualMigrateMgr) StatQueueTaskCnt() (inited, prepared, completed int) {
	return mgr.migrate.StatQueueTaskCnt()
}

func defaultMigrateConfig(clusterID proto.ClusterID) MigrateConfig {
	cfg := MigrateConfig{
		ClusterID: clusterID,
	}
	cfg.CheckAndFix()
	return cfg
}
