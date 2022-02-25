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
	"errors"
	"sort"
	"sync"
	"time"

	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/interrupt"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	api2 "github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/blobstore/util/log"
)

const (
	collectBalanceTaskPauseS = 5
)

var (
	// ErrNoBalanceVunit no balance volume unit on disk
	ErrNoBalanceVunit = errors.New("no balance volume unit on disk")
	// ErrTooManyBalancingTasks too many balancing tasks
	ErrTooManyBalancingTasks = errors.New("too many balancing tasks")
)

// BalanceMgrConfig balance task manager config
type BalanceMgrConfig struct {
	BalanceDiskCntLimit int   `json:"balance_disk_cnt_limit"`
	MaxDiskFreeChunkCnt int64 `json:"max_disk_free_chunk_cnt"`
	MinDiskFreeChunkCnt int64 `json:"min_disk_free_chunk_cnt"`
	MigrateConfig
}

// BalanceMgr balance manager
type BalanceMgr struct {
	migrateMgr     *MigrateMgr
	clusterTopoMgr *ClusterTopologyMgr

	taskSwitch *taskswitch.TaskSwitch
	cfg        *BalanceMgrConfig

	taskStatsMgr *base.TaskStatsMgr

	closeOnce *sync.Once
	closeDone chan struct{}
}

// NewBalanceMgr returns balance manager
func NewBalanceMgr(
	cmCli IMigrateCmCli,
	tinkerCli ITinkerCli,
	switchMgr *taskswitch.SwitchMgr,
	clusterTopMgr *ClusterTopologyMgr,
	svrTbl db.ISvrRegisterTbl,
	taskTbl db.IMigrateTaskTbl,
	conf *BalanceMgrConfig) (mgr *BalanceMgr, err error,
) {
	taskSwitch, err := switchMgr.AddSwitch(taskswitch.BalanceSwitchName)
	if err != nil {
		panic("unexpect add task switch fail")
	}

	mgr = &BalanceMgr{
		taskSwitch:     taskSwitch,
		clusterTopoMgr: clusterTopMgr,
		cfg:            conf,

		closeOnce: &sync.Once{},
		closeDone: make(chan struct{}),
	}
	mgr.migrateMgr = NewMigrateMgr(
		cmCli,
		tinkerCli,
		taskSwitch,
		svrTbl,
		taskTbl,
		&conf.MigrateConfig,
		proto.BalanceTaskType)

	mgr.migrateMgr.SetLockFailHandleFunc(mgr.migrateMgr.FinishTaskInAdvanceWhenLockFail)
	// stats
	mgr.taskStatsMgr = base.NewTaskStatsMgrAndRun(conf.ClusterID, proto.BalanceTaskType, mgr)
	return
}

// Load load balance task for database
func (mgr *BalanceMgr) Load() (err error) {
	return mgr.migrateMgr.Load()
}

// Run run balance task manager
func (mgr *BalanceMgr) Run() {
	go mgr.collectTaskLoop()
	mgr.migrateMgr.Run()
	go mgr.clearTaskLoop()
}

// Close close balance task manager
func (mgr *BalanceMgr) Close() {
	mgr.clusterTopoMgr.Close()

	mgr.closeOnce.Do(func() {
		close(mgr.closeDone)
	})
}

func (mgr *BalanceMgr) collectTaskLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CollectTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.taskSwitch.WaitEnable()
			err := mgr.collectionTask()
			if err == ErrTooManyBalancingTasks || err == ErrNoBalanceVunit {
				log.Debugf("no task to collect %v, sleep %d second", err, collectBalanceTaskPauseS)
				time.Sleep(time.Duration(collectBalanceTaskPauseS) * time.Second)
			}
		case <-mgr.closeDone:
			return
		}
	}
}

func (mgr *BalanceMgr) collectionTask() (err error) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "BalanceMgr.collectionTask")
	defer span.Finish()

	needBalanceDiskCnt := mgr.cfg.BalanceDiskCntLimit - mgr.migrateMgr.GetMigratingDiskNum()
	if needBalanceDiskCnt <= 0 {
		span.Warnf("the number of balancing disk is greater than config, cur:%d, conf:%d",
			mgr.migrateMgr.GetMigratingDiskNum(), mgr.cfg.BalanceDiskCntLimit)
		return ErrTooManyBalancingTasks
	}

	// select balance disks
	disks := mgr.selectDisks(mgr.cfg.MaxDiskFreeChunkCnt, mgr.cfg.MinDiskFreeChunkCnt)
	span.Debugf("select disks num: %d, ", len(disks))

	balanceDiskCnt := 0
	for _, disk := range disks {
		err = mgr.genOneBalanceTask(ctx, disk)
		if err != nil {
			continue
		}

		balanceDiskCnt++
		if balanceDiskCnt >= needBalanceDiskCnt {
			break
		}
	}
	// if balanceDiskCnt==0, means there is no balance volume unit on disk and need to do collect task later
	if balanceDiskCnt == 0 {
		span.Infof("select disks num %d and no balance volume unit on disk", len(disks))
		return ErrNoBalanceVunit
	}

	return nil
}

func (mgr *BalanceMgr) selectDisks(maxFreeChunkCnt, minFreeChunkCnt int64) []*api2.DiskInfoSimple {
	var allDisks []*api2.DiskInfoSimple
	for idcName := range mgr.clusterTopoMgr.GetIDCs() {
		if idcDisks, ok := mgr.clusterTopoMgr.GetIDCDisks(idcName); ok {
			if freeChunkCntMax(idcDisks) >= maxFreeChunkCnt {
				allDisks = append(allDisks, idcDisks...)
			}
		}
	}

	var selected []*api2.DiskInfoSimple
	for _, disk := range allDisks {
		if !disk.IsHealth() {
			continue
		}
		if ok := mgr.migrateMgr.IsMigratingDisk(disk.DiskID); ok {
			continue
		}
		if disk.FreeChunkCnt < minFreeChunkCnt {
			selected = append(selected, disk)
		}
	}
	return selected
}

func (mgr *BalanceMgr) genOneBalanceTask(ctx context.Context, diskInfo *client.DiskInfoSimple) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	vuid, err := mgr.selectBalanceVunit(ctx, diskInfo.DiskID)
	if err != nil {
		span.Errorf("generate task source failed, diskId: %d, err:%v", diskInfo.DiskID, err)
		return
	}

	span.Debugf("select balance volume unit info, vuid: %d, volumeId: %v", vuid, vuid.Vid())
	task := &proto.MigrateTask{
		TaskID: mgr.genUniqTaskID(vuid.Vid()),
		State:  proto.MigrateStateInited,

		SourceIdc:    diskInfo.Idc,
		SourceDiskID: diskInfo.DiskID,
		SourceVuid:   vuid,
	}
	interrupt.Inject("balance_collect_task")
	mgr.migrateMgr.AddTask(ctx, task)
	return
}

func (mgr *BalanceMgr) selectBalanceVunit(ctx context.Context, diskID proto.DiskID) (vuid proto.Vuid, err error) {
	span := trace.SpanFromContextSafe(ctx)

	vunits, err := mgr.migrateMgr.clusterMgrClient.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		return
	}

	sortVunitByUsed(vunits)

	for i := range vunits {
		volInfo, err := mgr.migrateMgr.clusterMgrClient.GetVolumeInfo(ctx, vunits[i].Vuid.Vid())
		if err != nil {
			span.Errorf("get volume info failed, vid: %d, err:%v", vunits[i].Vuid.Vid(), err)
			continue
		}
		if volInfo.IsIdle() {
			return vunits[i].Vuid, nil
		}
	}
	return vuid, ErrNoBalanceVunit
}

func (mgr *BalanceMgr) clearTaskLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CheckTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.taskSwitch.WaitEnable()
			mgr.ClearFinishedTask()
		case <-mgr.closeDone:
			return
		}
	}
}

// ClearFinishedTask clear finished balance task
func (mgr *BalanceMgr) ClearFinishedTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "BalanceMgr.ClearFinishedTask")
	defer span.Finish()

	clearStates := []proto.MigrateSate{proto.MigrateStateFinished, proto.MigrateStateFinishedInAdvance}
	mgr.migrateMgr.ClearTasksByStates(ctx, clearStates)
}

// AcquireTask acquire balance task
func (mgr *BalanceMgr) AcquireTask(ctx context.Context, idc string) (task *proto.MigrateTask, err error) {
	return mgr.migrateMgr.AcquireTask(ctx, idc)
}

// CancelTask cancel balance task
func (mgr *BalanceMgr) CancelTask(ctx context.Context, args *api.CancelTaskArgs) (err error) {
	mgr.taskStatsMgr.CancelTask()
	return mgr.migrateMgr.CancelTask(ctx, args)
}

// ReclaimTask reclaim balance task
func (mgr *BalanceMgr) ReclaimTask(
	ctx context.Context,
	idc, taskID string,
	src []proto.VunitLocation,
	oldDst proto.VunitLocation,
	newDst *client.AllocVunitInfo) (err error) {
	mgr.taskStatsMgr.ReclaimTask()
	return mgr.migrateMgr.ReclaimTask(ctx, idc, taskID, src, oldDst, newDst)
}

// RenewalTask renewal balance task
func (mgr *BalanceMgr) RenewalTask(ctx context.Context, idc, taskID string) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("Renewal balance taskID %s", taskID)

	err = mgr.migrateMgr.RenewalTask(ctx, idc, taskID)
	if err != nil {
		span.Warnf("Renewal balance taskID %s fail error:%v", taskID, err)
	}

	return
}

// CompleteTask complete balance task
func (mgr *BalanceMgr) CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	err = mgr.migrateMgr.CompleteTask(ctx, args)
	if err != nil {
		span.Errorf("BalanceMgr complete task fail err %+v", err)
		return
	}
	return
}

// ReportWorkerTaskStats report balance task stats
func (mgr *BalanceMgr) ReportWorkerTaskStats(taskID string, s proto.TaskStatistics, increaseDataSize, increaseShardCnt int) {
	mgr.taskStatsMgr.ReportWorkerTaskStats(taskID, s, increaseDataSize, increaseShardCnt)
}

// QueryTask return task statistics with taskID
func (mgr *BalanceMgr) QueryTask(ctx context.Context, taskID string) (proto.MigrateTask, proto.TaskStatistics, error) {
	taskInfo, err := mgr.migrateMgr.taskTbl.Find(ctx, taskID)
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
func (mgr *BalanceMgr) GetTaskStats() (finish, dataSize, shardCnt [counter.SLOT]int) {
	increaseDataSize, increaseShardCnt := mgr.taskStatsMgr.Counters()
	return mgr.migrateMgr.finishTaskCounter.Show(), increaseDataSize, increaseShardCnt
}

// StatQueueTaskCnt returns queue task stat
func (mgr *BalanceMgr) StatQueueTaskCnt() (inited, prepared, completed int) {
	return mgr.migrateMgr.StatQueueTaskCnt()
}

func (mgr *BalanceMgr) genUniqTaskID(vid proto.Vid) string {
	return base.GenTaskID("balance", vid)
}

func sortVunitByUsed(vunits []*client.VunitInfoSimple) {
	sort.Slice(vunits, func(i, j int) bool {
		return vunits[i].Used < vunits[j].Used
	})
}

func freeChunkCntMax(disks []*api2.DiskInfoSimple) int64 {
	var max int64
	for _, disk := range disks {
		if disk.FreeChunkCnt > max {
			max = disk.FreeChunkCnt
		}
	}
	return max
}
