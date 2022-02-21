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
	"time"

	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/interrupt"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/blobstore/util/log"
)

// DiskDropMgrConfig disk drop manager config
type DiskDropMgrConfig struct {
	MigrateConfig
}

type dropCmCli interface {
	ListDropDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error)
	SetDiskDropped(ctx context.Context, diskID proto.DiskID) (err error)
	GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *client.DiskInfoSimple, err error)
	IMigrateCmCli
}

// DiskDropMgr disk drop manager
type DiskDropMgr struct {
	migrateMgr     *MigrateMgr
	dropDisk       *client.DiskInfoSimple
	taskSwitch     *taskswitch.TaskSwitch
	mu             sync.Mutex
	droppingDiskID proto.DiskID
	cmCli          dropCmCli
	hasRevised     bool
	taskStatsMgr   *base.TaskStatsMgr
	cfg            *DiskDropMgrConfig

	closeOnce *sync.Once
	closeDone chan struct{}
}

// NewDiskDropMgr returns disk drop manager
func NewDiskDropMgr(
	cmCli dropCmCli,
	tinkerCli ITinkerCli,
	switchMgr *taskswitch.SwitchMgr,
	svrTbl db.ISvrRegisterTbl,
	taskTbl db.IMigrateTaskTbl,
	conf *DiskDropMgrConfig) (mgr *DiskDropMgr, err error) {
	taskSwitch, err := switchMgr.AddSwitch(taskswitch.DiskDropSwitchName)
	if err != nil {
		panic("unexpect add task switch fail")
	}

	mgr = &DiskDropMgr{
		taskSwitch: taskSwitch,
		cmCli:      cmCli,
		cfg:        conf,
		closeOnce:  &sync.Once{},
		closeDone:  make(chan struct{}),
	}

	mgr.migrateMgr = NewMigrateMgr(cmCli,
		tinkerCli,
		taskSwitch,
		svrTbl,
		taskTbl,
		&conf.MigrateConfig,
		proto.DiskDropTaskType)

	// stats
	mgr.taskStatsMgr = base.NewTaskStatsMgrAndRun(conf.ClusterID, proto.DiskDropTaskType, mgr)

	return
}

// Load load disk drop task from database
func (mgr *DiskDropMgr) Load() (err error) {
	err = mgr.migrateMgr.Load()
	if err != nil {
		return
	}

	allTasks, err := mgr.migrateMgr.GetAllTasks(context.Background())
	if err != nil {
		return err
	}
	if len(allTasks) == 0 {
		log.Infof("no drop tasks in db")
		return
	}

	mgr.setDroppingDiskID(allTasks[0].SrcMigDiskID())
	return
}

// Run run disk drop task
func (mgr *DiskDropMgr) Run() {
	go mgr.collectTaskLoop()
	mgr.migrateMgr.Run()
	go mgr.checkDroppedAndClearLoop()
}

// Close close repair task manager
func (mgr *DiskDropMgr) Close() {
	mgr.closeOnce.Do(func() {
		close(mgr.closeDone)
	})
}

// collectTaskLoop collect disk drop task loop
func (mgr *DiskDropMgr) collectTaskLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CollectTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.taskSwitch.WaitEnable()
			mgr.collectTask()
		case <-mgr.closeDone:
			return
		}
	}
}

func (mgr *DiskDropMgr) collectTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "DiskDropMgr.collectTask")
	defer span.Finish()

	if !mgr.hasRevised && mgr.hasDroppingDisk() {
		err := mgr.reviseDropTask(ctx)
		if err == nil {
			span.Infof("drop collect revise tasks success")
			mgr.hasRevised = true
			mgr.setDroppingDiskID(mgr.getDroppingDiskID())
			return
		}
		span.Errorf("drop collect revise task fail err:%+v", err)
		return
	}

	if mgr.hasDroppingDisk() {
		return
	}

	dropDisk, err := mgr.acquireDropDisk(ctx)
	if err != nil {
		span.Info("acquire drop disk fail err %+v", err)
		return
	}

	if dropDisk == nil {
		return
	}

	err = mgr.genDiskDropTasks(ctx, dropDisk.DiskID, dropDisk.Idc)
	if err != nil {
		span.Errorf("drop collect drop task fail err:%+v", err)
		return
	}

	mgr.setDroppingDiskID(dropDisk.DiskID)
}

func (mgr *DiskDropMgr) reviseDropTask(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	diskInfo, err := mgr.cmCli.GetDiskInfo(ctx, mgr.getDroppingDiskID())
	if err != nil {
		span.Errorf("cmCli.GetDiskInfo fail %+v", err)
		return err
	}

	err = mgr.genDiskDropTasks(ctx, diskInfo.DiskID, diskInfo.Idc)
	if err != nil {
		span.Errorf("gen disk drop tasks fail err:%+v", err)
		return err
	}
	return nil
}

func (mgr *DiskDropMgr) genDiskDropTasks(ctx context.Context, diskID proto.DiskID, diskIdc string) error {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("start genDiskDropTasks disk_id %d disk_idc %s", diskID, diskIdc)

	vuidsDb, err := mgr.dropVuidsFromDb(ctx, diskID)
	if err != nil {
		span.Errorf("get drop vuids from db fail %+v", err)
		return err
	}
	span.Infof("genDiskDropTasks drop vuids from DB len %d", len(vuidsDb))

	vuidsCm, err := mgr.dropVuidsFromCm(ctx, diskID)
	if err != nil {
		span.Errorf("get drop vuid from cm fail %+v", err)
		return err
	}
	span.Infof("genDiskDropTasks drop vuids from CM len %d", len(vuidsCm))

	remain := base.Subtraction(vuidsCm, vuidsDb)
	span.Infof("should gen tasks remain len %d", len(remain))
	for _, vuid := range remain {
		mgr.initOneTask(ctx, vuid, diskID, diskIdc)
		span.Infof("init drop task vuid %d success", vuid)
		interrupt.Inject("drop_init_one_task")
	}
	return nil
}

func (mgr *DiskDropMgr) dropVuidsFromDb(ctx context.Context, diskID proto.DiskID) (drops []proto.Vuid, err error) {
	tasks, err := mgr.migrateMgr.taskTbl.FindByDiskID(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, t := range tasks {
		drops = append(drops, t.SourceVuid)
	}
	return drops, nil
}

func (mgr *DiskDropMgr) dropVuidsFromCm(ctx context.Context, diskID proto.DiskID) (drops []proto.Vuid, err error) {
	vunits, err := mgr.cmCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, vunit := range vunits {
		drops = append(drops, vunit.Vuid)
	}
	return drops, nil
}

func (mgr *DiskDropMgr) initOneTask(ctx context.Context, src proto.Vuid, dropDiskID proto.DiskID, diskIDC string) {
	vid := src.Vid()
	t := proto.MigrateTask{
		TaskID:       mgr.genUniqTaskID(vid),
		State:        proto.MigrateStateInited,
		SourceDiskID: dropDiskID,
		SourceIdc:    diskIDC,
		SourceVuid:   src,
	}
	mgr.migrateMgr.AddTask(ctx, &t)
}

func (mgr *DiskDropMgr) acquireDropDisk(ctx context.Context) (*client.DiskInfoSimple, error) {
	// it will retry when break in collectTask,
	// should make sure acquire same disk
	if mgr.dropDisk != nil {
		return mgr.dropDisk, nil
	}

	dropDisks, err := mgr.cmCli.ListDropDisks(ctx)
	if err != nil {
		return nil, err
	}
	if len(dropDisks) == 0 {
		return nil, nil
	}

	mgr.dropDisk = dropDisks[0]
	return mgr.dropDisk, nil
}

func (mgr *DiskDropMgr) checkDroppedAndClearLoop() {
	t := time.NewTicker(time.Duration(mgr.cfg.CheckTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.taskSwitch.WaitEnable()
			mgr.checkDroppedAndClear()
		case <-mgr.closeDone:
			return
		}
	}
}

func (mgr *DiskDropMgr) checkDroppedAndClear() {
	diskID := mgr.getDroppingDiskID()

	span, ctx := trace.StartSpanFromContext(
		context.Background(),
		"DiskDropMgr.checkDroppedAndClear")
	defer span.Finish()

	if !mgr.hasDroppingDisk() {
		return
	}
	span.Infof("check dropped disk_id %d", diskID)
	dropped := mgr.checkDropped(ctx, diskID)
	if dropped {
		err := mgr.cmCli.SetDiskDropped(ctx, diskID)
		if err != nil {
			span.Errorf("set disk dropped fail err:%+v", err)
			return
		}
		interrupt.Inject("drop_clear_tasks_by_diskId")
		span.Infof("diskID %d dropped will start clear...", diskID)
		mgr.clearTasksByDiskID(ctx, diskID)
		mgr.emptyDroppingDiskID()
	}
}

func (mgr *DiskDropMgr) checkDropped(ctx context.Context, diskID proto.DiskID) bool {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("check dropped:check diskId %d drop tasks in db ", diskID)

	tasks, err := mgr.migrateMgr.GetAllTasks(ctx)
	if err != nil {
		span.Errorf("check dropped diskId %d find all tasks fail:%+v", diskID, err)
		return false
	}
	for _, task := range tasks {
		if !task.Finished() {
			return false
		}
	}

	span.Infof("disk_id %d task len %d has finished", diskID, len(tasks))

	vunitInfos, err := mgr.cmCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		span.Errorf("check dropped ListDiskVolumeUnits diskId %s fail err %+v", diskID, err)
		return false
	}
	span.Infof("check dropped: check with clusterMgr disk %d volume units len %d", diskID, len(vunitInfos))
	return len(vunitInfos) == 0
}

func (mgr *DiskDropMgr) clearTasksByDiskID(ctx context.Context, diskID proto.DiskID) {
	mgr.migrateMgr.ClearTasksByDiskIDLoop(ctx, diskID)
}

// AcquireTask acquire disk drop task
func (mgr *DiskDropMgr) AcquireTask(ctx context.Context, idc string) (task *proto.MigrateTask, err error) {
	return mgr.migrateMgr.AcquireTask(ctx, idc)
}

// CancelTask cancel disk drop task
func (mgr *DiskDropMgr) CancelTask(ctx context.Context, args *api.CancelTaskArgs) (err error) {
	return mgr.migrateMgr.CancelTask(ctx, args)
}

// ReclaimTask reclaim disk drop task
func (mgr *DiskDropMgr) ReclaimTask(
	ctx context.Context,
	idc, taskID string,
	src []proto.VunitLocation,
	oldDst proto.VunitLocation,
	newDst *client.AllocVunitInfo) (err error) {
	return mgr.migrateMgr.ReclaimTask(ctx, idc, taskID, src, oldDst, newDst)
}

// RenewalTask renewal disk drop task
func (mgr *DiskDropMgr) RenewalTask(ctx context.Context, idc, taskID string) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("Renewal drop taskID %s", taskID)

	err = mgr.migrateMgr.RenewalTask(ctx, idc, taskID)
	if err != nil {
		span.Warnf("Renewal drop taskID %s fail error:%v", taskID, err)
	}
	return
}

// CompleteTask complete disk drop task
func (mgr *DiskDropMgr) CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	err = mgr.migrateMgr.CompleteTask(ctx, args)
	if err != nil {
		span.Errorf("drop task complete fail err %+v", err)
		return
	}
	return
}

func (mgr *DiskDropMgr) setDroppingDiskID(diskID proto.DiskID) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.droppingDiskID = diskID
}

func (mgr *DiskDropMgr) emptyDroppingDiskID() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.droppingDiskID = base.EmptyDiskID
	mgr.dropDisk = nil
}

func (mgr *DiskDropMgr) getDroppingDiskID() proto.DiskID {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.droppingDiskID
}

func (mgr *DiskDropMgr) hasDroppingDisk() bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.droppingDiskID != base.EmptyDiskID
}

func (mgr *DiskDropMgr) genUniqTaskID(vid proto.Vid) string {
	return base.GenTaskID("disk_drop", vid)
}

// ReportWorkerTaskStats returns disk drop task stats
func (mgr *DiskDropMgr) ReportWorkerTaskStats(
	taskID string,
	s proto.TaskStatistics,
	increaseDataSize,
	increaseShardCnt int) {
	mgr.taskStatsMgr.ReportWorkerTaskStats(taskID, s, increaseDataSize, increaseShardCnt)
}

// StatQueueTaskCnt returns disk drop queue task count
func (mgr *DiskDropMgr) StatQueueTaskCnt() (inited, prepared, completed int) {
	return mgr.migrateMgr.StatQueueTaskCnt()
}

// QueryTask return disk drop task statistics with taskID
func (mgr *DiskDropMgr) QueryTask(ctx context.Context, taskID string) (proto.MigrateTask, proto.TaskStatistics, error) {
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

// GetTaskStats returns disk drop task stats
func (mgr *DiskDropMgr) GetTaskStats() (finish, dataSize, shardCnt [counter.SLOT]int) {
	increaseDataSize, increaseShardCnt := mgr.taskStatsMgr.Counters()
	return mgr.migrateMgr.finishTaskCounter.Show(), increaseDataSize, increaseShardCnt
}

// Progress returns disk drop progress
func (mgr *DiskDropMgr) Progress(ctx context.Context) (dropDiskID proto.DiskID, total, dropped int) {
	span := trace.SpanFromContextSafe(ctx)

	dropDiskID = mgr.getDroppingDiskID()
	if dropDiskID == base.EmptyDiskID {
		return base.EmptyDiskID, 0, 0
	}

	allTasks, err := mgr.migrateMgr.GetAllTasks(ctx)
	if err != nil {
		span.Errorf("find all task fail err %+v", err)
		return dropDiskID, 0, 0
	}
	total = len(allTasks)
	for _, task := range allTasks {
		if task.SourceDiskID == dropDiskID && task.Finished() {
			dropped++
		}
	}
	return dropDiskID, total, dropped
}
