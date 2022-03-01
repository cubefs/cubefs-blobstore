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
	"sync"
	"time"

	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/interrupt"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/blobstore/util/log"
)

// disk repair

const (
	prepareIntervalS = 1
	finishIntervalS  = 5
)

type repairCmCli interface {
	UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error)
	AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (ret *client.AllocVunitInfo, err error)
	ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (ret []*client.VunitInfoSimple, err error)
	GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *client.VolumeInfoSimple, err error)
	ListBrokenDisks(ctx context.Context, count int) (disks []*client.DiskInfoSimple, err error)
	SetDiskRepairing(ctx context.Context, diskID proto.DiskID) (err error)
	SetDiskRepaired(ctx context.Context, diskID proto.DiskID) (err error)
	GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *client.DiskInfoSimple, err error)
}

// RepairMgrCfg repair manager config
type RepairMgrCfg struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	base.TaskCommonConfig
}

// RepairMgr repair task manager
type RepairMgr struct {
	repairingDiskID proto.DiskID // only supports repair one disk at the same time temporarily
	brokenDisk      *client.DiskInfoSimple

	mu sync.Mutex

	taskTbl db.IRepairTaskTbl

	prepareQueue *base.TaskQueue
	workQueue    *base.WorkerTaskQueue
	finishQueue  *base.TaskQueue

	cmCli repairCmCli

	taskSwitch *taskswitch.TaskSwitch

	// for stats
	finishTaskCounter counter.Counter
	taskStatsMgr      *base.TaskStatsMgr

	hasRevised bool
	RepairMgrCfg

	closeOnce *sync.Once
	closeDone chan struct{}
}

// NewRepairMgr returns repair manager
func NewRepairMgr(cfg *RepairMgrCfg, switchMgr *taskswitch.SwitchMgr, taskTbl db.IRepairTaskTbl, cmCli repairCmCli) (*RepairMgr, error) {
	log.Infof("repair Cfg %+v", cfg)
	ts, err := switchMgr.AddSwitch(taskswitch.DiskRepairSwitchName)
	if err != nil {
		return nil, err
	}

	mgr := &RepairMgr{
		taskTbl:      taskTbl,
		prepareQueue: base.NewTaskQueue(time.Duration(cfg.PrepareQueueRetryDelayS) * time.Second),
		workQueue:    base.NewWorkerTaskQueue(time.Duration(cfg.CancelPunishDurationS) * time.Second),
		finishQueue:  base.NewTaskQueue(time.Duration(cfg.FinishQueueRetryDelayS) * time.Second),

		cmCli:        cmCli,
		taskSwitch:   ts,
		RepairMgrCfg: *cfg,

		hasRevised: false,

		closeOnce: &sync.Once{},
		closeDone: make(chan struct{}),
	}
	mgr.taskStatsMgr = base.NewTaskStatsMgrAndRun(cfg.ClusterID, proto.RepairTaskType, mgr)
	return mgr, nil
}

// Load load repair task from database
func (mgr *RepairMgr) Load() error {
	log.Infof("RepairMgr start load...")
	ctx := context.Background()

	tasks, err := mgr.taskTbl.FindAll(ctx)
	if err != nil {
		panic("load repair task fail " + err.Error())
	}
	log.Infof("repair load tasks len %d", len(tasks))

	if len(tasks) == 0 {
		return nil
	}

	repairingDiskID := tasks[0].RepairDiskID
	mgr.setRepairingDiskID(repairingDiskID)

	for _, t := range tasks {
		if t.RepairDiskID != repairingDiskID {
			panic("can not allow many disk repairing")
		}

		if t.Running() {
			err = VolTaskLockerInst().TryLock(ctx, t.Vid())
			if err != nil {
				log.Panicf("repair task conflict,task:%+v,err:%+v",
					t, err.Error())
			}
		}

		log.Infof("load task taskId %s state %d", t.TaskID, t.State)
		switch t.State {
		case proto.RepairStateInited:
			mgr.prepareQueue.PushTask(t.TaskID, t)
		case proto.RepairStatePrepared:
			mgr.workQueue.AddPreparedTask(t.BrokenDiskIDC, t.TaskID, t)
		case proto.RepairStateWorkCompleted:
			mgr.finishQueue.PushTask(t.TaskID, t)
		case proto.RepairStateFinished, proto.RepairStateFinishedInAdvance:
			continue
		default:
			panic("unexpect repair state")
		}
	}

	return nil
}

// Run run repair task includes collect/prepare/finish/check phase
func (mgr *RepairMgr) Run() {
	go mgr.collectTaskLoop()
	go mgr.prepareTaskLoop()
	go mgr.finishTaskLoop()
	go mgr.checkRepairedAndClearLoop()
}

// Close close repair task manager
func (mgr *RepairMgr) Close() {
	mgr.closeOnce.Do(func() {
		close(mgr.closeDone)
	})
}

func (mgr *RepairMgr) collectTaskLoop() {
	t := time.NewTicker(time.Duration(mgr.CollectTaskIntervalS) * time.Second)
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

func (mgr *RepairMgr) collectTask() {
	span, ctx := trace.StartSpanFromContext(
		context.Background(),
		"RepairMgr.collectTask")
	defer span.Finish()

	// revise repair tasks to make sure data consistency when services start
	if !mgr.hasRevised && mgr.hasRepairingDisk() {
		span.Infof("first collect task will revise repair task")
		err := mgr.reviseRepairTask(ctx)
		if err == nil {
			span.Infof("firstCollectTask finished!")
			mgr.hasRevised = true
		}
		return
	}

	if mgr.hasRepairingDisk() {
		span.Infof("disk_id %d is repairing,skip collect task...", mgr.getRepairingDiskID())
		return
	}

	span.Infof("CollectTask start")
	brokenDisk, err := mgr.acquireBrokenDisk(ctx)
	if err != nil {
		span.Errorf("acquire broken disk fail err %+v", err)
		return
	}
	if brokenDisk == nil {
		return
	}

	err = mgr.genDiskRepairTasks(ctx, brokenDisk.DiskID, brokenDisk.Idc)
	if err != nil {
		span.Errorf("initBrokenDiskRepairTask fail err %+v", err)
		return
	}

	interrupt.Inject("repair_collect_task")

	base.LoopExecUntilSuccess(ctx, "set disk diskId %d repairing fail", func() error {
		return mgr.cmCli.SetDiskRepairing(ctx, brokenDisk.DiskID)
	})

	mgr.setRepairingDiskID(brokenDisk.DiskID)
}

func (mgr *RepairMgr) reviseRepairTask(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)

	diskInfo, err := mgr.cmCli.GetDiskInfo(ctx, mgr.getRepairingDiskID())
	if err != nil {
		span.Errorf("cmCli.GetDiskInfo fail %+v", err)
		return err
	}
	span.Infof("reviseRepairTask GetDiskInfo %+v", diskInfo)

	if diskInfo.IsBroken() {
		err = mgr.genDiskRepairTasks(ctx, mgr.getRepairingDiskID(), diskInfo.Idc)
		if err != nil {
			span.Errorf("gen disk repair tasks fail err %+v", err)
			return err
		}

		execMsg := fmt.Sprintf("set disk diskId %d repairing", mgr.getRepairingDiskID())
		base.LoopExecUntilSuccess(ctx, execMsg, func() error {
			return mgr.cmCli.SetDiskRepairing(ctx, mgr.getRepairingDiskID())
		})

	}
	return nil
}

func (mgr *RepairMgr) genDiskRepairTasks(ctx context.Context, diskID proto.DiskID, diskIdc string) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start genDiskRepairTasks disk_id %d disk_idc %s", diskID, diskIdc)

	vuidsDb, err := mgr.badVuidsFromDb(ctx, diskID)
	if err != nil {
		span.Errorf("get bad vuids from db fail %+v", err)
		return err
	}
	span.Infof("genDiskRepairTasks badVuidsFromDb len %d", len(vuidsDb))

	vuidsCm, err := mgr.badVuidsFromCm(ctx, diskID)
	if err != nil {
		span.Errorf("get bad vuid from cm fail %+v", err)
		return err
	}
	span.Infof("genDiskRepairTasks badVuidFromCm len %d", len(vuidsCm))

	remain := base.Subtraction(vuidsCm, vuidsDb)
	span.Infof("should gen tasks remain len %d", len(remain))
	for _, vuid := range remain {
		mgr.initOneTask(ctx, vuid, diskID, diskIdc)
		span.Infof("init repair task vuid %d success", vuid)
		interrupt.Inject("repair_init_one_task")
	}
	return nil
}

func (mgr *RepairMgr) badVuidsFromDb(ctx context.Context, diskID proto.DiskID) (bads []proto.Vuid, err error) {
	tasks, err := mgr.taskTbl.FindByDiskID(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, t := range tasks {
		bads = append(bads, t.RepairVuid())
	}
	return bads, nil
}

func (mgr *RepairMgr) badVuidsFromCm(ctx context.Context, diskID proto.DiskID) (bads []proto.Vuid, err error) {
	vunits, err := mgr.cmCli.ListDiskVolumeUnits(ctx, diskID)
	if err != nil {
		return nil, err
	}

	for _, vunit := range vunits {
		bads = append(bads, vunit.Vuid)
	}
	return bads, nil
}

func (mgr *RepairMgr) initOneTask(ctx context.Context, badVuid proto.Vuid, brokenDiskID proto.DiskID, brokenDiskIdc string) {
	span := trace.SpanFromContextSafe(ctx)

	vid := badVuid.Vid()
	t := proto.VolRepairTask{
		TaskID:       mgr.genUniqTaskID(vid),
		State:        proto.RepairStateInited,
		RepairDiskID: brokenDiskID,

		BadVuid: badVuid,
		BadIdx:  badVuid.Index(),

		BrokenDiskIDC: brokenDiskIdc,
		TriggerBy:     proto.BrokenDiskTrigger,
	}
	base.LoopExecUntilSuccess(ctx, "repair init one task insert task to tbl", func() error {
		return mgr.taskTbl.Insert(ctx, &t)
	})

	mgr.prepareQueue.PushTask(t.TaskID, &t)
	span.Infof("init repair task success %+v", t)
}

func (mgr *RepairMgr) genUniqTaskID(vid proto.Vid) string {
	return base.GenTaskID("repair", vid)
}

func (mgr *RepairMgr) acquireBrokenDisk(ctx context.Context) (*client.DiskInfoSimple, error) {
	// can not assume request cm to acquire broken disk is the same disk
	// because break in generate tasks(eg. generate task return an error),
	// and reentry(not because of starting of service) need the same disk
	// cache last broken disk acquired from cm
	if mgr.brokenDisk != nil {
		return mgr.brokenDisk, nil
	}

	brokenDisks, err := mgr.cmCli.ListBrokenDisks(ctx, 1)
	if err != nil {
		return nil, err
	}
	if len(brokenDisks) == 0 {
		return nil, nil
	}

	mgr.brokenDisk = brokenDisks[0]
	return mgr.brokenDisk, nil
}

func (mgr *RepairMgr) prepareTaskLoop() {
	for {
		mgr.taskSwitch.WaitEnable()
		todo, doing := mgr.workQueue.StatsTasks()
		if !mgr.hasRepairingDisk() || todo+doing >= mgr.WorkQueueSize {
			time.Sleep(1 * time.Second)
			continue
		}

		err := mgr.popTaskAndPrepare()
		if err == base.ErrNoTaskInQueue {
			time.Sleep(time.Duration(prepareIntervalS) * time.Second)
		}
	}
}

func (mgr *RepairMgr) popTaskAndPrepare() error {
	_, task, exist := mgr.prepareQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	var err error
	span, ctx := trace.StartSpanFromContext(
		context.Background(),
		"RepairMgr.popTaskAndPrepare")
	defer span.Finish()

	defer func() {
		if err != nil {
			span.Errorf("prepare task %s fail %+v and retry task", task.(*proto.VolRepairTask).TaskID, err)
			mgr.prepareQueue.RetryTask(task.(*proto.VolRepairTask).TaskID)
		}
	}()

	//why:avoid to change task in queue
	t := task.(*proto.VolRepairTask).Copy()
	span.Infof("pop taskId %s task %+v", t.TaskID, t)
	// whether vid has another running task
	err = VolTaskLockerInst().TryLock(ctx, t.Vid())
	if err != nil {
		span.Warnf("TryLock fail vid %d has task running", t.Vid())
		return base.ErrVolNotOnlyOneTask
	}
	defer func() {
		if err != nil {
			span.Errorf("prepare task taskId %s fail %+v and unlock VolTaskLock", t.TaskID, err)
			VolTaskLockerInst().Unlock(ctx, t.Vid())
		}
	}()

	err = mgr.prepareTask(t)
	if err != nil {
		span.Errorf("prepare task_id %s fail err %v", t.TaskID, err)
		return err
	}

	span.Infof("prepare task_id %s success", t.TaskID)
	return nil
}

func (mgr *RepairMgr) prepareTask(t *proto.VolRepairTask) error {
	span, ctx := trace.StartSpanFromContext(
		context.Background(),
		"RepairMgr.prepareTask")
	defer span.Finish()

	span.Infof("start prepare repair taskId %s task %+v", t.TaskID, t)

	volInfo, err := mgr.cmCli.GetVolumeInfo(ctx, t.Vid())
	if err != nil {
		span.Errorf("prepare task get volume info fail err:%+v", err)
		return err
	}

	// 1.check necessity of generating current task
	badVuid := t.RepairVuid()
	if volInfo.VunitLocations[t.BadIdx].Vuid != badVuid {
		span.Infof("repair task %s finish in advance", t.TaskID)
		mgr.finishTaskInAdvance(ctx, t)
		return nil
	}

	// 2.generate src and destination for task & task persist
	allocDstVunit, err := base.AllocVunitSafe(ctx, mgr.cmCli, badVuid, t.Sources)
	if err != nil {
		span.Errorf("repair AllocVolumeUnit fail %+v", err)
		return err
	}

	t.CodeMode = volInfo.CodeMode
	t.Sources = volInfo.VunitLocations
	t.Destination = allocDstVunit.Location()
	t.State = proto.RepairStatePrepared
	base.LoopExecUntilSuccess(ctx, "repair prepare task update task tbl", func() error {
		return mgr.taskTbl.Update(ctx, t)
	})

	mgr.sendToWorkQueue(t)
	return nil
}

func (mgr *RepairMgr) sendToWorkQueue(t *proto.VolRepairTask) {
	mgr.workQueue.AddPreparedTask(t.BrokenDiskIDC, t.TaskID, t)
	mgr.prepareQueue.RemoveTask(t.TaskID)
}

func (mgr *RepairMgr) finishTaskInAdvance(ctx context.Context, t *proto.VolRepairTask) {
	t.State = proto.RepairStateFinishedInAdvance
	base.LoopExecUntilSuccess(ctx, "repair finish task in advance update task tbl", func() error {
		return mgr.taskTbl.Update(ctx, t)
	})

	mgr.finishTaskCounter.Add()
	mgr.prepareQueue.RemoveTask(t.TaskID)
	VolTaskLockerInst().Unlock(ctx, t.Vid())
}

func (mgr *RepairMgr) finishTaskLoop() {
	for {
		mgr.taskSwitch.WaitEnable()
		err := mgr.popTaskAndFinish()
		if err == base.ErrNoTaskInQueue {
			time.Sleep(time.Duration(finishIntervalS) * time.Second)
		}
	}
}

func (mgr *RepairMgr) popTaskAndFinish() error {
	_, task, exist := mgr.finishQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	span, ctx := trace.StartSpanFromContext(
		context.Background(),
		"RepairMgr.popTaskAndFinish")
	defer span.Finish()

	t := task.(*proto.VolRepairTask).Copy()
	err := mgr.finishTask(ctx, t)
	if err != nil {
		span.Errorf("finish task fail err %+v", err)
		return err
	}

	span.Infof("finish task_id %s success", t.TaskID)
	return nil
}

func (mgr *RepairMgr) finishTask(ctx context.Context, task *proto.VolRepairTask) (retErr error) {
	span := trace.SpanFromContextSafe(ctx)

	defer func() {
		if retErr != nil {
			mgr.finishQueue.RetryTask(task.TaskID)
		}
	}()

	if task.State != proto.RepairStateWorkCompleted {
		span.Panicf("taskId %s finish state expect %d but actual %d", proto.RepairStateWorkCompleted, task.State)
	}
	// complete stage can not make sure to save task info to db,
	// finish stage make sure to save task info to db
	// execute update volume mapping relation when can not save task with completed state is dangerous
	// because if process restart will reload task and redo by worker
	// worker will write data to chunk which is online
	interrupt.Inject("repair_save_completed")
	base.LoopExecUntilSuccess(ctx, "repair finish task update task state completed", func() error {
		return mgr.taskTbl.Update(ctx, task)
	})

	newVuid := task.Destination.Vuid
	oldVuid := task.RepairVuid()
	err := mgr.cmCli.UpdateVolume(ctx, newVuid, oldVuid, task.NewDiskId())
	if err != nil {
		span.Errorf("UpdateVolume fail:%+v", err)
		return mgr.handleUpdateVolMappingFail(ctx, task, err)
	}

	task.State = proto.RepairStateFinished
	interrupt.Inject("repair_save_finished")
	base.LoopExecUntilSuccess(ctx, "repair finish task update task state finished", func() error {
		return mgr.taskTbl.Update(ctx, task)
	})

	mgr.finishTaskCounter.Add()
	// 1.remove task in memory
	// 2.release lock of volume task
	mgr.finishQueue.RemoveTask(task.TaskID)
	VolTaskLockerInst().Unlock(ctx, task.Vid())

	return nil
}

func (mgr *RepairMgr) handleUpdateVolMappingFail(ctx context.Context, task *proto.VolRepairTask, err error) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("handle update vol mapping fail taskId %s state %d dest vuid %d", task.TaskID, task.State, task.Destination.Vuid)

	code := rpc.DetectStatusCode(err)
	if code == errors.CodeOldVuidNotMatch {
		span.Panicf("change volume unit relationship got unexpected err")
	}

	if base.ShouldAllocAndRedo(code) {
		span.Infof("re-alloc vunit and redo task_id %s", task.TaskID)

		newVunit, err := base.AllocVunitSafe(ctx, mgr.cmCli, task.BadVuid, task.Sources)
		if err != nil {
			span.Errorf("re-alloc failed vuid %d err:%v", task.BadVuid, err)
			return err
		}
		task.SetDest(newVunit.Location())
		task.State = proto.RepairStatePrepared
		task.WorkerRedoCnt++

		base.LoopExecUntilSuccess(ctx, "repair redo task update task tbl", func() error {
			return mgr.taskTbl.Update(ctx, task)
		})

		mgr.finishQueue.RemoveTask(task.TaskID)
		mgr.workQueue.AddPreparedTask(task.BrokenDiskIDC, task.TaskID, task)
		span.Infof("task %+v redo again", task)
		return nil
	}

	return err
}

func (mgr *RepairMgr) checkRepairedAndClearLoop() {
	t := time.NewTicker(time.Duration(mgr.CheckTaskIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.taskSwitch.WaitEnable()
			mgr.checkRepairedAndClear()
		case <-mgr.closeDone:
			return
		}
	}
}

func (mgr *RepairMgr) checkRepairedAndClear() {
	diskID := mgr.getRepairingDiskID()
	span, ctx := trace.StartSpanFromContext(
		context.Background(),
		"RepairMgr.checkRepairedAndClear")
	defer span.Finish()

	if !mgr.hasRepairingDisk() {
		return
	}

	span.Infof("check repair disk_id %d", diskID)
	repaired := mgr.checkRepaired(ctx, diskID)
	if repaired {
		err := mgr.cmCli.SetDiskRepaired(ctx, diskID)
		if err != nil {
			return
		}
		interrupt.Inject("repair_clear_tasks_by_diskId")
		span.Infof("diskID %d repaired will start clear...", diskID)
		mgr.clearTasksByDiskID(diskID)
		mgr.emptyRepairingDiskID()
	}
}

func (mgr *RepairMgr) checkRepaired(ctx context.Context, diskID proto.DiskID) bool {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("check repaired diskID %d repair tasks in db ", diskID)

	tasks, err := mgr.taskTbl.FindAll(ctx)
	if err != nil {
		span.Errorf("check repaired diskID %d find all tasks fail:%+v", diskID, err)
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
		span.Errorf("check repaired ListDiskVolumeUnits diskID %s fail err %+v", diskID, err)
		return false
	}
	span.Infof("check repaired: check with clusterMgr disk %d volume units len %d", diskID, len(vunitInfos))
	return len(vunitInfos) == 0
}

func (mgr *RepairMgr) clearTasksByDiskID(diskID proto.DiskID) {
	span, ctx := trace.StartSpanFromContext(context.Background(),
		"RepairMgr.clearTasksByDiskID")
	defer span.Finish()

	base.LoopExecUntilSuccess(ctx, "repair clear task by diskID", func() error {
		return mgr.taskTbl.MarkDeleteByDiskID(ctx, diskID)
	})
}

func (mgr *RepairMgr) setRepairingDiskID(diskID proto.DiskID) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.repairingDiskID = diskID
	mgr.brokenDisk = nil
}

func (mgr *RepairMgr) emptyRepairingDiskID() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.repairingDiskID = base.EmptyDiskID
}

func (mgr *RepairMgr) getRepairingDiskID() proto.DiskID {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.repairingDiskID
}

func (mgr *RepairMgr) hasRepairingDisk() bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.repairingDiskID != base.EmptyDiskID
}

// AcquireTask acquire repair task
func (mgr *RepairMgr) AcquireTask(ctx context.Context, idc string) (*proto.VolRepairTask, error) {
	if !mgr.taskSwitch.Enabled() {
		return nil, proto.ErrTaskPaused
	}

	_, task, _ := mgr.workQueue.Acquire(idc)
	if task != nil {
		t := task.(*proto.VolRepairTask)
		span := trace.SpanFromContextSafe(ctx)
		span.Infof("acquire Repair task %+v", t)
		return t, nil
	}
	return nil, proto.ErrTaskEmpty
}

// CancelTask cancel repair task
func (mgr *RepairMgr) CancelTask(ctx context.Context, args *api.CancelTaskArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("cancel repair taskId %s", args.TaskId)

	err := mgr.workQueue.Cancel(args.IDC, args.TaskId, args.Src, args.Dest)
	if err != nil {
		span.Errorf("cancel repair taskId %s fail error:%v", args.TaskId, err)
	}

	mgr.taskStatsMgr.CancelTask()

	return err
}

// ReclaimTask reclaim repair task
func (mgr *RepairMgr) ReclaimTask(ctx context.Context,
	idc, taskID string,
	src []proto.VunitLocation,
	oldDst proto.VunitLocation,
	newDst *client.AllocVunitInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("reclaim repair taskID %s", taskID)

	err := mgr.workQueue.Reclaim(idc, taskID, src, oldDst, newDst.Location(), newDst.DiskID)
	if err != nil {
		// task has finished,because only complete will remove task from queue
		span.Errorf("repair taskID %s reclaim fail err %+v", taskID, err)
		return err
	}

	task, err := mgr.workQueue.Query(idc, taskID)
	if err != nil {
		span.Errorf("found task in workQueue failed, idc: %s,taskID: %s,err:%v", idc, taskID, err)
		return err
	}

	err = mgr.taskTbl.Update(ctx, task.(*proto.VolRepairTask))
	if err != nil {
		span.Warnf("update reclaim task failed, taskID:%s,err:%v", taskID, err)
	}

	mgr.taskStatsMgr.ReclaimTask()
	return nil
}

// CompleteTask complete repair task
func (mgr *RepairMgr) CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("complete repair taskId %s", args.TaskId)

	completeTask, err := mgr.workQueue.Complete(args.IDC, args.TaskId, args.Src, args.Dest)
	if err != nil {
		span.Errorf("complete repair taskId %s fail error:%v", args.TaskId, err)
		return err
	}

	t := completeTask.(*proto.VolRepairTask)
	t.State = proto.RepairStateWorkCompleted

	mgr.finishQueue.PushTask(args.TaskId, t)
	// as complete func is face to svr api, so can not loop save task
	// to db until success, it will make saving task info to be difficult,
	// that delay saving task info in finish stage is a simply way
	return nil
}

// RenewalTask renewal repair task
func (mgr *RepairMgr) RenewalTask(ctx context.Context, idc, taskID string) error {
	if !mgr.taskSwitch.Enabled() {
		// renewal task stopping will touch off worker to stop task
		return proto.ErrTaskPaused
	}

	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("Renewal repair taskID %s", taskID)
	err := mgr.workQueue.Renewal(idc, taskID)
	if err != nil {
		span.Warnf("Renewal repair taskID %s fail error:%v", taskID, err)
	}

	return err
}

// ReportWorkerTaskStats reports task stats
func (mgr *RepairMgr) ReportWorkerTaskStats(
	taskID string,
	s proto.TaskStatistics,
	increaseDataSize,
	increaseShardCnt int) {
	mgr.taskStatsMgr.ReportWorkerTaskStats(taskID, s, increaseDataSize, increaseShardCnt)
}

// QueryTask return task statistics
func (mgr *RepairMgr) QueryTask(ctx context.Context, taskID string) (proto.VolRepairTask, proto.TaskStatistics, error) {
	taskInfo, err := mgr.taskTbl.Find(ctx, taskID)
	if err != nil {
		return proto.VolRepairTask{}, proto.TaskStatistics{}, err
	}
	detailRunInfo, err := mgr.taskStatsMgr.QueryTaskDetail(taskID)
	if err != nil {
		return *taskInfo, proto.TaskStatistics{}, nil
	}
	return *taskInfo, detailRunInfo.Statistics, nil
}

// GetTaskStats returns task stats
func (mgr *RepairMgr) GetTaskStats() (finish, dataSize, shardCnt [counter.SLOT]int) {
	increaseDataSize, increaseShardCnt := mgr.taskStatsMgr.Counters()
	return mgr.finishTaskCounter.Show(), increaseDataSize, increaseShardCnt
}

// StatQueueTaskCnt returns task queue stats
func (mgr *RepairMgr) StatQueueTaskCnt() (inited, prepared, completed int) {
	todo, doing := mgr.prepareQueue.StatsTasks()
	inited = todo + doing

	todo, doing = mgr.workQueue.StatsTasks()
	prepared = todo + doing

	todo, doing = mgr.finishQueue.StatsTasks()
	completed = todo + doing

	return
}

// Progress repair manager progress
func (mgr *RepairMgr) Progress(ctx context.Context) (repairingDiskID proto.DiskID, total, repaired int) {
	span := trace.SpanFromContextSafe(ctx)
	repairingDiskID = mgr.getRepairingDiskID()
	if repairingDiskID == base.EmptyDiskID {
		return base.EmptyDiskID, 0, 0
	}

	allTasks, err := mgr.taskTbl.FindAll(ctx)
	if err != nil {
		span.Errorf("find all task fail err %+v", err)
		return repairingDiskID, 0, 0
	}
	total = len(allTasks)
	for _, task := range allTasks {
		if task.RepairDiskID == repairingDiskID && task.Finished() {
			repaired++
		}
	}

	return repairingDiskID, total, repaired
}
