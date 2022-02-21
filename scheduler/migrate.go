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

const (
	prepareMigrateTaskIntervalS = 1
	finishMigrateTaskIntervalS  = 1
	prepareTaskPauseS           = 2
)

// IMigrateCmCli define the interface of clustermgr used by migrate
type IMigrateCmCli interface {
	GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *client.VolumeInfoSimple, err error)
	LockVolume(ctx context.Context, Vid proto.Vid) (err error)
	UnlockVolume(ctx context.Context, Vid proto.Vid) (err error)
	UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error)
	AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (ret *client.AllocVunitInfo, err error)
	ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID) (err error)
	ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (ret []*client.VunitInfoSimple, err error)
}

// ITinkerCli define the interface of tinker used by migrate
type ITinkerCli interface {
	UpdateVol(ctx context.Context, host string, vid proto.Vid, clusterID proto.ClusterID) (err error)
}

// MigratingVuids record migrating vuid info
type MigratingVuids map[proto.Vuid]string

type diskMigratingVuids struct {
	vuids map[proto.DiskID]MigratingVuids
	lock  sync.RWMutex
}

func newDiskMigratingVuids() *diskMigratingVuids {
	return &diskMigratingVuids{
		vuids: make(map[proto.DiskID]MigratingVuids),
	}
}

func (m *diskMigratingVuids) addMigratingVuid(diskID proto.DiskID, vuid proto.Vuid, taskID string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.vuids[diskID] == nil {
		m.vuids[diskID] = make(MigratingVuids)
	}
	m.vuids[diskID][vuid] = taskID
}

func (m *diskMigratingVuids) deleteMigratingVuid(diskID proto.DiskID, vuid proto.Vuid) {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.vuids[diskID], vuid)
	if len(m.vuids[diskID]) == 0 {
		delete(m.vuids, diskID)
	}
}

func (m *diskMigratingVuids) getCurrMigratingDisksCnt() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.vuids)
}

func (m *diskMigratingVuids) isMigratingDisk(diskID proto.DiskID) (ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	_, ok = m.vuids[diskID]
	return
}

// MigrateConfig migrate config
type MigrateConfig struct {
	ClusterID proto.ClusterID
	base.TaskCommonConfig
}

// MigrateMgr migrate manager
type MigrateMgr struct {
	taskType           string
	diskMigratingVuids *diskMigratingVuids

	svrTbl  db.ISvrRegisterTbl
	taskTbl db.IMigrateTaskTbl

	clusterMgrClient IMigrateCmCli
	tinkerClient     ITinkerCli

	taskSwitch *taskswitch.TaskSwitch

	prepareQueue *base.TaskQueue       // store inited task
	workQueue    *base.WorkerTaskQueue // store prepared task
	finishQueue  *base.TaskQueue       // store completed task

	finishTaskCounter counter.CounterByMin

	*MigrateConfig

	// handle func when lock volume fail
	lockFailHandleFunc func(ctx context.Context, task *proto.MigrateTask)
}

// NewMigrateMgr returns migrate manager
func NewMigrateMgr(
	cmCli IMigrateCmCli,
	tinkerCli ITinkerCli,
	taskSwitch *taskswitch.TaskSwitch,
	svrTbl db.ISvrRegisterTbl,
	taskTbl db.IMigrateTaskTbl,
	conf *MigrateConfig,
	taskType string,
) *MigrateMgr {
	return &MigrateMgr{
		taskType:           taskType,
		diskMigratingVuids: newDiskMigratingVuids(),

		svrTbl:  svrTbl,
		taskTbl: taskTbl,

		taskSwitch: taskSwitch,

		clusterMgrClient: cmCli,
		tinkerClient:     tinkerCli,

		prepareQueue: base.NewTaskQueue(time.Duration(conf.PrepareQueueRetryDelayS) * time.Second),
		workQueue:    base.NewWorkerTaskQueue(time.Duration(conf.CancelPunishDurationS) * time.Second),
		finishQueue:  base.NewTaskQueue(time.Duration(conf.FinishQueueRetryDelayS) * time.Second),

		MigrateConfig: conf,
	}
}

// SetLockFailHandleFunc set lock failed func
func (mgr *MigrateMgr) SetLockFailHandleFunc(lockFailHandleFunc func(ctx context.Context, task *proto.MigrateTask)) {
	mgr.lockFailHandleFunc = lockFailHandleFunc
}

// Load load migrate task from databse
func (mgr *MigrateMgr) Load() (err error) {
	log.Infof("MigrateMgr task_type %s start load...", mgr.taskType)
	ctx := context.Background()

	// load task from db
	tasks, err := mgr.taskTbl.FindAll(ctx)
	if err != nil {
		log.Errorf("find all tasks failed, err:%v", err)
		return
	}
	log.Infof("load task_type %s tasks len %d", mgr.taskType, len(tasks))

	for i := range tasks {
		if tasks[i].Running() {
			err = VolTaskLockerInst().TryLock(ctx, tasks[i].SourceVuid.Vid())
			if err != nil {
				log.Panicf("migrate task conflict,vid %d task:%+v,err:%+v",
					tasks[i].SourceVuid.Vid(), tasks[i], err.Error())
			}
		}

		if !tasks[i].Finished() {
			mgr.diskMigratingVuids.addMigratingVuid(tasks[i].SourceDiskID, tasks[i].SourceVuid, tasks[i].TaskID)
		}

		log.Infof("load %s task add prepareQueue taskId %s state %d", mgr.taskType, tasks[i].TaskID, tasks[i].State)
		switch tasks[i].State {
		case proto.MigrateStateInited:
			mgr.prepareQueue.PushTask(tasks[i].TaskID, tasks[i])
		case proto.MigrateStatePrepared:
			mgr.workQueue.AddPreparedTask(tasks[i].SourceIdc, tasks[i].TaskID, tasks[i])
		case proto.MigrateStateWorkCompleted:
			mgr.finishQueue.PushTask(tasks[i].TaskID, tasks[i])
		case proto.MigrateStateFinished, proto.MigrateStateFinishedInAdvance:
			continue
		default:
			log.Panicf("unexpect migrate state,task:%+v", tasks[i])
		}
	}
	return
}

// Run run migrate task do prepare and finish task phase
func (mgr *MigrateMgr) Run() {
	go mgr.prepareTaskLoop()
	go mgr.finishTaskLoop()
}

func (mgr *MigrateMgr) prepareTaskLoop() {
	for {
		mgr.taskSwitch.WaitEnable()
		todo, doing := mgr.workQueue.StatsTasks()
		if todo+doing >= mgr.WorkQueueSize {
			time.Sleep(time.Duration(prepareTaskPauseS) * time.Second)
			continue
		}
		err := mgr.prepareTask()
		if err == base.ErrNoTaskInQueue {
			log.Debugf("no task in prepare queue, sleep %d second", prepareMigrateTaskIntervalS)
			time.Sleep(time.Duration(prepareMigrateTaskIntervalS) * time.Second)
		}
	}
}

func (mgr *MigrateMgr) prepareTask() (err error) {
	_, task, exist := mgr.prepareQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "MigrateMgr.prepareTask")
	defer span.Finish()

	defer func() {
		if err != nil {
			mgr.prepareQueue.RetryTask(task.(*proto.MigrateTask).TaskID)
		}
	}()

	migTask := task.(*proto.MigrateTask).Copy()

	span.Infof("prepare task phase, taskId: %s, state: %v", migTask.TaskID, migTask.State)

	err = VolTaskLockerInst().TryLock(ctx, migTask.SourceVuid.Vid())
	if err != nil {
		span.Warnf("lock volume failed, volumeId:%v,err:%v", migTask.SourceVuid.Vid(), err)
		return base.ErrVolNotOnlyOneTask
	}
	defer func() {
		if err != nil {
			VolTaskLockerInst().Unlock(ctx, task.(*proto.MigrateTask).SourceVuid.Vid())
		}
	}()

	volInfo, err := mgr.clusterMgrClient.GetVolumeInfo(ctx, migTask.SourceVuid.Vid())
	if err != nil {
		span.Errorf("prepare task failed, err:%v", err)
		return err
	}

	// check necessity of generating current task
	if migTask.SourceVuid != volInfo.VunitLocations[migTask.SourceVuid.Index()].Vuid {
		span.Infof("the source unit has been moved and finish task immediately,taskId: %s, task source vuid: %v, current vuid %v",
			migTask.TaskID, migTask.SourceVuid, volInfo.VunitLocations[migTask.SourceVuid.Index()].Vuid)

		// volume may be locked, try unlock the volume
		// for example
		// 1. lock volume success
		// 2. alloc chunk failed and VolTaskLockerInst().Unlock
		// 3. this volume maybe execute other tasks, such as disk repair
		// 4. then enter this branch and volume status is locked
		err := mgr.clusterMgrClient.UnlockVolume(ctx, migTask.SourceVuid.Vid())
		if err != nil {
			span.Errorf("before finish in advance try unlock volume failed, vid: %d, err: %v",
				migTask.SourceVuid.Vid(), err)
			return err
		}

		mgr.finishTaskInAdvance(ctx, migTask, "volume has migrated")
		return nil
	}

	// lock volume
	err = mgr.clusterMgrClient.LockVolume(ctx, migTask.SourceVuid.Vid())
	if err != nil {
		if rpc.DetectStatusCode(err) == errors.CodeLockNotAllow && mgr.lockFailHandleFunc != nil {
			mgr.lockFailHandleFunc(ctx, migTask)
			return nil
		}
		span.Errorf("lock volume failed, volumeId:%v,err:%v", migTask.SourceVuid.Vid(), err)
		return err
	}

	// alloc volume unit
	ret, err := base.AllocVunitSafe(ctx, mgr.clusterMgrClient, migTask.SourceVuid, migTask.Sources)
	if err != nil {
		span.Errorf("alloc volume unit failed, err:%v", err)
		return
	}

	migTask.CodeMode = volInfo.CodeMode
	migTask.Sources = volInfo.VunitLocations
	migTask.SetDest(ret.Location())
	migTask.State = proto.MigrateStatePrepared

	interrupt.Inject(mgr.taskType + "_prepare_task")

	// update db
	base.LoopExecUntilSuccess(ctx, "migrate prepare task update task tbl", func() error {
		return mgr.taskTbl.Update(ctx, proto.MigrateStateInited, migTask)
	})

	// send task to worker queue and remove task in prepareQueue
	mgr.workQueue.AddPreparedTask(migTask.SourceIdc, migTask.TaskID, migTask)
	mgr.prepareQueue.RemoveTask(migTask.TaskID)

	span.Infof("prepare task success, taskId: %s, state: %v", migTask.TaskID, migTask.State)
	return
}

func (mgr *MigrateMgr) finishTaskLoop() {
	for {
		mgr.taskSwitch.WaitEnable()
		err := mgr.finishTask()
		if err == base.ErrNoTaskInQueue {
			log.Debugf("no task in finish queue, sleep %d second", finishMigrateTaskIntervalS)
			time.Sleep(time.Duration(finishMigrateTaskIntervalS) * time.Second)
		}
	}
}

func (mgr *MigrateMgr) finishTask() (err error) {
	_, task, exist := mgr.finishQueue.PopTask()
	if !exist {
		return base.ErrNoTaskInQueue
	}

	span, ctx := trace.StartSpanFromContext(context.Background(), "MigrateMgr.finishTask")
	defer span.Finish()

	defer func() {
		if err != nil {
			mgr.finishQueue.RetryTask(task.(*proto.MigrateTask).TaskID)
		}
	}()

	migTask := task.(*proto.MigrateTask).Copy()
	span.Infof("finish task phase, taskId: %s, state: %v", migTask.TaskID, migTask.State)

	if migTask.State != proto.MigrateStateWorkCompleted {
		span.Panicf("taskId %s finish state expect %d but actual %d", proto.MigrateStateWorkCompleted, migTask.State)
	}

	// because competed task did not persisted to the database, so in finish phase need to do it
	// the task maybe update more than once, which is allowed
	base.LoopExecUntilSuccess(ctx, "migrate finish task update task tbl to state completed ", func() error {
		return mgr.taskTbl.Update(ctx, proto.MigrateStatePrepared, migTask)
	})

	// update volume mapping relationship
	err = mgr.clusterMgrClient.UpdateVolume(
		ctx,
		migTask.Destination.Vuid,
		migTask.SourceVuid,
		migTask.DestinationDiskId())
	if err != nil {
		span.Errorf("change volume unit relationship failed, old vuid:%d, new vuid:%d, new diskId: %d,err:%v",
			migTask.SourceVuid,
			migTask.Destination.Vuid,
			migTask.DestinationDiskId(),
			err)
		return mgr.handleUpdateVolMappingFail(ctx, migTask, err)
	}

	err = mgr.clusterMgrClient.ReleaseVolumeUnit(ctx, migTask.SourceVuid, migTask.SourceDiskID)
	if err != nil {
		span.Errorf("release volume unit failed,err:%v", err)
		// 1. CodeVuidNotFound means the volume unit dose not exist and ignore it
		// 2. CodeDiskBroken need ignore it
		// 3. Other err, all tinkers need to be notified to update the volume mapping relationship
		// to avoid affecting the deletion process due to caching the old mapping relationship.
		// If the update is successful, continue with the following process, and return err it fails.
		httpCode := rpc.DetectStatusCode(err)
		if httpCode != errors.CodeVuidNotFound && httpCode != errors.CodeDiskBroken {
			err = mgr.notifyTinkerUpdateVolMapping(ctx, migTask)
			if err != nil {
				return base.ErrNotifyTinkerUpdateVol
			}
		}
		err = nil
	}

	err = mgr.clusterMgrClient.UnlockVolume(ctx, migTask.SourceVuid.Vid())
	if err != nil {
		span.Errorf("unlock volume failed, err:%v", err)
		return
	}
	interrupt.Inject(mgr.taskType + "_finish_task")
	// update db
	migTask.State = proto.MigrateStateFinished
	base.LoopExecUntilSuccess(ctx, "migrate finish task update task tbl", func() error {
		return mgr.taskTbl.Update(ctx, proto.MigrateStateWorkCompleted, migTask)
	})

	mgr.finishQueue.RemoveTask(migTask.TaskID)

	VolTaskLockerInst().Unlock(ctx, migTask.SourceVuid.Vid())
	mgr.diskMigratingVuids.deleteMigratingVuid(migTask.SourceDiskID, migTask.SourceVuid)

	mgr.finishTaskCounter.Add()

	span.Infof("finish task phase success, taskId: %s, state: %v", migTask.TaskID, migTask.State)
	return
}

func (mgr *MigrateMgr) notifyTinkerUpdateVolMapping(ctx context.Context, task *proto.MigrateTask) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("notify tinker to update volume mapping, vid:%d, taskId: %s", task.SourceVuid.Vid(), task.TaskID)

	hosts, err := mgr.getTinkerHosts(ctx)
	if err != nil {
		return
	}
	g := base.NewNErrsGroup(len(hosts))
	for index, host := range hosts {
		index := index
		host := host
		vid := task.SourceVuid.Vid()
		g.Go(func() error {
			return mgr.tinkerClient.UpdateVol(ctx, host, vid, mgr.ClusterID)
		}, index)
	}
	for index, err := range g.Wait() {
		if err != nil {
			span.Errorf("notify tinker failed, host:%v, err:%v", hosts[index], err)
			return err
		}
	}
	return nil
}

// AddTask adds migrate task
func (mgr *MigrateMgr) AddTask(ctx context.Context, task *proto.MigrateTask) {
	// add task to db
	base.LoopExecUntilSuccess(ctx, "migrate add task insert task to tbl", func() error {
		return mgr.taskTbl.Insert(ctx, task)
	})

	// add task to prepare queue
	mgr.prepareQueue.PushTask(task.TaskID, task)

	mgr.diskMigratingVuids.addMigratingVuid(task.SourceDiskID, task.SourceVuid, task.TaskID)
}

// FinishTaskInAdvanceWhenLockFail finish migrate task in advance when lock volume failed
func (mgr *MigrateMgr) FinishTaskInAdvanceWhenLockFail(ctx context.Context, task *proto.MigrateTask) {
	mgr.finishTaskInAdvance(ctx, task, "lock volume fail")
}

func (mgr *MigrateMgr) finishTaskInAdvance(ctx context.Context, task *proto.MigrateTask, reason string) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("finish task in advance, taskId:%s, task: %+v", task.TaskID, task)

	task.State = proto.MigrateStateFinishedInAdvance
	task.FinishAdvanceReason = reason

	base.LoopExecUntilSuccess(ctx, "migrate finish task in advance update tbl", func() error {
		return mgr.taskTbl.Update(ctx, proto.MigrateStateInited, task)
	})

	mgr.finishTaskCounter.Add()
	mgr.prepareQueue.RemoveTask(task.TaskID)
	VolTaskLockerInst().Unlock(ctx, task.SourceVuid.Vid())
}

// ClearTasksByStates clear migrate task and set migrateState to DeleteMark
func (mgr *MigrateMgr) ClearTasksByStates(ctx context.Context, states []proto.MigrateSate) {
	base.LoopExecUntilSuccess(ctx, "migrate clear tasks by states", func() error {
		return mgr.taskTbl.MarkDeleteByStates(ctx, states)
	})
}

func (mgr *MigrateMgr) handleUpdateVolMappingFail(ctx context.Context, task *proto.MigrateTask, err error) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("handle update vol mapping fail taskId %s state %d dest vuid %d", task.TaskID, task.State, task.Destination.Vuid)

	code := rpc.DetectStatusCode(err)
	if code == errors.CodeOldVuidNotMatch {
		span.Panicf("change volume unit relationship failed, old vuid not match")
	}

	if base.ShouldAllocAndRedo(code) {
		span.Infof("re-alloc vunit and redo task_id %s", task.TaskID)
		newVunit, err := base.AllocVunitSafe(ctx, mgr.clusterMgrClient, task.SourceVuid, task.Sources)
		if err != nil {
			span.Errorf("re-alloc failed vuid %d err:%v", task.SourceVuid, err)
			return err
		}
		task.SetDest(newVunit.Location())
		task.State = proto.MigrateStatePrepared
		task.WorkerRedoCnt++

		base.LoopExecUntilSuccess(ctx, "migrate redo task update task tbl", func() error {
			return mgr.taskTbl.Update(ctx, proto.MigrateStateWorkCompleted, task)
		})

		mgr.finishQueue.RemoveTask(task.TaskID)
		mgr.workQueue.AddPreparedTask(task.SourceIdc, task.TaskID, task)
		span.Infof("task %+v redo again", task)

		return nil
	}

	return err
}

func (mgr *MigrateMgr) getTinkerHosts(ctx context.Context) (hosts []string, err error) {
	span := trace.SpanFromContextSafe(ctx)

	svrs, err := mgr.svrTbl.FindAll(ctx, proto.ServiceNameTinker, "")
	if err != nil {
		span.Errorf("get tinker svr failed, err:%v", err)
		return
	}
	for i := range svrs {
		hosts = append(hosts, svrs[i].Host)
	}
	return
}

// StatQueueTaskCnt returns queue task count
func (mgr *MigrateMgr) StatQueueTaskCnt() (inited, prepared, completed int) {
	todo, doing := mgr.prepareQueue.StatsTasks()
	inited = todo + doing

	todo, doing = mgr.workQueue.StatsTasks()
	prepared = todo + doing

	todo, doing = mgr.finishQueue.StatsTasks()
	completed = todo + doing
	return
}

// AcquireTask acquire migrate task
func (mgr *MigrateMgr) AcquireTask(ctx context.Context, idc string) (task *proto.MigrateTask, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("acquire migrate task,idc:%s", idc)

	if !mgr.taskSwitch.Enabled() {
		return nil, proto.ErrTaskPaused
	}

	_, migTask, _ := mgr.workQueue.Acquire(idc)
	if migTask != nil {
		task = migTask.(*proto.MigrateTask)
		span.Infof("acquire %s taskId: %s", mgr.taskType, task.TaskID)
		return task, nil
	}
	return nil, proto.ErrTaskEmpty
}

// CancelTask cancel migrate task
func (mgr *MigrateMgr) CancelTask(ctx context.Context, args *api.CancelTaskArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("cancel migrate taskId %s", args.TaskId)

	err = mgr.workQueue.Cancel(args.IDC, args.TaskId, args.Src, args.Dest)
	if err != nil {
		span.Errorf("cancel %s failed, taskId:%s, err:%v", mgr.taskType, args.TaskId, err)
	}

	return
}

// ReclaimTask reclaim migrate task
func (mgr *MigrateMgr) ReclaimTask(ctx context.Context, idc, taskID string,
	src []proto.VunitLocation,
	oldDst proto.VunitLocation,
	newDst *client.AllocVunitInfo) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("reclaim migrate taskID %s", taskID)

	err = mgr.workQueue.Reclaim(idc, taskID, src, oldDst, newDst.Location(), newDst.DiskID)
	if err != nil {
		span.Errorf("reclaim %s task failed, taskID:%s,err:%v", mgr.taskType, taskID, err)
		return err
	}

	task, err := mgr.workQueue.Query(idc, taskID)
	if err != nil {
		span.Errorf("found task in workQueue failed, idc: %s,taskID: %s,err:%v", idc, taskID, err)
		return err
	}

	err = mgr.taskTbl.Update(ctx, proto.MigrateStatePrepared, task.(*proto.MigrateTask))
	if err != nil {
		span.Errorf("update reclaim task failed, taskID:%s,err:%v", taskID, err)
	}

	return
}

// CompleteTask complete migrate task
func (mgr *MigrateMgr) CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("complete migrate taskId %s", args.TaskId)

	completeTask, err := mgr.workQueue.Complete(args.IDC, args.TaskId, args.Src, args.Dest)
	if err != nil {
		span.Errorf("complete migrate task failed,taskId: %s, err:%v", args.TaskId, err)
		return err
	}

	t := completeTask.(*proto.MigrateTask)
	t.State = proto.MigrateStateWorkCompleted

	// as this func is call by http request, so we cannot use LoopExecUntilSuccess to update task in db
	err = mgr.taskTbl.Update(ctx, proto.MigrateStatePrepared, t)
	if err != nil {
		// there is no impact if we fail to update task state in db,
		// because we will do it in finishTask again
		span.Errorf("complete migrate task into db failed, taskId: %s,err:%v", t.TaskID, err)
		return
	}
	mgr.finishQueue.PushTask(args.TaskId, t)
	return
}

// RenewalTask renewal migrate task
func (mgr *MigrateMgr) RenewalTask(ctx context.Context, idc, taskID string) (err error) {
	if !mgr.taskSwitch.Enabled() {
		return proto.ErrTaskPaused
	}

	span := trace.SpanFromContextSafe(ctx)
	span.Infof("Renewal migrate task, taskID:%s", taskID)

	err = mgr.workQueue.Renewal(idc, taskID)
	if err != nil {
		span.Warnf("renewal %s task failed, taskID:%s,err:%v", mgr.taskType, taskID, err)
	}
	return
}

// IsMigratingDisk returns true if disk is migrating
func (mgr *MigrateMgr) IsMigratingDisk(diskID proto.DiskID) bool {
	return mgr.diskMigratingVuids.isMigratingDisk(diskID)
}

// GetMigratingDiskNum returns migrating disk count
func (mgr *MigrateMgr) GetMigratingDiskNum() int {
	return mgr.diskMigratingVuids.getCurrMigratingDisksCnt()
}

// GetAllTasks returns all migrate task
func (mgr *MigrateMgr) GetAllTasks(ctx context.Context) (tasks []*proto.MigrateTask, err error) {
	return mgr.taskTbl.FindAll(ctx)
}

// ClearTasksByDiskIDLoop clear migrate task by diskID
func (mgr *MigrateMgr) ClearTasksByDiskIDLoop(ctx context.Context, diskID proto.DiskID) {
	base.LoopExecUntilSuccess(ctx, "migrate clear task by diskId", func() error {
		return mgr.taskTbl.MarkDeleteByDiskID(ctx, diskID)
	})
}
