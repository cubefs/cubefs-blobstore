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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/codemode"
	comErr "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
)

//------------------------------------------------------------------------------
//repair_mgr UT init
func newMockSwitchMap() map[string]string {
	return map[string]string{
		taskswitch.BalanceSwitchName:     taskswitch.SwitchOpen,
		taskswitch.DiskRepairSwitchName:  taskswitch.SwitchOpen,
		taskswitch.DiskDropSwitchName:    taskswitch.SwitchOpen,
		taskswitch.BlobDeleteSwitchName:  taskswitch.SwitchOpen,
		taskswitch.ShardRepairSwitchName: taskswitch.SwitchOpen,
		taskswitch.VolInspectSwitchName:  taskswitch.SwitchOpen,
	}
}

func newMockVolInfoMap() map[proto.Vid]*client.VolumeInfoSimple {
	return map[proto.Vid]*client.VolumeInfoSimple{
		1: MockGenVolInfo(1, codemode.EC6P6, proto.VolumeStatusIdle),
		2: MockGenVolInfo(2, codemode.EC6P10L2, proto.VolumeStatusIdle),
		3: MockGenVolInfo(3, codemode.EC6P10L2, proto.VolumeStatusActive),
		4: MockGenVolInfo(4, codemode.EC6P6, proto.VolumeStatusLock),
		5: MockGenVolInfo(5, codemode.EC6P6, proto.VolumeStatusLock),

		6: MockGenVolInfo(6, codemode.EC6P6, proto.VolumeStatusLock),
		7: MockGenVolInfo(7, codemode.EC6P6, proto.VolumeStatusLock),
	}
}

func MockGenVolInfo(vid proto.Vid, cm codemode.CodeMode, status proto.VolumeStatus) *client.VolumeInfoSimple {
	vol := client.VolumeInfoSimple{}
	cmInfo := cm.Tactic()
	vunitCnt := cmInfo.M + cmInfo.N + cmInfo.L
	host := "127.0.0.0:xxx"
	locations := make([]proto.VunitLocation, vunitCnt)
	var idx uint8
	for i := 0; i < vunitCnt; i++ {
		locations[i].Vuid, _ = proto.NewVuid(vid, idx, 1)
		locations[i].Host = host
		locations[i].DiskID = proto.DiskID(locations[i].Vuid)
		idx++
	}
	vol.Status = status
	vol.VunitLocations = locations
	vol.Vid = vid
	vol.CodeMode = cm
	return &vol
}

func newMockDisksMap() map[proto.DiskID]*client.DiskInfoSimple {
	disk1 := client.DiskInfoSimple{
		ClusterID: 1,
		Idc:       "z0",
		Rack:      "r0",
		Host:      "127.0.0.1:xx",
		DiskID:    1,
		Status:    proto.DiskStatusBroken,
	}
	disk2 := client.DiskInfoSimple{
		ClusterID: 1,
		Idc:       "z1",
		Rack:      "r1",
		Host:      "127.0.0.1:xx",
		DiskID:    2,
		Status:    proto.DiskStatusNormal,
	}
	disk3 := client.DiskInfoSimple{
		ClusterID: 1,
		Idc:       "z2",
		Rack:      "r2",
		Host:      "127.0.0.1:xx",
		DiskID:    3,
		Status:    proto.DiskStatusNormal,
	}

	MockDisksMap = map[proto.DiskID]*client.DiskInfoSimple{
		1: &disk1,
		2: &disk2,
		3: &disk3,
	}
	return MockDisksMap
}

//------------------------------------------------------------------------
//mock class
type mockCmClient struct {
	mu     sync.Mutex
	RetErr error

	updateErr       error
	updateErrCnt    int
	updateErrMaxCnt int

	SwitchMap  map[string]string
	VolInfoMap map[proto.Vid]*client.VolumeInfoSimple
	DisksMap   map[proto.DiskID]*client.DiskInfoSimple

	DroppedVuid map[proto.Vuid]bool
}

func NewMockCmClient(retErr error, taskTbl *mockBaseRepairTbl) client.IClusterMgr {
	m := &mockCmClient{
		RetErr:      retErr,
		SwitchMap:   newMockSwitchMap(),
		VolInfoMap:  newMockVolInfoMap(),
		DisksMap:    newMockDisksMap(),
		DroppedVuid: make(map[proto.Vuid]bool),
	}
	if taskTbl == nil {
		return m
	}

	for _, task := range taskTbl.tasksMap {
		if task.Finished() {
			m.DroppedVuid[task.BadVuid] = true
		}
	}
	return m
}

func (m *mockCmClient) emptyDroppedVuid() {
	m.DroppedVuid = make(map[proto.Vuid]bool)
}

func (m *mockCmClient) GetConfig(ctx context.Context, key string) (val string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SwitchMap[key], err
}

func (m *mockCmClient) NotUpdateSwitch(switchName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SwitchMap[switchName] = "Invalidation"
}

// volume op api
func (m *mockCmClient) GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *client.VolumeInfoSimple, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.VolInfoMap[Vid]; !ok {
		return nil, errors.New("not exist")
	}

	return m.VolInfoMap[Vid], m.RetErr
}

func (m *mockCmClient) ListVolume(ctx context.Context, vid proto.Vid, count int) (volInfo []*client.VolumeInfoSimple, retVid proto.Vid, err error) {
	return
}

func (m *mockCmClient) LockVolume(ctx context.Context, Vid proto.Vid) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.RetErr
}

func (m *mockCmClient) UnlockVolume(ctx context.Context, Vid proto.Vid) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.RetErr
}

func (m *mockCmClient) UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DroppedVuid[oldVuid] = true

	defer func() {
		m.updateErrCnt++
	}()

	err = m.updateErr
	if m.updateErrCnt >= m.updateErrMaxCnt {
		err = nil
	}

	return err
}

func (m *mockCmClient) AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (ret *client.AllocVunitInfo, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return MockAlloc(vuid), m.RetErr
}

func (m *mockCmClient) ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.RetErr
}

func (m *mockCmClient) ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (ret []*client.VunitInfoSimple, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	used := uint64(100000)
	for _, volInfo := range m.VolInfoMap {
		vuid := volInfo.VunitLocations[0].Vuid
		if m.vuidIsDropped(vuid) {
			continue
		}

		tmp := &client.VunitInfoSimple{
			Vuid:   vuid,
			DiskID: diskID,
			Host:   "127.0.0.1:xx",
			Used:   used,
		}
		ret = append(ret, tmp)
		used--
	}
	return ret, nil
}

func (m *mockCmClient) vuidIsDropped(vuid proto.Vuid) bool {
	_, dropped := m.DroppedVuid[vuid]
	return dropped
}

// disk op api

func (m *mockCmClient) ListClusterDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.allDisks(), m.RetErr
}

func (m *mockCmClient) ListBrokenDisks(ctx context.Context, count int) (disks []*client.DiskInfoSimple, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Printf("===>ListBrokenDisks\n")
	for _, disk := range m.DisksMap {
		fmt.Printf("===>disk status %d\n", disk.Status)
		if disk.Status == proto.DiskStatusBroken {
			disks = append(disks, disk)
		}
		if len(disks) == count {
			return disks, m.RetErr
		}
	}
	return disks, m.RetErr
}

func (m *mockCmClient) ListRepairingDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, disk := range m.DisksMap {
		if disk.Status == proto.DiskStatusRepairing {
			disks = append(disks, disk)
		}
	}
	return disks, m.RetErr
}

func (m *mockCmClient) ListDropDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.allDisks(), m.RetErr
}

func (m *mockCmClient) SetDiskRepairing(ctx context.Context, diskID proto.DiskID) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DisksMap[diskID].Status = proto.DiskStatusRepairing
	return m.RetErr
}

func (m *mockCmClient) SetDiskRepaired(ctx context.Context, diskID proto.DiskID) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DisksMap[diskID].Status = proto.DiskStatusRepaired
	return m.RetErr
}

func (m *mockCmClient) SetDiskDropped(ctx context.Context, diskID proto.DiskID) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DisksMap[diskID].Status = proto.DiskStatusDropped
	return m.RetErr
}

func (m *mockCmClient) GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *client.DiskInfoSimple, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Printf("mockCmClient GetDiskInfo diskID %d\n", diskID)
	return m.DisksMap[diskID], m.RetErr
}

func (m *mockCmClient) allDisks() []*client.DiskInfoSimple {
	var ret []*client.DiskInfoSimple
	for _, disk := range m.DisksMap {
		ret = append(ret, disk)
	}
	return ret
}

// ---------------------------------------------------------------------------
// mock vol repair tbl
type mockBaseRepairTbl struct {
	retErr error

	tasksMap map[string]*proto.VolRepairTask
	mu       sync.RWMutex
}

func NewMockRepairTbl(err error, taskMap map[string]*proto.VolRepairTask) (tbl db.IRepairTaskTbl) {
	return &mockBaseRepairTbl{
		retErr:   err,
		tasksMap: taskMap,
	}
}

func (tbl *mockBaseRepairTbl) Insert(ctx context.Context, t *proto.VolRepairTask) error {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	if _, ok := tbl.tasksMap[t.TaskID]; ok {
		return errors.New("mock taskId duplicate")
	}
	tbl.tasksMap[t.TaskID] = t
	return tbl.retErr
}

func (tbl *mockBaseRepairTbl) Update(ctx context.Context, t *proto.VolRepairTask) error {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	if _, ok := tbl.tasksMap[t.TaskID]; !ok {
		return errors.New("mock task not found")
	}
	tbl.tasksMap[t.TaskID] = t
	return tbl.retErr
}

func (tbl *mockBaseRepairTbl) Find(ctx context.Context, taskID string) (task *proto.VolRepairTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	if _, ok := tbl.tasksMap[taskID]; !ok {
		return nil, base.ErrNoDocuments
	}
	return tbl.tasksMap[taskID], tbl.retErr
}

func (tbl *mockBaseRepairTbl) FindByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.VolRepairTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	for _, task := range tbl.tasksMap {
		if task.RepairDiskID == diskID {
			tasks = append(tasks, task)
		}
	}
	return tasks, tbl.retErr
}

func (tbl *mockBaseRepairTbl) FindAll(ctx context.Context) (tasks []*proto.VolRepairTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	for _, task := range tbl.tasksMap {
		tasks = append(tasks, task)
	}
	return tasks, tbl.retErr
}

func (tbl *mockBaseRepairTbl) FindMarkDeletedTask(ctx context.Context) (tasks []*proto.VolRepairTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	return tasks, tbl.retErr
}

func (tbl *mockBaseRepairTbl) MarkDeleteByDiskID(ctx context.Context, diskID proto.DiskID) error {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	for _, task := range tbl.tasksMap {
		if task.RepairDiskID == diskID {
			delete(tbl.tasksMap, task.TaskID)
		}
	}
	return tbl.retErr
}

func newMockTbl(retErr error, volInfoMap map[proto.Vid]*client.VolumeInfoSimple) *mockBaseRepairTbl {
	tbl := mockBaseRepairTbl{retErr: retErr}
	tbl.tasksMap = make(map[string]*proto.VolRepairTask)

	// prepare queue:1
	// worker queue:1
	// finish queue:1
	t1 := mockGenVolRepairTask(1, proto.RepairStateInited, 1, volInfoMap)
	t2 := mockGenVolRepairTask(2, proto.RepairStatePrepared, 1, volInfoMap)
	t3 := mockGenVolRepairTask(3, proto.RepairStateFinishedInAdvance, 1, volInfoMap)
	t4 := mockGenVolRepairTask(4, proto.RepairStateWorkCompleted, 1, volInfoMap)
	t5 := mockGenVolRepairTask(5, proto.RepairStateFinished, 1, volInfoMap)

	tbl.tasksMap[t1.TaskID] = t1
	tbl.tasksMap[t2.TaskID] = t2
	tbl.tasksMap[t3.TaskID] = t3
	tbl.tasksMap[t4.TaskID] = t4
	tbl.tasksMap[t5.TaskID] = t5

	return &tbl
}

func resetMockTbl(tbl *mockBaseRepairTbl, tasks map[string]*proto.VolRepairTask) {
	tbl.tasksMap = tasks
}

//--------------------------------------------------------------------------------
func getTaskIDByVid(tasksMap map[string]*proto.VolRepairTask, vid proto.Vid) string {
	for taskID := range tasksMap {
		if strings.Contains(taskID, fmt.Sprintf("repair-%d-", vid)) {
			return taskID
		}
	}
	return ""
}

//-------------------------------------------------------------------------------------------
//init repair_mgr object
func initRepairMgrWithErr(updateErr error, updateErrCnt int) (*RepairMgr, error) {
	mgr, err := initRepairMgr()
	if err != nil {
		return mgr, err
	}
	mgr.cmCli.(*mockCmClient).updateErr = updateErr
	mgr.cmCli.(*mockCmClient).updateErrMaxCnt = updateErrCnt

	return mgr, nil
}

func initRepairMgr() (*RepairMgr, error) {
	tmpCfg := RepairMgrCfg{}
	tmpCfg.WorkQueueSize = 10
	tmpCfg.CancelPunishDurationS = 1
	tmpCfg.FinishQueueRetryDelayS = 1
	tmpCfg.CollectTaskIntervalS = 1
	tmpCfg.CheckTaskIntervalS = 1
	tmpCfg.PrepareQueueRetryDelayS = 1
	tbl := newMockTbl(nil, newMockVolInfoMap())
	mockCmCli := NewMockCmClient(nil, tbl)
	switchMgr := taskswitch.NewSwitchMgr(mockCmCli)
	return NewRepairMgr(&tmpCfg, switchMgr, tbl, mockCmCli)
}

func RepairMgrFaultInjection(mgr *RepairMgr, cmCliErr, dbErr error) {
	mgr.cmCli.(*mockCmClient).RetErr = cmCliErr
	mgr.taskTbl.(*mockBaseRepairTbl).retErr = dbErr
}

//--------------------------------------------------------------------------
//start ut
func TestRepairMgr(t *testing.T) {
	MockEmptyVolTaskLocker()

	mgr, err := initRepairMgr()
	require.NoError(t, err)
	testLoad(mgr, t)
	testCollectTask(mgr, t)
	testPopTaskAndPrepare(mgr, t)
	testAcquireCancelReclaimComplete(mgr, t)
	testFinish(mgr, t)
	testCheckRepairedAndClear(mgr, t)
}

func testLoad(mgr *RepairMgr, t *testing.T) {
	err := mgr.Load()
	require.NoError(t, err)
	tasks, _ := mgr.taskTbl.FindAll(context.Background())
	require.Equal(t, 5, len(tasks))

	// test stats
	_, _, err = mgr.QueryTask(context.Background(), tasks[0].TaskID)
	require.NoError(t, err)
	mgr.GetTaskStats()
	mgr.StatQueueTaskCnt()

	_, exist := mgr.prepareQueue.Query(getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 1))
	require.Equal(t, true, exist)
	_, err = mgr.workQueue.Query("z0", getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 2))
	require.NoError(t, err)
	_, exist = mgr.finishQueue.Query(getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 3))
	require.Equal(t, false, exist)
	_, exist = mgr.finishQueue.Query(getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 4))
	require.Equal(t, true, exist)
	_, exist = mgr.finishQueue.Query(getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 5))
	require.Equal(t, false, exist)
}

func testCollectTask(mgr *RepairMgr, t *testing.T) {
	mgr.collectTask()
	tasks, _ := mgr.taskTbl.FindAll(context.Background())
	require.Equal(t, 7, len(tasks))
	todo, doing := mgr.prepareQueue.StatsTasks()
	require.Equal(t, len(mgr.cmCli.(*mockCmClient).VolInfoMap)-5+1, todo+doing)
	todo, doing = mgr.workQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)
	todo, doing = mgr.finishQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)
}

func testPopTaskAndPrepare(mgr *RepairMgr, t *testing.T) {
	err := mgr.popTaskAndPrepare()
	require.NoError(t, err)
	err = mgr.popTaskAndPrepare()
	require.NoError(t, err)
	err = mgr.popTaskAndPrepare()
	require.NoError(t, err)

	task, err := mgr.workQueue.Query("z0", getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 1))
	require.NoError(t, err)
	require.Equal(t, proto.RepairStatePrepared, task.(*proto.VolRepairTask).State)

	require.NoError(t, err)
	task, err = mgr.workQueue.Query("z0", getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 6))
	require.NoError(t, err)
	require.Equal(t, proto.RepairStatePrepared, task.(*proto.VolRepairTask).State)

	require.NoError(t, err)
	task, _ = mgr.workQueue.Query("z0", getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 7))
	require.Equal(t, proto.RepairStatePrepared, task.(*proto.VolRepairTask).State)

	task, err = mgr.workQueue.Query("z0", getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 2))
	require.NoError(t, err)
	require.Equal(t, proto.RepairStatePrepared, task.(*proto.VolRepairTask).State)

	todo, doing := mgr.prepareQueue.StatsTasks()
	require.Equal(t, 0, todo+doing)
	todo, doing = mgr.workQueue.StatsTasks()
	require.Equal(t, 4, todo+doing)
}

func testAcquireCancelReclaimComplete(mgr *RepairMgr, t *testing.T) {
	ctx := context.Background()
	mgr.taskSwitch.Enable()
	// task1
	task, err := mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)
	args := api.CancelTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.BrokenDiskIDC,
		TaskType: proto.RepairTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CancelTask(ctx, &args)
	require.NoError(t, err)
	// task2
	task, err = mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)
	args = api.CancelTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.BrokenDiskIDC,
		TaskType: proto.RepairTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CancelTask(ctx, &args)
	require.NoError(t, err)
	// task3
	task, err = mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)
	args = api.CancelTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.BrokenDiskIDC,
		TaskType: proto.RepairTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CancelTask(ctx, &args)
	require.NoError(t, err)
	// task4
	task, err = mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)
	args = api.CancelTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.BrokenDiskIDC,
		TaskType: proto.RepairTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CancelTask(ctx, &args)
	require.NoError(t, err)

	// no task to acquire
	_, err = mgr.AcquireTask(ctx, "z0")
	require.Error(t, err)
	require.EqualError(t, proto.ErrTaskEmpty, err.Error())

	time.Sleep(time.Duration(mgr.CancelPunishDurationS) * time.Second)
	// test punish time expired
	task, err = mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)

	// test reclaim
	newDst, _ := mgr.cmCli.AllocVolumeUnit(ctx, task.Destination.Vuid)
	fmt.Printf("===>Destination:%+v\n", task.Destination)
	fmt.Printf("===>newDst:%+v\n", newDst)
	err = mgr.ReclaimTask(ctx, task.BrokenDiskIDC, task.TaskID, task.Sources, task.Destination, newDst)
	require.NoError(t, err)
	taskTmp, err := mgr.workQueue.Query(task.BrokenDiskIDC, task.TaskID)
	require.NoError(t, err)
	fmt.Printf("===>task:%+v\n", task)
	fmt.Printf("===>taskTmp:%+v\n", taskTmp)

	// test complete
	completeArgs := api.CompleteTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.BrokenDiskIDC,
		TaskType: proto.RepairTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CompleteTask(ctx, &completeArgs)
	require.NoError(t, err)
	taskDb, _ := mgr.taskTbl.Find(ctx, task.TaskID)
	require.Equal(t, proto.RepairStateWorkCompleted, taskDb.State)
	_, err = mgr.workQueue.Query(task.BrokenDiskIDC, task.TaskID)
	require.Error(t, err)
	require.EqualError(t, base.ErrNoSuchMessageID, err.Error())
	taskFromQueue, exist := mgr.finishQueue.Query(task.TaskID)
	require.Equal(t, true, exist)
	require.Equal(t, proto.RepairStateWorkCompleted, taskFromQueue.(*proto.VolRepairTask).State)

	todo, doing := mgr.prepareQueue.StatsTasks()
	require.Equal(t, 0, todo+doing)
	todo, doing = mgr.workQueue.StatsTasks()
	require.Equal(t, 3, todo+doing)
	todo, doing = mgr.finishQueue.StatsTasks()
	require.Equal(t, 2, todo+doing)
}

func testFinish(mgr *RepairMgr, t *testing.T) {
	err := mgr.popTaskAndFinish()
	require.NoError(t, err)
	todo, doing := mgr.finishQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)
}

func TestFinishFail(t *testing.T) {
	MockEmptyVolTaskLocker()

	mgr, err := initRepairMgrWithErr(comErr.ErrNewVuidNotMatch, 1)
	require.NoError(t, err)
	testLoad(mgr, t)

	todo, doing := mgr.workQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)

	err = mgr.popTaskAndFinish()
	require.NoError(t, err)

	todo, doing = mgr.finishQueue.StatsTasks()
	require.Equal(t, 0, todo+doing)

	todo, doing = mgr.workQueue.StatsTasks()
	require.Equal(t, 2, todo+doing)
}

func testCheckRepairedAndClear(mgr *RepairMgr, t *testing.T) {
	ctx := context.Background()

	mgr.checkRepairedAndClear()
	tasks, err := mgr.taskTbl.FindAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 7, len(tasks))

	todo, doing := mgr.prepareQueue.StatsTasks()
	for i := 1; i <= todo+doing; i++ {
		mgr.popTaskAndPrepare()
	}
	todo, doing = mgr.workQueue.StatsTasks()
	for i := 1; i <= todo+doing; i++ {
		task, err := mgr.AcquireTask(ctx, "z0")
		require.NoError(t, err)
		completeArgs := api.CompleteTaskArgs{
			TaskId:   task.TaskID,
			IDC:      task.BrokenDiskIDC,
			TaskType: proto.RepairTaskType,
			Src:      task.Sources,
			Dest:     task.Destination,
		}
		err = mgr.CompleteTask(ctx, &completeArgs)
		require.NoError(t, err)
	}

	todo, doing = mgr.finishQueue.StatsTasks()
	for i := 1; i <= todo+doing; i++ {
		mgr.popTaskAndFinish()
	}
	repairingDiskID := mgr.getRepairingDiskID()
	tasks, _ = mgr.taskTbl.FindAll(ctx)
	for _, task := range tasks {
		fmt.Printf("keno task state %d\n", task.State)
	}

	mgr.checkRepairedAndClear()
	tasks, err = mgr.taskTbl.FindAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(tasks))
	require.Equal(t, proto.DiskID(base.EmptyDiskID), mgr.getRepairingDiskID())
	ret, err := mgr.cmCli.GetDiskInfo(context.Background(), repairingDiskID)
	require.NoError(t, err)
	require.Equal(t, proto.DiskStatusRepaired, ret.Status)
}

func TestCollectTaskNotFirst(t *testing.T) {
	MockEmptyVolTaskLocker()
	mgr, err := initRepairMgr()
	require.NoError(t, err)
	mgr.hasRevised = true

	resetMockTbl(mgr.taskTbl.(*mockBaseRepairTbl), make(map[string]*proto.VolRepairTask))
	mgr.cmCli.(*mockCmClient).emptyDroppedVuid()

	mgr.collectTask()
	brokenDisks, _ := mgr.cmCli.ListBrokenDisks(context.Background(), 1)
	fmt.Printf("brokenDisks len %d\n", len(brokenDisks))
	tasks, _ := mgr.taskTbl.FindAll(context.Background())
	require.Equal(t, 7, len(tasks))
	for _, task := range tasks {
		require.Equal(t, proto.RepairStateInited, task.State)
		fmt.Printf("taskId %s state %d\n", task.TaskID, task.State)
	}

	mockCmCli := mgr.cmCli.(*mockCmClient)
	disks, _ := mockCmCli.ListRepairingDisks(context.Background())
	require.Equal(t, 1, len(disks))
	require.Equal(t, proto.DiskStatusRepairing, disks[0].Status)
	require.Equal(t, proto.DiskID(1), disks[0].DiskID)
}

func TestPrepareFinishAdvance(t *testing.T) {
	MockEmptyVolTaskLocker()
	mgr, err := initRepairMgrWithErr(comErr.ErrNewVuidNotMatch, 1)
	require.NoError(t, err)
	testLoad(mgr, t)
	todo, doing := mgr.prepareQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)
	todo, doing = mgr.finishQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)

	todo, doing = mgr.workQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)

	task, exist := mgr.prepareQueue.Query(getTaskIDByVid(mgr.taskTbl.(*mockBaseRepairTbl).tasksMap, 1))
	require.Equal(t, true, exist)
	BadVuid := task.(*proto.VolRepairTask).BadVuid
	newBadVuid, err := mgr.cmCli.AllocVolumeUnit(context.Background(), BadVuid)
	require.NoError(t, err)
	task.(*proto.VolRepairTask).BadVuid = newBadVuid.Vuid
	err = mgr.prepareTask(task.(*proto.VolRepairTask))
	require.NoError(t, err)

	todo, doing = mgr.prepareQueue.StatsTasks()
	require.Equal(t, 0, todo+doing)

	todo, doing = mgr.finishQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)

	todo, doing = mgr.workQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)
}

func TestRenewalTask(t *testing.T) {
	MockEmptyVolTaskLocker()
	mgr, err := initRepairMgr()
	require.NoError(t, err)
	volInfoMap := newMockVolInfoMap()
	task := mockGenVolRepairTask(1, proto.RepairStateInited, 1, volInfoMap)
	mgr.taskSwitch.Enable()
	mgr.workQueue.SetLeaseExpiredS(200 * time.Millisecond)

	mgr.workQueue.AddPreparedTask(task.BrokenDiskIDC, task.TaskID, task)
	acquireTask, err := mgr.AcquireTask(context.Background(), task.BrokenDiskIDC)
	require.NoError(t, err)
	require.Equal(t, task.TaskID, acquireTask.TaskID)

	time.Sleep(time.Duration(100) * time.Millisecond)
	err = mgr.RenewalTask(context.Background(), task.BrokenDiskIDC, task.TaskID)
	require.NoError(t, err)
	time.Sleep(time.Duration(100) * time.Millisecond)
	_, err = mgr.AcquireTask(context.Background(), task.BrokenDiskIDC)
	require.Error(t, err)
	require.EqualError(t, proto.ErrTaskEmpty, err.Error())

	time.Sleep(time.Duration(140) * time.Millisecond)
	acquireTask, err = mgr.AcquireTask(context.Background(), task.BrokenDiskIDC)
	require.NoError(t, err)
	require.Equal(t, task.TaskID, acquireTask.TaskID)
}

func TestRun(t *testing.T) {
	MockEmptyVolTaskLocker()
	mgr, err := initRepairMgr()
	require.NoError(t, err)
	mgr.Run()

	mgr.cmCli.(*mockCmClient).NotUpdateSwitch(taskswitch.DiskRepairSwitchName)
	mgr.taskSwitch.Disable()
	err = mgr.RenewalTask(context.Background(), "", "")
	require.Error(t, err)
	require.EqualError(t, proto.ErrTaskPaused, err.Error())

	_, err = mgr.AcquireTask(context.Background(), "")
	require.Error(t, err)
	require.EqualError(t, proto.ErrTaskPaused, err.Error())

	mgr.taskSwitch.Enable()
	args := api.CancelTaskArgs{
		TaskId: "XX",
		IDC:    "z0",
	}
	err = mgr.CancelTask(context.Background(), &args)
	require.Error(t, err)

	completeArgs := api.CompleteTaskArgs{
		TaskId: "XX",
		IDC:    "z0",
	}
	err = mgr.CompleteTask(context.Background(), &completeArgs)
	require.Error(t, err)

	newDst := client.AllocVunitInfo{}
	err = mgr.ReclaimTask(context.Background(), "z0", "", nil, proto.VunitLocation{}, &newDst)
	require.Error(t, err)
	mgr.Close()
}

func TestPrepareErr(t *testing.T) {
	MockEmptyVolTaskLocker()

	mgr, err := initRepairMgr()
	require.NoError(t, err)
	testLoad(mgr, t)
	testCollectTask(mgr, t)
	RepairMgrFaultInjection(mgr, errors.New("cm cli fake error"), nil)
	taskMap1 := VolTaskLockerInst().taskMap
	todo1, doing1 := mgr.prepareQueue.StatsTasks()
	err = mgr.popTaskAndPrepare()
	require.Error(t, err)
	todo2, doing2 := mgr.prepareQueue.StatsTasks()
	require.Equal(t, todo1+doing1, todo2+doing2)
	taskMap2 := VolTaskLockerInst().taskMap
	require.Equal(t, len(taskMap1), len(taskMap2))
	for k, v := range taskMap1 {
		v2, ok := taskMap2[k]
		require.Equal(t, true, ok)
		require.Equal(t, v, v2)
	}
}

func TestAcquireBrokenDisk(t *testing.T) {
	MockEmptyVolTaskLocker()
	mgr, err := initRepairMgr()
	require.NoError(t, err)

	ctx := context.Background()

	mgr.brokenDisk = &client.DiskInfoSimple{DiskID: 999}
	disk, err := mgr.acquireBrokenDisk(ctx)
	require.NoError(t, err)
	require.Equal(t, proto.DiskID(999), disk.DiskID)

	mgr.brokenDisk = nil
	mgr.cmCli.(*mockCmClient).RetErr = errors.New("fake error")
	_, err = mgr.acquireBrokenDisk(ctx)
	require.Error(t, err)

	mgr.brokenDisk = nil
	mgr.cmCli.(*mockCmClient).RetErr = nil
	mgr.cmCli.(*mockCmClient).DisksMap = make(map[proto.DiskID]*client.DiskInfoSimple)
	_, err = mgr.acquireBrokenDisk(ctx)
	require.NoError(t, err)

	mgr.cmCli.(*mockCmClient).DisksMap[888] = &client.DiskInfoSimple{
		DiskID: 888,
		Status: proto.DiskStatusBroken,
	}

	disk, err = mgr.acquireBrokenDisk(ctx)
	fmt.Printf("disk %+v\n", disk)
	require.NoError(t, err)
	require.Equal(t, proto.DiskID(888), disk.DiskID)
}
