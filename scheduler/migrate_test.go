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
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/common/codemode"
	comerrors "github.com/cubefs/blobstore/common/errors"
	errors2 "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
)

var (
	ErrMockResponse          = errors.New("response err")
	ErrMockUpdateVol         = errors.New("update vol err")
	ErrMockReleaseVolumeUnit = errors.New("release volume unit err")
	ErrMockUnlockVolume      = errors.New("unlock volume err")
	ErrMockAllocVolumeUnit   = errors.New("alloc volume err")
)

var (
	MockMigrateSwitchMap = map[string]string{
		taskswitch.BalanceSwitchName:     taskswitch.SwitchOpen,
		taskswitch.DiskRepairSwitchName:  taskswitch.SwitchOpen,
		taskswitch.DiskDropSwitchName:    taskswitch.SwitchOpen,
		taskswitch.BlobDeleteSwitchName:  taskswitch.SwitchOpen,
		taskswitch.ShardRepairSwitchName: taskswitch.SwitchOpen,
	}
	MockMigrateVolInfoMap = map[proto.Vid]*client.VolumeInfoSimple{
		100: MockMigrateGenVolInfo(100, codemode.EC6P6, proto.VolumeStatusIdle),
		101: MockMigrateGenVolInfo(101, codemode.EC6P10L2, proto.VolumeStatusIdle),
		102: MockMigrateGenVolInfo(102, codemode.EC6P10L2, proto.VolumeStatusActive),
		103: MockMigrateGenVolInfo(103, codemode.EC6P6, proto.VolumeStatusLock),
		104: MockMigrateGenVolInfo(104, codemode.EC6P6, proto.VolumeStatusLock),
		105: MockMigrateGenVolInfo(105, codemode.EC6P6, proto.VolumeStatusActive),

		300: MockMigrateGenVolInfo(300, codemode.EC6P6, proto.VolumeStatusIdle),
		301: MockMigrateGenVolInfo(301, codemode.EC6P10L2, proto.VolumeStatusIdle),
		302: MockMigrateGenVolInfo(302, codemode.EC6P10L2, proto.VolumeStatusActive),

		400: MockMigrateGenVolInfo(400, codemode.EC6P6, proto.VolumeStatusIdle),
		401: MockMigrateGenVolInfo(401, codemode.EC6P10L2, proto.VolumeStatusIdle),
		402: MockMigrateGenVolInfo(402, codemode.EC6P10L2, proto.VolumeStatusActive),
	}
)

func MockMigrateTasks(volInfos map[proto.Vid]*client.VolumeInfoSimple) map[string]*proto.MigrateTask {
	m := make(map[string]*proto.MigrateTask)

	t1 := mockGenMigrateTask("z0", 4, 100, proto.MigrateStateInited, volInfos)
	t2 := mockGenMigrateTask("z0", 5, 101, proto.MigrateStatePrepared, volInfos)
	t3 := mockGenMigrateTask("z1", 6, 102, proto.MigrateStateWorkCompleted, volInfos)
	t4 := mockGenMigrateTask("z2", 7, 103, proto.MigrateStateFinishedInAdvance, volInfos)
	t5 := mockGenMigrateTask("z2", 8, 104, proto.MigrateStateFinished, volInfos)
	t6 := mockGenMigrateTask("z0", 4, 105, proto.MigrateStateInited, volInfos)

	m[t1.TaskID] = t1
	m[t2.TaskID] = t2
	m[t3.TaskID] = t3
	m[t4.TaskID] = t4
	m[t5.TaskID] = t5
	m[t6.TaskID] = t6
	return m
}

func stateInArray(state proto.MigrateSate, arr []proto.MigrateSate) bool {
	for _, val := range arr {
		if state == val {
			return true
		}
	}
	return false
}

func findMigrateTaskByVid(m map[string]proto.MigrateTask, vid proto.Vid) (tasks []proto.MigrateTask, err error) {
	for _, task := range m {
		if task.SourceVuid.Vid() == vid {
			tasks = append(tasks, task)
		}
	}
	if len(tasks) == 0 {
		return nil, errors.New("mock task not found")
	}
	return
}

func NewMockRegisterTbl(respErr error) (tbl db.ISvrRegisterTbl) {
	mmap := make(map[string]*proto.SvrInfo)
	svr1 := &proto.SvrInfo{
		Host:   "127.0.0.1:xxx",
		Module: proto.ServiceNameTinker,
		IDC:    "z0",
		Ctime:  time.Now().String(),
	}
	svr2 := &proto.SvrInfo{
		Host:   "127.0.0.2:xxx",
		Module: proto.ServiceNameTinker,
		IDC:    "z1",
		Ctime:  time.Now().String(),
	}
	svr3 := &proto.SvrInfo{
		Host:   "127.0.0.3:xxx",
		Module: proto.ServiceNameTinker,
		IDC:    "z2",
		Ctime:  time.Now().String(),
	}
	mmap[svr1.Host] = svr1
	mmap[svr2.Host] = svr2
	mmap[svr3.Host] = svr3

	return NewMockServiceRegisterTbl(respErr, mmap)
}

func MockMigrateGenVolInfo(vid proto.Vid, cm codemode.CodeMode, status proto.VolumeStatus) *client.VolumeInfoSimple {
	vol := client.VolumeInfoSimple{}
	cmInfo := cm.Tactic()
	vunitCnt := cmInfo.M + cmInfo.N + cmInfo.L
	host := "127.0.0.0:xxx"
	diskID := 1
	locations := make([]proto.VunitLocation, vunitCnt)
	var idx uint8
	for i := 0; i < vunitCnt; i++ {
		locations[i].Vuid, _ = proto.NewVuid(vid, idx, 1)
		locations[i].Host = host
		locations[i].DiskID = proto.DiskID(diskID)
		idx++
	}
	vol.Status = status
	vol.VunitLocations = locations
	vol.Vid = vid
	vol.CodeMode = cm
	return &vol
}

func MockMigrateAlloc(vuid proto.Vuid) *client.AllocVunitInfo {
	vid := vuid.Vid()
	idx := vuid.Index()
	epoch := vuid.Epoch()
	epoch++
	newVuid, _ := proto.NewVuid(vid, idx, epoch)
	return &client.AllocVunitInfo{
		VunitLocation: proto.VunitLocation{
			Vuid:   newVuid,
			DiskID: proto.DiskID(newVuid),
			Host:   "127.0.0.0:xxx",
		},
	}
}

type mockMigrateCmClient struct {
	commRespErr    error
	disableRespErr bool

	methodRespErrMap map[string]error

	switchMap    map[string]string
	volInfoMap   map[proto.Vid]*client.VolumeInfoSimple
	disksMap     map[proto.DiskID]*client.DiskInfoSimple
	droppedDisks map[proto.DiskID]bool
	updatedVid   map[proto.Vid]bool
}

func NewMigrateMockCmClient(retErr error,
	mRespErr map[string]error,
	volInfoMap map[proto.Vid]*client.VolumeInfoSimple,
	disksMap map[proto.DiskID]*client.DiskInfoSimple) client.IClusterMgr {
	if mRespErr == nil {
		mRespErr = make(map[string]error)
	}
	return &mockMigrateCmClient{
		commRespErr:      retErr,
		methodRespErrMap: mRespErr,
		switchMap:        MockMigrateSwitchMap,
		volInfoMap:       volInfoMap,
		disksMap:         disksMap,
		droppedDisks:     make(map[proto.DiskID]bool),
		updatedVid:       make(map[proto.Vid]bool),
	}
}

func (m *mockMigrateCmClient) disableCommRespErr() {
	m.disableRespErr = true
}

func (m *mockMigrateCmClient) GetConfig(ctx context.Context, key string) (val string, err error) {
	return m.switchMap[key], m.getErrInfo()
}

func (m *mockMigrateCmClient) ListVolume(ctx context.Context, vid proto.Vid, count int) (volInfo []*client.VolumeInfoSimple, retVid proto.Vid, err error) {
	return
}

func (m *mockMigrateCmClient) GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *client.VolumeInfoSimple, err error) {
	return m.volInfoMap[Vid], m.getErrInfo()
}

func (m *mockMigrateCmClient) LockVolume(ctx context.Context, vid proto.Vid) (err error) {
	if m.disableRespErr == false {
		return m.getErrInfo()
	}
	vol := m.volInfoMap[vid]
	if vol.IsActive() {
		return errors2.ErrLockNotAllow
	}
	return nil
}

func (m *mockMigrateCmClient) UnlockVolume(ctx context.Context, Vid proto.Vid) (err error) {
	return m.getErrInfo()
}

func (m *mockMigrateCmClient) UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error) {
	m.updatedVid[newVuid.Vid()] = true
	return m.getErrInfo()
}

func (m *mockMigrateCmClient) AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (ret *client.AllocVunitInfo, err error) {
	return MockMigrateAlloc(vuid), m.getErrInfo()
}

func (m *mockMigrateCmClient) ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID) (err error) {
	return m.getErrInfo()
}

func (m *mockMigrateCmClient) ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (ret []*client.VunitInfoSimple, err error) {
	used := uint64(100000)
	for _, volInfo := range m.volInfoMap {
		if _, ok := m.updatedVid[volInfo.Vid]; ok {
			continue
		}

		tmp := &client.VunitInfoSimple{
			Vuid:   volInfo.VunitLocations[0].Vuid,
			DiskID: diskID,
			Host:   "127.0.0.1:xx",
			Used:   used,
		}
		ret = append(ret, tmp)
		used--
	}
	return ret, m.getErrInfo()
}

func (m *mockMigrateCmClient) ListClusterDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	return m.allDisks(), m.getErrInfo()
}

func (m *mockMigrateCmClient) ListBrokenDisks(ctx context.Context, count int) (disks []*client.DiskInfoSimple, err error) {
	return m.allDisks(), m.getErrInfo()
}

func (m *mockMigrateCmClient) ListRepairingDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	return m.allDisks(), m.getErrInfo()
}

func (m *mockMigrateCmClient) ListDropDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	return m.allDisks(), m.getErrInfo()
}

func (m *mockMigrateCmClient) SetDiskRepairing(ctx context.Context, diskID proto.DiskID) (err error) {
	return m.getErrInfo()
}

func (m *mockMigrateCmClient) SetDiskRepaired(ctx context.Context, diskID proto.DiskID) (err error) {
	return m.getErrInfo()
}

func (m *mockMigrateCmClient) SetDiskDropped(ctx context.Context, diskID proto.DiskID) (err error) {
	m.droppedDisks[diskID] = true
	return m.getErrInfo()
}

func (m *mockMigrateCmClient) GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *client.DiskInfoSimple, err error) {
	return m.disksMap[diskID], m.getErrInfo()
}

func (m *mockMigrateCmClient) allDisks() []*client.DiskInfoSimple {
	var ret []*client.DiskInfoSimple
	for _, disk := range m.disksMap {
		ret = append(ret, disk)
	}
	return ret
}

func (m *mockMigrateCmClient) getErrInfo() error {
	if val, ok := m.methodRespErrMap[runFuncName()]; ok {
		return val
	}
	return m.commRespErr
}

type mockMigrateTinkerClient struct {
	commRespErr error
}

func NewTinkerMockClient(retErr error) client.ITinker {
	return &mockMigrateTinkerClient{
		commRespErr: retErr,
	}
}

func (m mockMigrateTinkerClient) UpdateVol(ctx context.Context, host string, vid proto.Vid, clusterID proto.ClusterID) (err error) {
	return m.commRespErr
}

func initMigrateMgr(respErr error, conf *MigrateConfig) (mgr *MigrateMgr, err error) {
	mockCmCli := NewMigrateMockCmClient(respErr, nil, MockMigrateVolInfoMap, MockDisksMap)
	switchMgr := taskswitch.NewSwitchMgr(mockCmCli)
	mockTinkerCli := NewTinkerMockClient(respErr)

	taskSwitch, err := switchMgr.AddSwitch(taskswitch.BalanceSwitchName)
	if err != nil {
		panic("unexpect add task switch fail")
	}

	mockRegisterTbl := NewMockRegisterTbl(respErr)
	mockMigrateTbl := NewMockMigrateTbl(respErr, MockMigrateTasks(MockMigrateVolInfoMap))

	mgr = NewMigrateMgr(
		mockCmCli,
		mockTinkerCli,
		taskSwitch,
		mockRegisterTbl,
		mockMigrateTbl, conf,
		proto.BalanceTaskType)
	mgr.SetLockFailHandleFunc(mgr.FinishTaskInAdvanceWhenLockFail)
	return
}

func TestMigrateMgr(t *testing.T) {
	conf := &MigrateConfig{
		TaskCommonConfig: base.TaskCommonConfig{
			PrepareQueueRetryDelayS: 1,
			FinishQueueRetryDelayS:  1,
			CancelPunishDurationS:   1,
			WorkQueueSize:           3,
		},
	}
	mgr, err := initMigrateMgr(nil, conf)
	require.NoError(t, err)

	testMigrateLoad(t, mgr)
	testMigrateRun(t, mgr)
	testMigrateTaskChange(t, mgr)

	conf = &MigrateConfig{}
	conf.CheckAndFix()
	mgr, err = initMigrateMgr(nil, conf)
	require.NoError(t, err)
	testPrepareTaskErr(t, mgr)
	testFinishTaskErr(t, mgr)
}

func testMigrateLoad(t *testing.T, mgr *MigrateMgr) {
	err := mgr.Load()
	require.NoError(t, err)

	tasks, err := mgr.taskTbl.FindAll(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 6, len(tasks))

	tasks, err = mgr.taskTbl.(*mockBaseMigrateTbl).FindByVid(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	_, exist := mgr.prepareQueue.Query(tasks[0].TaskID)
	require.Equal(t, true, exist)

	tasks, err = mgr.taskTbl.(*mockBaseMigrateTbl).FindByVid(context.Background(), 101)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	_, err = mgr.workQueue.Query("z0", tasks[0].TaskID)
	require.NoError(t, err)

	tasks, err = mgr.taskTbl.(*mockBaseMigrateTbl).FindByVid(context.Background(), 102)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	_, exist = mgr.finishQueue.Query(tasks[0].TaskID)
	require.Equal(t, true, exist)

	tasks, err = mgr.taskTbl.(*mockBaseMigrateTbl).FindByVid(context.Background(), 103)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	_, exist = mgr.finishQueue.Query(tasks[0].TaskID)
	require.Equal(t, false, exist)

	tasks, err = mgr.taskTbl.(*mockBaseMigrateTbl).FindByVid(context.Background(), 104)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	_, exist = mgr.finishQueue.Query(tasks[0].TaskID)
	require.Equal(t, false, exist)

	tasks, err = mgr.taskTbl.(*mockBaseMigrateTbl).FindByVid(context.Background(), 105)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	mgr.finishTaskInAdvance(context.Background(), tasks[0], "")
}

func testPrepareTaskErr(t *testing.T, mgr *MigrateMgr) {
	mockCmCli := NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	task := mockGenMigrateTask("z0", 5, 300, proto.MigrateStateInited, MockMigrateVolInfoMap)
	mgr.prepareQueue.PushTask(task.TaskID, task)

	m := make(map[string]error)
	m["LockVolume"] = comerrors.ErrLockNotAllow
	mockCmCli = NewMigrateMockCmClient(nil, m, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	err := mgr.prepareTask()
	require.NoError(t, err)

	mockCmCli = NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	task = mockGenMigrateTask("z0", 5, 301, proto.MigrateStateInited, MockMigrateVolInfoMap)
	mgr.prepareQueue.PushTask(task.TaskID, task)

	m = make(map[string]error)
	m["LockVolume"] = ErrMockResponse
	mockCmCli = NewMigrateMockCmClient(nil, m, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	err = mgr.prepareTask()
	require.EqualError(t, ErrMockResponse, err.Error(), MockMigrateVolInfoMap)

	mockCmCli = NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	task = mockGenMigrateTask("z0", 5, 302, proto.MigrateStateInited, MockMigrateVolInfoMap)
	mgr.prepareQueue.PushTask(task.TaskID, task)

	m = make(map[string]error)
	m["AllocVolumeUnit"] = ErrMockAllocVolumeUnit
	mockCmCli = NewMigrateMockCmClient(nil, m, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	err = mgr.prepareTask()
	require.Error(t, err)
	require.EqualError(t, ErrMockAllocVolumeUnit, err.Error())

	mockCmCli = NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	task = mockGenMigrateTask("z0", 5, 400, proto.MigrateStateInited, MockMigrateVolInfoMap)
	mgr.prepareQueue.PushTask(task.TaskID, task)

	volMap := MockMigrateVolInfoMap
	vuid := volMap[400].VunitLocations[0].Vuid
	vid := vuid.Vid()
	idx := vuid.Index()
	epoch := vuid.Epoch()
	epoch++
	newVuid, _ := proto.NewVuid(vid, idx, epoch)
	volMap[400].VunitLocations[0].Vuid = newVuid
	mockCmCli = NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	err = mgr.prepareTask()
	require.NoError(t, err)

	mockCmCli = NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	task = mockGenMigrateTask("z0", 5, 401, proto.MigrateStateInited, MockMigrateVolInfoMap)
	mgr.prepareQueue.PushTask(task.TaskID, task)

	volMap = MockMigrateVolInfoMap
	vuid = volMap[401].VunitLocations[0].Vuid
	vid = vuid.Vid()
	idx = vuid.Index()
	epoch = vuid.Epoch()
	epoch++
	newVuid, _ = proto.NewVuid(vid, idx, epoch)
	volMap[401].VunitLocations[0].Vuid = newVuid
	m = make(map[string]error)
	m["UnlockVolume"] = ErrMockUnlockVolume
	mockCmCli = NewMigrateMockCmClient(nil, m, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	err = mgr.prepareTask()
	require.EqualError(t, ErrMockUnlockVolume, err.Error())
}

func testFinishTaskErr(t *testing.T, mgr *MigrateMgr) {
	mockCmCli := NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	task := mockGenMigrateTask("z0", 5, 400, proto.MigrateStateWorkCompleted, MockMigrateVolInfoMap)
	mgr.finishQueue.PushTask(task.TaskID, task)

	m := make(map[string]error)
	m["UpdateVolume"] = comerrors.ErrNewVuidNotMatch
	mockCmCli = NewMigrateMockCmClient(nil, m, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	err := mgr.finishTask()
	require.NoError(t, err)

	mockCmCli = NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	task = mockGenMigrateTask("z0", 5, 401, proto.MigrateStateWorkCompleted, MockMigrateVolInfoMap)
	mgr.finishQueue.PushTask(task.TaskID, task)

	m = make(map[string]error)
	m["ReleaseVolumeUnit"] = ErrMockReleaseVolumeUnit
	m["UnlockVolume"] = ErrMockUnlockVolume
	mockCmCli = NewMigrateMockCmClient(nil, m, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	mockTinkerCli := NewTinkerMockClient(nil)
	mgr.tinkerClient = mockTinkerCli
	err = mgr.finishTask()
	// release volume unit fail and notify tinker success,unlock volume err
	require.EqualError(t, ErrMockUnlockVolume, err.Error())

	mockCmCli = NewMigrateMockCmClient(nil, nil, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	task = mockGenMigrateTask("z0", 5, 402, proto.MigrateStateWorkCompleted, MockMigrateVolInfoMap)
	mgr.finishQueue.PushTask(task.TaskID, task)

	m = make(map[string]error)
	m["ReleaseVolumeUnit"] = ErrMockReleaseVolumeUnit
	m["UnlockVolume"] = ErrMockUnlockVolume
	mockCmCli = NewMigrateMockCmClient(nil, m, MockMigrateVolInfoMap, MockDisksMap)
	mgr.clusterMgrClient = mockCmCli
	mockTinkerCli = NewTinkerMockClient(ErrMockResponse)
	mgr.tinkerClient = mockTinkerCli
	err = mgr.finishTask()
	// release volume unit fail and notify tinker fail
	require.EqualError(t, base.ErrNotifyTinkerUpdateVol, err.Error())
}

func testMigrateRun(t *testing.T, mgr *MigrateMgr) {
	err := mgr.prepareTask()
	require.NoError(t, err)
	err = mgr.finishTask()
	require.NoError(t, err)

	tasks, err := findMigrateTaskByVid(mgr.taskTbl.(*mockBaseMigrateTbl).tasksMap, 102)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	_, exist := mgr.finishQueue.Query(tasks[0].TaskID)
	require.Equal(t, false, exist)

	tasks, err = findMigrateTaskByVid(mgr.taskTbl.(*mockBaseMigrateTbl).tasksMap, 100)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	_, exist = mgr.prepareQueue.Query(tasks[0].TaskID)
	require.Equal(t, false, exist)

	tasks, err = findMigrateTaskByVid(mgr.taskTbl.(*mockBaseMigrateTbl).tasksMap, 100)
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))
	mgr.workQueue.Query("z0", tasks[0].TaskID)
	require.Equal(t, false, exist)
}

func checkTestTask(task *proto.MigrateTask) bool {
	if task.SourceDiskID == 4 && task.SourceVuid.Vid() == 100 {
		return true
	}
	if task.SourceDiskID == 5 && task.SourceVuid.Vid() == 101 {
		return true
	}
	return false
}

func testMigrateTaskChange(t *testing.T, mgr *MigrateMgr) {
	ctx := context.Background()
	mgr.taskSwitch.Enable()
	// task1
	task, err := mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)
	require.Equal(t, true, checkTestTask(task))

	args := api.CancelTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.SourceIdc,
		TaskType: proto.BalanceTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CancelTask(ctx, &args)
	require.NoError(t, err)

	// task2
	task, err = mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)
	require.Equal(t, true, checkTestTask(task))

	args = api.CancelTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.SourceIdc,
		TaskType: proto.BalanceTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CancelTask(ctx, &args)
	require.NoError(t, err)

	// no task
	_, err = mgr.AcquireTask(ctx, "z0")
	require.EqualError(t, proto.ErrTaskEmpty, err.Error())

	// sleep punish time and get task
	time.Sleep(time.Duration(mgr.CancelPunishDurationS) * time.Second)
	task, err = mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)
	require.Equal(t, true, checkTestTask(task))

	// reclaim task
	oldTask := *task
	newDst, _ := mgr.clusterMgrClient.AllocVolumeUnit(ctx, oldTask.Destination.Vuid)
	err = mgr.ReclaimTask(ctx, oldTask.SourceIdc, oldTask.TaskID, oldTask.Sources, oldTask.Destination, newDst)
	require.NoError(t, err)
	newTask, err := mgr.workQueue.Query(oldTask.SourceIdc, oldTask.TaskID)
	require.NoError(t, err)
	require.NotEqual(t, oldTask.GetDest().Vuid, newTask.GetDest())

	// complete task
	task, err = mgr.AcquireTask(ctx, "z0")
	require.NoError(t, err)
	require.Equal(t, proto.DiskID(4), task.SourceDiskID)
	require.Equal(t, proto.Vid(100), task.SourceVuid.Vid())

	completeArgs := api.CompleteTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.SourceIdc,
		TaskType: proto.BalanceTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
	err = mgr.CompleteTask(ctx, &completeArgs)
	require.NoError(t, err)

	_, err = mgr.workQueue.Query(task.SourceIdc, task.TaskID)
	require.EqualError(t, base.ErrNoSuchMessageID, err.Error())

	_, ok := mgr.finishQueue.Query(task.TaskID)
	require.Equal(t, true, ok)

	todo, doing := mgr.prepareQueue.StatsTasks()
	require.Equal(t, 0, todo+doing)
	todo, doing = mgr.workQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)
	todo, doing = mgr.finishQueue.StatsTasks()
	require.Equal(t, 1, todo+doing)
}

func TestMigrateStats(t *testing.T) {
	conf := &MigrateConfig{
		TaskCommonConfig: base.TaskCommonConfig{
			PrepareQueueRetryDelayS: 1,
			FinishQueueRetryDelayS:  1,
			CancelPunishDurationS:   1,
			WorkQueueSize:           3,
		},
	}
	mgr, err := initMigrateMgr(nil, conf)
	require.NoError(t, err)
	mgr.StatQueueTaskCnt()
}

func runFuncName() string {
	pc := make([]uintptr, 1)
	runtime.Callers(3, pc)
	return strings.TrimPrefix(filepath.Ext(runtime.FuncForPC(pc[0]).Name()), ".")
}

func genCompleteArgs(task *proto.MigrateTask) *api.CompleteTaskArgs {
	return &api.CompleteTaskArgs{
		TaskId:   task.TaskID,
		IDC:      task.SourceIdc,
		TaskType: proto.DiskDropTaskType,
		Src:      task.Sources,
		Dest:     task.Destination,
	}
}
