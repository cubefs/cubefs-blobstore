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
	"sync"
	"time"

	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/blobstore/util/log"
)

func init() {
	log.SetOutputLevel(log.Lfatal)
}

type mockBaseCmClient struct{}

func NewMockClusterManagerClient() client.IClusterMgr {
	return &mockBaseCmClient{}
}

func (cm *mockBaseCmClient) GetConfig(ctx context.Context, key string) (val string, err error) {
	return
}

func (cm *mockBaseCmClient) GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *client.VolumeInfoSimple, err error) {
	return
}

func (cm *mockBaseCmClient) LockVolume(ctx context.Context, Vid proto.Vid) (err error) {
	return
}

func (cm *mockBaseCmClient) UnlockVolume(ctx context.Context, Vid proto.Vid) (err error) {
	return
}

func (cm *mockBaseCmClient) UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error) {
	return
}

func (cm *mockBaseCmClient) AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (ret *client.AllocVunitInfo, err error) {
	return mockAllocVunit(vuid), nil
}

func (cm *mockBaseCmClient) ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (ret []*client.VunitInfoSimple, err error) {
	return
}

func (cm *mockBaseCmClient) ListVolume(ctx context.Context, vid proto.Vid, count int) (volInfo []*client.VolumeInfoSimple, retVid proto.Vid, err error) {
	return
}

func (cm *mockBaseCmClient) ListClusterDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	return
}

func (cm *mockBaseCmClient) ListBrokenDisks(ctx context.Context, count int) (disks []*client.DiskInfoSimple, err error) {
	return
}

func (cm *mockBaseCmClient) ListRepairingDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	return
}

func (cm *mockBaseCmClient) ListDropDisks(ctx context.Context) (disks []*client.DiskInfoSimple, err error) {
	return
}

func (cm *mockBaseCmClient) SetDiskRepairing(ctx context.Context, diskID proto.DiskID) (err error) {
	return
}

func (cm *mockBaseCmClient) SetDiskRepaired(ctx context.Context, diskID proto.DiskID) (err error) {
	return
}

func (cm *mockBaseCmClient) SetDiskDropped(ctx context.Context, diskID proto.DiskID) (err error) {
	return
}

func (cm *mockBaseCmClient) GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *client.DiskInfoSimple, err error) {
	return mockDiskInfo(diskID), err
}

func (cm *mockBaseCmClient) ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID) (err error) {
	return
}

type mockBaseTinkerClient struct{}

func NewMockTinkerClient() client.ITinker {
	return &mockBaseTinkerClient{}
}

func (tinker *mockBaseTinkerClient) UpdateVol(ctx context.Context, host string, vid proto.Vid, clusterID proto.ClusterID) (err error) {
	return
}

type mockMqProxy struct{}

func (m *mockMqProxy) SendShardRepairMsg(ctx context.Context, vid proto.Vid, bid proto.BlobID, badIdx []uint8) error {
	return nil
}

// MockVolsList mock volume list getter
type MockVolsList struct {
	vols   []*client.VolumeInfoSimple
	maxVid proto.Vid
}

// NewMockVolsList return volume list for mock
func NewMockVolsList() *MockVolsList {
	return &MockVolsList{
		maxVid: 0,
	}
}

func initAllocMockVol(volsGetter *MockVolsList) {
	// 5 vol
	for i := 0; i < 4; i++ {
		volsGetter.allocVolume(codemode.EC6P10L2, proto.VolumeStatusLock)
	}
	volsGetter.allocVolume(codemode.EC6P10L2, proto.VolumeStatusActive)
}

func (m *MockVolsList) allocVolume(mode codemode.CodeMode, status proto.VolumeStatus) {
	m.maxVid++
	replicas, _ := genMockVol(m.maxVid, mode)
	vol := client.VolumeInfoSimple{
		Vid:            m.maxVid,
		VunitLocations: replicas,
		CodeMode:       mode,
		Status:         status,
	}

	m.vols = append(m.vols, &vol)
}

func (m *MockVolsList) ListVolume(ctx context.Context, vid proto.Vid, count int) ([]*client.VolumeInfoSimple, proto.Vid, error) {
	n := 0
	var ret []*client.VolumeInfoSimple
	if vid == 0 {
		vid = 1
	}
	startIdx := int(uint32(vid) - 1)
	for ; startIdx < len(m.vols); startIdx++ {
		ret = append(ret, m.vols[startIdx])
		n++
		if n >= count {
			break
		}
	}

	return ret, proto.Vid(uint32(vid) + uint32(n)), nil
}

func (m *MockVolsList) GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *client.VolumeInfoSimple, err error) {
	for _, vol := range m.vols {
		if vol.Vid == Vid {
			return vol, nil
		}
	}
	return nil, errors.New("not found")
}

type mockBaseMigrateTbl struct {
	respErr error

	tasksMap map[string]proto.MigrateTask
	mu       sync.RWMutex
}

func NewMockMigrateTbl(respErr error, taskMap map[string]*proto.MigrateTask) (tbl db.IMigrateTaskTbl) {
	return &mockBaseMigrateTbl{
		tasksMap: copyMap(taskMap),
		respErr:  respErr,
	}
}

func (tbl *mockBaseMigrateTbl) Insert(ctx context.Context, task *proto.MigrateTask) error {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	tbl.tasksMap[task.TaskID] = *task
	return tbl.respErr
}

func (tbl *mockBaseMigrateTbl) Delete(ctx context.Context, taskID string) error {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	delete(tbl.tasksMap, taskID)
	return tbl.respErr
}

func (tbl *mockBaseMigrateTbl) MarkDeleteByDiskID(ctx context.Context, diskID proto.DiskID) error {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	for key, task := range tbl.tasksMap {
		if task.SourceDiskID == diskID {
			delete(tbl.tasksMap, key)
		}
	}
	return tbl.respErr
}

func (tbl *mockBaseMigrateTbl) MarkDeleteByStates(ctx context.Context, states []proto.MigrateSate) error {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	for key, task := range tbl.tasksMap {
		if stateInArray(task.State, states) {
			delete(tbl.tasksMap, key)
		}
	}
	return tbl.respErr
}

func (tbl *mockBaseMigrateTbl) Update(ctx context.Context, state proto.MigrateSate, task *proto.MigrateTask) error {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	tbl.tasksMap[task.TaskID] = *task
	return tbl.respErr
}

func (tbl *mockBaseMigrateTbl) Find(ctx context.Context, taskID string) (task *proto.MigrateTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	if _, ok := tbl.tasksMap[taskID]; !ok {
		return nil, base.ErrNoDocuments
	}
	t := tbl.tasksMap[taskID]
	return &t, tbl.respErr
}

func (tbl *mockBaseMigrateTbl) FindByVid(ctx context.Context, vid proto.Vid) (tasks []*proto.MigrateTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	for _, task := range tbl.tasksMap {
		if task.SourceVuid.Vid() == vid {
			t := task.Copy()
			tasks = append(tasks, t)
		}
	}
	if len(tasks) == 0 {
		return nil, base.ErrNoDocuments
	}
	return tasks, tbl.respErr
}

func (tbl *mockBaseMigrateTbl) FindByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	for _, task := range tbl.tasksMap {
		if task.SourceDiskID == diskID {
			t := task.Copy()
			tasks = append(tasks, t)
		}
	}
	return tasks, tbl.respErr
}

func (tbl *mockBaseMigrateTbl) FindAll(ctx context.Context) (tasks []*proto.MigrateTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	for _, task := range tbl.tasksMap {
		t := task.Copy()
		tasks = append(tasks, t)
	}
	return tasks, tbl.respErr
}

func (tbl *mockBaseMigrateTbl) FindMarkDeletedTask(ctx context.Context) (tasks []*proto.MigrateTask, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	return tasks, tbl.respErr
}

type MockBaseServiceRegisterTbl struct {
	respErr error
	svrMap  map[string]*proto.SvrInfo
	mu      sync.RWMutex
}

func NewMockServiceRegisterTbl(respErr error, svrMap map[string]*proto.SvrInfo) (tbl db.ISvrRegisterTbl) {
	tbl = &MockBaseServiceRegisterTbl{
		svrMap:  svrMap,
		respErr: respErr,
	}
	return
}

func (tbl *MockBaseServiceRegisterTbl) Register(ctx context.Context, info *proto.SvrInfo) error {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	tbl.svrMap[info.Host] = info
	return tbl.respErr
}

func (tbl *MockBaseServiceRegisterTbl) Delete(ctx context.Context, host string) error {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	delete(tbl.svrMap, host)
	return tbl.respErr
}

func (tbl *MockBaseServiceRegisterTbl) Find(ctx context.Context, host string) (svr *proto.SvrInfo, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	if _, ok := tbl.svrMap[host]; !ok {
		return nil, base.ErrNoDocuments
	}
	return tbl.svrMap[host], tbl.respErr
}

func (tbl *MockBaseServiceRegisterTbl) FindAll(ctx context.Context, module, idc string) (svrs []*proto.SvrInfo, err error) {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	for _, val := range tbl.svrMap {
		if val.Module == module {
			svrs = append(svrs, val)
		}
	}
	return svrs, tbl.respErr
}

type mockCheckpointTbl struct {
	ck proto.InspectCheckPoint
}

func newMockCheckpointTbl() *mockCheckpointTbl {
	return &mockCheckpointTbl{
		ck: proto.InspectCheckPoint{
			Id:       "inspect_checkpoint",
			StartVid: minVid,
			Ctime:    time.Now().String(),
		},
	}
}

func (m *mockCheckpointTbl) GetCheckPoint(ctx context.Context) (ck *proto.InspectCheckPoint, err error) {
	return &m.ck, nil
}

func (m *mockCheckpointTbl) SaveCheckPoint(ctx context.Context, startVid proto.Vid) error {
	m.ck.StartVid = startVid
	return nil
}

func mockGenMigrateTask(idc string, diskID proto.DiskID, vid proto.Vid, state proto.MigrateSate, volInfoMap map[proto.Vid]*client.VolumeInfoSimple) (task *proto.MigrateTask) {
	srcs := volInfoMap[vid].VunitLocations

	codeMode := volInfoMap[vid].CodeMode
	vunitInfo := MockMigrateAlloc(volInfoMap[vid].VunitLocations[0].Vuid)
	task = &proto.MigrateTask{
		TaskID:       base.GenTaskID("balance", vid),
		State:        state,
		SourceIdc:    idc,
		SourceDiskID: diskID,
		SourceVuid:   volInfoMap[vid].VunitLocations[0].Vuid,
		Sources:      srcs,
		CodeMode:     codeMode,

		Destination: vunitInfo.Location(),
		Ctime:       time.Now().String(),
		MTime:       time.Now().String(),
	}
	return task
}

func mockGenVolRepairTask(vid proto.Vid, state proto.RepairState, diskID proto.DiskID, volInfoMap map[proto.Vid]*client.VolumeInfoSimple) *proto.VolRepairTask {
	vunitLocations := volInfoMap[vid].VunitLocations
	codeMode := volInfoMap[vid].CodeMode
	dst := MockAlloc(volInfoMap[vid].VunitLocations[0].Vuid).Location()
	task := proto.VolRepairTask{
		TaskID:        base.GenTaskID("repair", vid),
		State:         state,
		RepairDiskID:  diskID,
		CodeMode:      codeMode,
		Sources:       vunitLocations,
		BadVuid:       vunitLocations[0].Vuid,
		BadIdx:        0,
		BrokenDiskIDC: "z0",
	}

	if state == proto.RepairStatePrepared ||
		state == proto.RepairStateWorkCompleted ||
		state == proto.RepairStateFinished {
		task.Destination = dst
	}
	return &task
}

func MockAlloc(vuid proto.Vuid) *client.AllocVunitInfo {
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

func mockAllocVunit(vuid proto.Vuid) *client.AllocVunitInfo {
	vid := vuid.Vid()
	idx := vuid.Index()
	epoch := vuid.Epoch()
	epoch++
	newVuid, _ := proto.NewVuid(vid, idx, epoch)
	return &client.AllocVunitInfo{
		VunitLocation: proto.VunitLocation{
			Vuid:   newVuid,
			DiskID: 111,
			Host:   "127.0.0.0:xxx",
		},
	}
}

func mockDiskInfo(diskID proto.DiskID) *client.DiskInfoSimple {
	return &client.DiskInfoSimple{
		DiskID: diskID,
		Status: proto.DiskStatusNormal,
	}
}

func copyMap(taskMap map[string]*proto.MigrateTask) map[string]proto.MigrateTask {
	ret := make(map[string]proto.MigrateTask, len(taskMap))
	for id, task := range taskMap {
		ret[id] = *task
	}
	return ret
}
