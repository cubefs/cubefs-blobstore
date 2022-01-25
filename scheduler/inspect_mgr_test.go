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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/util/errors"
)

func genMockVol(vid proto.Vid, mode codemode.CodeMode) ([]proto.VunitLocation, codemode.CodeMode) {
	modeInfo := mode.Tactic()
	replicas := make([]proto.VunitLocation, modeInfo.N+modeInfo.M+modeInfo.L)
	for i := 0; i < modeInfo.N+modeInfo.M+modeInfo.L; i++ {
		vuid, _ := proto.NewVuid(vid, uint8(i), 1)
		replicas[i] = proto.VunitLocation{
			Vuid: vuid,
			Host: "127.0.0.1:xxxx",
		}
	}
	return replicas, mode
}

func genMockFailShards(vid proto.Vid, bids []proto.BlobID) []*proto.MissedShard {
	vuid, _ := proto.NewVuid(vid, 1, 1)
	var FailShards []*proto.MissedShard
	for _, bid := range bids {
		FailShards = append(FailShards, &proto.MissedShard{Vuid: vuid, Bid: bid})
	}
	return FailShards
}

type XXInfo struct {
	InspectErr error
	FailShards []*proto.MissedShard
	Timeout    bool
}

func (XX *XXInfo) errStr() string {
	if XX.InspectErr != nil {
		return XX.InspectErr.Error()
	}
	return ""
}

func doInspectTest(vid proto.Vid) *XXInfo {
	m := make(map[proto.Vid]*XXInfo)
	m[1] = &XXInfo{InspectErr: errors.New("fake error")}
	m[2] = &XXInfo{Timeout: true}
	m[3] = &XXInfo{FailShards: genMockFailShards(3, []proto.BlobID{3, 4})}
	m[4] = &XXInfo{FailShards: genMockFailShards(4, []proto.BlobID{4, 5, 6})}
	m[5] = &XXInfo{FailShards: genMockFailShards(5, []proto.BlobID{3, 4, 5, 6, 7})}
	if _, ok := m[vid]; ok {
		return m[vid]
	}
	return &XXInfo{}
}

type mockmqProxyClient struct {
	mq []*proto.ShardRepairMsg
}

func (m *mockmqProxyClient) SendShardRepairMsg(ctx context.Context, vid proto.Vid, bid proto.BlobID, badIdx []uint8) error {
	msg := &proto.ShardRepairMsg{
		ClusterID: 0,
		Bid:       bid,
		Vid:       vid,
		BadIdx:    badIdx,
	}
	m.mq = append(m.mq, msg)
	return nil
}

func TestInspectMgr(t *testing.T) {
	cfg := InspectMgrCfg{
		InspectBatch:      3,
		ListVolIntervalMs: 100,
		ListVolStep:       1,
		TimeoutMs:         100,
	}
	mockCmCli := NewMockCmClient(nil, nil)
	switchMgr := taskswitch.NewSwitchMgr(mockCmCli)
	ckTbl := newMockCheckpointTbl()

	volsList := NewMockVolsList()
	initAllocMockVol(volsList)

	mqproxyCli := mockmqProxyClient{}
	mgr, _ := NewInspectMgr(&cfg, ckTbl, volsList, &mqproxyCli, switchMgr)

	require.Equal(t, 5, len(volsList.vols))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("return finish")
				return
			default:
				// fmt.Printf("start AcquireInspect\n")
				task, err := mgr.AcquireInspect(ctx)
				// fmt.Printf("start AcquireInspect task %+v err %+v\n", task, err)
				time.Sleep(10 * time.Millisecond)
				if err == nil {
					retInfo := doInspectTest(task.Replicas[0].Vuid.Vid())
					inspectRet := proto.InspectRet{
						TaskID:        task.TaskId,
						InspectErrStr: retInfo.errStr(),
						MissedShards:  retInfo.FailShards,
					}
					// fmt.Printf("CompleteInspect task %+v\n", task)
					if !retInfo.Timeout {
						mgr.CompleteInspect(ctx, &inspectRet)
					}
				}
			}
		}
	}()
	// test prepare
	require.Equal(t, true, mgr.firstPrepare)
	require.Equal(t, 0, len(mgr.tasks))
	mgr.prepare(ctx)
	require.Equal(t, int(1), int(mgr.startVid))
	require.Equal(t, int(4), int(mgr.nextVid))
	require.Equal(t, false, mgr.acquireEnable)
	require.Equal(t, false, mgr.firstPrepare)
	require.Equal(t, 3, len(mgr.tasks))

	// test wait complete
	mgr.waitCompleted(ctx)
	verifyInspectTaskTest(t, mgr)
	require.Equal(t, false, mgr.acquireEnable)

	// test finish
	mgr.finish(ctx)
	require.Equal(t, 0, len(mgr.tasks))

	mgr.prepare(ctx)
	require.Equal(t, int(4), int(mgr.startVid))
	require.Equal(t, int(6), int(mgr.nextVid))
	require.Equal(t, 1, len(mgr.tasks))

	taskVids := testGetTasksVid(mgr)
	require.Equal(t, true, testVidListEqual(taskVids, []proto.Vid{4}))
	mgr.waitCompleted(ctx)
	mgr.finish(ctx)
	require.Equal(t, 0, len(mgr.tasks))

	mgr.prepare(ctx)
	require.Equal(t, int(6), int(mgr.startVid))
	require.Equal(t, int(6), int(mgr.nextVid))
	require.Equal(t, 0, len(mgr.tasks))
	mgr.waitCompleted(ctx)
	mgr.finish(ctx)
	require.Equal(t, 0, len(mgr.tasks))

	mgr.prepare(ctx)
	require.Equal(t, int(zeroVid), int(mgr.startVid))
	require.Equal(t, int(4), int(mgr.nextVid))
	require.Equal(t, 3, len(mgr.tasks))

	cancel()
}

func verifyInspectTaskTest(t *testing.T, mgr *InspectMgr) {
	for _, task := range mgr.tasks {
		if !task.timeout(time.Duration(mgr.cfg.TimeoutMs)) {
			require.Equal(t, true, task.completed())
		}
	}
}

func TestCollectVolInspectBads(t *testing.T) {
	cfg := InspectMgrCfg{
		InspectBatch:      3,
		ListVolIntervalMs: 100,
		ListVolStep:       1,
		TimeoutMs:         100,
	}
	ctx := context.Background()
	mockCmCli := NewMockCmClient(nil, nil)
	switchMgr := taskswitch.NewSwitchMgr(mockCmCli)
	ckTbl := newMockCheckpointTbl()

	volsList := NewMockVolsList()
	initAllocMockVol(volsList)

	mqproxyCli := mockmqProxyClient{}
	mgr, _ := NewInspectMgr(&cfg, ckTbl, volsList, &mqproxyCli, switchMgr)

	replicas, _ := genMockVol(1, codemode.EC6P6)
	volMissedShards := []*proto.MissedShard{
		{Bid: 1, Vuid: replicas[0].Vuid},
		{Bid: 1, Vuid: replicas[1].Vuid},
		{Bid: 1, Vuid: replicas[2].Vuid},
		{Bid: 2, Vuid: replicas[0].Vuid},
		{Bid: 2, Vuid: replicas[3].Vuid},
	}
	bads, err := mgr.collectVolInspectBads(ctx, volMissedShards)
	require.NoError(t, err)
	fmt.Printf("bads %+v", bads)
	require.Equal(t, 3, len(bads[1]))
	require.Equal(t, uint8(0), bads[1][0])
	require.Equal(t, uint8(1), bads[1][1])
	require.Equal(t, uint8(2), bads[1][2])

	require.Equal(t, 2, len(bads[2]))
	require.Equal(t, uint8(0), bads[2][0])
	require.Equal(t, uint8(3), bads[2][1])

	replicas2, _ := genMockVol(2, codemode.EC6P6)
	volMissedShards = []*proto.MissedShard{
		{Bid: 1, Vuid: replicas[0].Vuid},
		{Bid: 1, Vuid: replicas[1].Vuid},
		{Bid: 1, Vuid: replicas[2].Vuid},
		{Bid: 2, Vuid: replicas[0].Vuid},
		{Bid: 2, Vuid: replicas2[3].Vuid},
	}
	_, err = mgr.collectVolInspectBads(ctx, volMissedShards)
	require.Error(t, err)
}

func TestTaskTimeout(t *testing.T) {
	task := inspectTaskInfo{}
	require.NoError(t, task.tryAcquire())
	require.Equal(t, false, task.timeout(5))
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, true, task.timeout(5))
	require.Equal(t, false, task.timeout(10000))
}

func testGetTasksVid(mgr *InspectMgr) []proto.Vid {
	var taskVids []proto.Vid
	for _, task := range mgr.tasks {
		taskVids = append(taskVids, task.t.Replicas[0].Vuid.Vid())
	}
	return taskVids
}

func testVidListEqual(l1, l2 []proto.Vid) bool {
	if len(l1) != len(l2) {
		return false
	}
	m := make(map[proto.Vid]bool)
	for _, e := range l1 {
		m[e] = true
	}

	for _, e := range l2 {
		if _, ok := m[e]; !ok {
			return false
		}
	}
	return true
}

func TestBadShardDeduplicator(t *testing.T) {
	d := newBadShardDeduplicator(3)
	require.Equal(t, false, d.reduplicate(1, 1, []uint8{1, 2}))

	d.add(1, 1, []uint8{1, 2})
	require.Equal(t, true, d.reduplicate(1, 1, []uint8{1, 2}))

	require.Equal(t, false, d.reduplicate(2, 1, []uint8{1, 2}))
	d.add(2, 1, []uint8{1, 2})

	require.Equal(t, false, d.reduplicate(2, 1, []uint8{1, 2, 3}))
	d.add(2, 1, []uint8{1, 2, 3})

	d.add(2, 2, []uint8{1, 2, 3})
	require.Equal(t, false, d.reduplicate(2, 1, []uint8{1, 2, 3}))
}
