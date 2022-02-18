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

package clustermgr

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/taskpool"
)

/*
	implements raftserver StateMachine
*/

var applyTaskPool = taskpool.New(5, 5)

func (s *Service) ApplyMemberChange(cc raftserver.ConfChange, index uint64) error {
	// record apply index and flush all memory data
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Info("receive member change: ", cc)
	member := base.RaftMember{
		ID:   cc.NodeID,
		Host: string(cc.Context),
	}
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode:
		if cc.Type == raftpb.ConfChangeAddLearnerNode {
			member.Learner = true
		}
		if err := s.raftNode.RecordRaftMember(ctx, member, false); err != nil {
			return err
		}
	case raftpb.ConfChangeRemoveNode:
		if err := s.raftNode.RecordRaftMember(ctx, member, true); err != nil {
			return err
		}
	}
	return s.raftNode.RecordApplyIndex(ctx, index, true)
}

func (s *Service) Apply(data [][]byte, index uint64) error {
	var (
		err       error
		errs      []error
		span, ctx = trace.StartSpanFromContext(context.Background(), "")
	)

	start := time.Now()
	// 1. decode all propose data and gather by module
	moduleOperTypes := make(map[string][]int32)
	moduleDatas := make(map[string][][]byte)
	moduleContexts := make(map[string][]base.ProposeContext)
	for i := range data {
		proposeInfo := base.DecodeProposeInfo(data[i])
		if proposeInfo == nil || proposeInfo.Module == "" || proposeInfo.OperType == 0 || proposeInfo.Data == nil {
			errMsg := fmt.Sprintf("raft statemachine Apply check failed ==> invalid propose data: %v", data)
			span.Error(errMsg)
			return errors.New(errMsg)
		}
		moduleOperTypes[proposeInfo.Module] = append(moduleOperTypes[proposeInfo.Module], proposeInfo.OperType)
		moduleDatas[proposeInfo.Module] = append(moduleDatas[proposeInfo.Module], proposeInfo.Data)
		moduleContexts[proposeInfo.Module] = append(moduleContexts[proposeInfo.Module], proposeInfo.Context)
	}
	decodeCost := time.Since(start)
	start = time.Now()

	// 2. call module applies's Apply method
	wg := sync.WaitGroup{}
	wg.Add(len(moduleOperTypes))
	errs = make([]error, len(moduleOperTypes))
	i := 0

	for module := range moduleOperTypes {
		idx := i
		_module := module
		applyTaskPool.Run(func() {
			defer wg.Done()
			errs[idx] = s.raftNode.ModuleApply(ctx, _module, moduleOperTypes[_module], moduleDatas[_module], moduleContexts[_module])
		})
		i += 1
	}
	wg.Wait()
	moduleApplyCost := time.Since(start)
	start = time.Now()

	for i := range errs {
		if errs[i] != nil {
			span.Error(errors.Detail(errs[i]))
			return errs[i]
		}
	}

	// 3. record apply index
	err = s.raftNode.RecordApplyIndex(ctx, index, false)
	if err != nil {
		err = errors.Info(err, "raft statemachine Apply record apply index failed").Detail(err)
		span.Error(errors.Detail(err))
		return err
	}
	span.Infof("state machine apply, total data: %d, decode cost: %dus, module apply cost: %dus, record apply index cost: %dus",
		len(data), decodeCost/time.Microsecond, moduleApplyCost/time.Microsecond, time.Since(start)/time.Microsecond)

	return nil
}

func (s *Service) Snapshot() (raftserver.Snapshot, error) {
	snapshot := s.raftNode.CreateRaftSnapshot(s.dbs, s.RaftConfig.SnapshotPatchNum)
	return snapshot, nil
}

func (s *Service) ApplySnapshot(meta raftserver.SnapshotMeta, st raftserver.Snapshot) error {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	// check if node has data already
	// if has, then panic program and notice someone to clean db path by manual
	currentIndex := s.raftNode.GetCurrentApplyIndex()
	span.Debugf("current apply index: ", currentIndex)
	if currentIndex > 0 {
		panic("node has persistent data already, should clean all db path and restart to begin a snapshot apply")
	}

	atomic.StoreUint32(&s.status, ServiceStatusSnapshot)
	span.Debugf("state machine receive apply snapshot")
	// decode snapshot data and put
	err := s.raftNode.ApplyRaftSnapshot(ctx, s.dbs, st)
	if err != nil {
		span.Errorf("apply raft snapshot failed, err: %v", err)
		return err
	}
	err = s.raftNode.RecordApplyIndex(ctx, meta.Index, true)
	if err != nil {
		span.Errorf("apply raft snapshot record apply index failed, err: %v", err)
		return err
	}
	atomic.StoreUint32(&s.status, ServiceStatusNormal)
	return nil
}

func (s *Service) LeaderChange(leader uint64, host string) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Debugf("receive leader change, leader: %d, host: %s ", leader, host)

	if leader > 0 {
		s.raftNode.SetLeaderHost(leader, host)
		// use for blocking raft start
		s.raftStartOnce.Do(func() {
			close(s.raftStartCh)
		})
		s.raftNode.NotifyLeaderChange(ctx, leader, host)
		// service has been elected to leader, then set service's electedLeaderReadIndex into NeedReadeIndex
		if leader == s.raftNode.Status().Id {
			atomic.StoreUint32(&s.electedLeaderReadIndex, NeedReadIndex)
		}
		return
	}
	s.raftNode.SetLeaderHost(0, "")
}
