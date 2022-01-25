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

package raftserver

//go:generate mockgen -destination=./storage_mock_test.go -package=raftserver -mock_names KVStorage=MockKVStorage github.com/cubefs/blobstore/common/raftserver KVStorage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/common/raftserver/wal"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	walDir = "/tmp/test/wal"
	nodeId = 1
)

type storeSnapshot struct {
	name  string
	index uint64
}

func (s *storeSnapshot) Name() string {
	return s.name
}

func (s *storeSnapshot) Index() uint64 {
	return s.index
}

func (s *storeSnapshot) Read() ([]byte, error) {
	return nil, nil
}

func (s *storeSnapshot) Close() {
}

type storeSM struct {
	applied   uint64
	snapIndex int
}

func (sm *storeSM) Apply(data [][]byte, index uint64) error {
	sm.applied = index
	return nil
}

func (sm *storeSM) ApplyMemberChange(cc ConfChange, index uint64) error {
	sm.applied = index
	return nil
}

func (sm *storeSM) Snapshot() (Snapshot, error) {
	sm.snapIndex++
	name := fmt.Sprintf("snapshot-%d", sm.snapIndex)
	return &storeSnapshot{name, sm.applied}, nil
}

func (sm *storeSM) ApplySnapshot(meta SnapshotMeta, st Snapshot) error {
	sm.applied = meta.Index
	return nil
}

func (sm *storeSM) LeaderChange(leader uint64, host string) {
}

func TestNewRaftStorage(t *testing.T) {
	os.RemoveAll(walDir)
	{
		ctrl := gomock.NewController(t)
		kv := NewMockKVStorage(ctrl)
		kv.EXPECT().Get(gomock.Any()).Return(nil, nil)
		store, err := NewRaftStorage(walDir, true, nodeId, kv, &storeSM{}, newSnapshotter(5, time.Second*10))
		require.Nil(t, err)
		store.Close()
		ctrl.Finish()
	}

	{
		ctrl := gomock.NewController(t)
		kv := NewMockKVStorage(ctrl)
		kv.EXPECT().Get(gomock.Any()).Return(nil, errors.New("get members error"))
		_, err := NewRaftStorage(walDir, true, nodeId, kv, &storeSM{}, newSnapshotter(5, time.Second*10))
		require.NotNil(t, err)
		ctrl.Finish()
	}

	{
		ctrl := gomock.NewController(t)
		kv := NewMockKVStorage(ctrl)
		kv.EXPECT().Get(gomock.Any()).Return([]byte("this is not a members value"), nil)
		_, err := NewRaftStorage(walDir, true, nodeId, kv, &storeSM{}, newSnapshotter(5, time.Second*10))
		require.NotNil(t, err)
		ctrl.Finish()
	}
}

func TestStorage(t *testing.T) {
	{
		os.RemoveAll(walDir)
		ctrl := gomock.NewController(t)
		kv := NewMockKVStorage(ctrl)
		kv.EXPECT().Get(gomock.Any()).Return(nil, nil)
		store, err := NewRaftStorage(walDir, true, nodeId, kv, &storeSM{}, newSnapshotter(5, time.Second*10))
		require.Nil(t, err)
		hs, cs, _ := store.InitialState()
		require.Equal(t, hs, pb.HardState{})
		require.Equal(t, len(cs.Voters), 0)
		require.Equal(t, len(cs.Learners), 0)
		store.Close()
		ctrl.Finish()
	}

	{
		os.RemoveAll(walDir)
		ctrl := gomock.NewController(t)
		memkv := make(map[string][]byte)
		kv := NewMockKVStorage(ctrl)
		kv.EXPECT().Get(gomock.Any()).DoAndReturn(func(key []byte) ([]byte, error) {
			val, hit := memkv[string(key)]
			if hit {
				return val, nil
			}
			return nil, nil
		})
		kv.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(func(key, value []byte) error {
			memkv[string(key)] = value
			return nil
		})
		store, err := NewRaftStorage(walDir, true, nodeId, kv, &storeSM{}, newSnapshotter(5, time.Second*10))
		require.Nil(t, err)
		var entries []pb.Entry
		for i := 0; i < 1000; i++ {
			entry := pb.Entry{
				Term:  uint64(i / 20),
				Index: uint64(i + 1),
				Type:  pb.EntryNormal,
				Data:  []byte("nfdujaiuerkljhoasiujkjfdoar"),
			}
			entries = append(entries, entry)
		}
		err = store.SaveEntries(entries)
		require.Nil(t, err)

		lastIndex, err := store.LastIndex()
		require.Nil(t, err)
		require.Equal(t, uint64(1000), lastIndex)

		firstIndex, err := store.FirstIndex()
		require.Nil(t, err)
		require.Equal(t, uint64(1), firstIndex)

		entries, err = store.Entries(1, 1001, math.MaxUint64)
		require.Nil(t, err)
		require.Equal(t, 1000, len(entries))

		err = store.Truncate(20)
		require.Nil(t, err)
		entries, err = store.Entries(21, 1001, math.MaxUint64)
		require.Nil(t, err)
		require.Equal(t, 980, len(entries))

		firstIndex, err = store.FirstIndex()
		require.Nil(t, err)
		require.Equal(t, uint64(21), firstIndex)

		term, err := store.Term(21)
		require.Nil(t, err)
		require.Equal(t, uint64(1), term)

		hs := pb.HardState{
			Term:   4,
			Vote:   1,
			Commit: 90,
		}
		err = store.SaveHardState(hs)
		require.Nil(t, err)

		store.SetApplied(uint64(90))
		require.Equal(t, uint64(90), store.Applied())

		err = store.ApplySnapshot(wal.Snapshot{Index: 90, Term: 4})
		require.Nil(t, err)
		firstIndex, err = store.FirstIndex()
		require.Nil(t, err)
		lastIndex, err = store.LastIndex()
		require.Nil(t, err)
		require.Equal(t, uint64(91), firstIndex)
		require.Equal(t, uint64(90), lastIndex)

		members := []Member{
			{1, "127.0.0.1:8080", false},
			{2, "127.0.0.1:8081", false},
			{3, "127.0.0.1:8082", false},
		}
		mbs := Members{
			Mbs: []Member{
				{1, "127.0.0.1:8080", false},
				{2, "127.0.0.1:8081", false},
				{3, "127.0.0.1:8082", false},
			},
		}
		err = store.SetMembers(mbs)
		require.Nil(t, err)

		mbs = store.Members()
		require.Equal(t, 3, len(mbs.Mbs))
		for i := 0; i < 3; i++ {
			require.Equal(t, members[i].Id, mbs.Mbs[i].Id)
			require.Equal(t, members[i].Host, mbs.Mbs[i].Host)
			require.Equal(t, members[i].Learner, mbs.Mbs[i].Learner)
		}

		_, err = store.Snapshot()
		require.NotNil(t, err)
		store.sm.(*storeSM).applied = 91
		_, err = store.Snapshot()
		require.NotNil(t, err)
		store.sm.(*storeSM).applied = 90
		snap, err := store.Snapshot()
		require.Nil(t, err)
		require.Equal(t, fmt.Sprintf("snapshot-%d", store.sm.(*storeSM).snapIndex), string(snap.Data))
		require.Equal(t, store.Applied(), snap.Metadata.Index)
		require.Equal(t, len(mbs.ConfState().Voters), len(snap.Metadata.ConfState.Voters))
		require.Equal(t, len(mbs.ConfState().Learners), len(snap.Metadata.ConfState.Learners))

		store.Close()
		ctrl.Finish()
	}
}
