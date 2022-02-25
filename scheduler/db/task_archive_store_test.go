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

package db

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/log"
)

func init() {
	log.SetOutputLevel(log.Lfatal)
}

type mockSrcTbl struct {
	mu        sync.Mutex
	records   []*ArchiveRecord
	name      string
	queryErr  error
	removeErr error
}

func (m *mockSrcTbl) QueryMarkDeleteTasks(ctx context.Context, delayMin int) (records []*ArchiveRecord, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.records, m.queryErr
}

func (m *mockSrcTbl) RemoveMarkDelete(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var records []*ArchiveRecord
	for _, r := range m.records {
		if r.TaskID != taskID {
			records = append(records, r)
		}
	}
	m.records = records
	return m.removeErr
}

func (m *mockSrcTbl) Name() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.name
}

type mockArchiveStore struct {
	mu      sync.Mutex
	records []*ArchiveRecord
}

func (m *mockArchiveStore) Insert(ctx context.Context, record *ArchiveRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, record)
	return nil
}

func (m *mockArchiveStore) FindTask(ctx context.Context, taskID string) (record *ArchiveRecord, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.records {
		if r.TaskID == taskID {
			return r, nil
		}
	}
	return nil, base.ErrNoDocuments
}

func TestArchiveStore(t *testing.T) {
	r1 := ArchiveRecord{
		TaskID:   "task1",
		TaskType: "mock1",
		Content:  "str1",
	}
	r2 := ArchiveRecord{
		TaskID:   "task2",
		TaskType: "mock2",
		Content:  "str2",
	}
	r3 := ArchiveRecord{
		TaskID:   "task3",
		TaskType: "mock3",
		Content:  "str3",
	}
	r4 := ArchiveRecord{
		TaskID:   "task4",
		TaskType: "mock4",
		Content:  "str4",
	}

	records1 := []*ArchiveRecord{&r1, &r2}
	records2 := []*ArchiveRecord{&r3}
	records3 := []*ArchiveRecord{&r4}

	srcTbl1 := &mockSrcTbl{
		records: records1,
		name:    "type1",
	}

	srcTbl2 := &mockSrcTbl{
		records:  records2,
		name:     "type2",
		queryErr: errors.New("fake error"),
	}

	srcTbl3 := &mockSrcTbl{
		records:   records3,
		name:      "type3",
		removeErr: errors.New("fake error"),
	}

	err := ArchiveStoreInst().registerArchiveStore(srcTbl1.Name(), srcTbl1)
	require.NoError(t, err)

	ArchiveStoreInst().archTbl = &mockArchiveStore{}
	ArchiveStoreInst().run()

	t.Logf("len arch records %d", len(ArchiveStoreInst().archTbl.(*mockArchiveStore).records))
	require.Equal(t, 2, len(ArchiveStoreInst().archTbl.(*mockArchiveStore).records))

	err = ArchiveStoreInst().registerArchiveStore(srcTbl2.Name(), srcTbl2)
	require.NoError(t, err)

	err = ArchiveStoreInst().registerArchiveStore(srcTbl3.Name(), srcTbl3)
	require.NoError(t, err)

	err = ArchiveStoreInst().registerArchiveStore(srcTbl3.Name(), srcTbl3)
	require.Error(t, err)
	require.EqualError(t, errors.New("key has exist"), err.Error())

	ArchiveStoreInst().run()

	t.Logf("len arch records %d", len(ArchiveStoreInst().archTbl.(*mockArchiveStore).records))
	require.Equal(t, 3, len(ArchiveStoreInst().archTbl.(*mockArchiveStore).records))
}

func TestInDelayTime(t *testing.T) {
	require.Equal(t, false, inDelayTime(0, 1))
}

func TestDeleteBson(t *testing.T) {
	t.Log(deleteBson())
}
