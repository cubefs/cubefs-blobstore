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

package base

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockKafkaOffsetTable struct {
	mockErr error
	offset  int64
}

func (m *mockKafkaOffsetTable) UpdateOffset(ctx context.Context, topic string, partition int32, off int64) error {
	if m.mockErr != nil {
		return m.mockErr
	}
	atomic.AddInt64(&m.offset, off)
	return nil
}

func (m *mockKafkaOffsetTable) GetOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	if m.mockErr != nil {
		return 0, m.mockErr
	}
	return atomic.LoadInt64(&m.offset), nil
}

func TestMgoOffAccessor_GetAndPut(t *testing.T) {
	tbl := &mockKafkaOffsetTable{}
	a := NewMgoOffAccessor(tbl)
	off, err := a.Get("my_topic", 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	LoopExecUntilSuccess(context.Background(), "update offset", func() error {
		return a.Put("my_topic", 1, 10)
	})

	off, err = a.Get("my_topic", 1)
	require.NoError(t, err)
	require.Equal(t, int64(10), off)
}
