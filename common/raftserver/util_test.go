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

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifier(t *testing.T) {
	nt := newNotifier()
	stopc := make(chan struct{})
	go func() {
		nt.notify(errors.New("error"))
	}()
	assert.NotNil(t, nt.wait(context.TODO(), stopc))

	nt = newNotifier()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	assert.NotNil(t, nt.wait(ctx, stopc))
	cancel()

	nt = newNotifier()
	go func() {
		nt.notify(nil)
	}()
	assert.Nil(t, nt.wait(context.TODO(), stopc))

	nt = newNotifier()
	go func() {
		close(stopc)
	}()
	assert.NotNil(t, nt.wait(context.TODO(), stopc))
}

func TestNormalEntryDecode(t *testing.T) {
	data := normalEntryEncode(100, []byte("123456"))
	id, b := normalEntryDecode(data)
	require.Equal(t, uint64(100), id)
	require.Equal(t, []byte("123456"), b)
}
