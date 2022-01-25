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
	"encoding/binary"
)

type notifier chan error

func newNotifier() notifier {
	return make(chan error, 1)
}

func (nc notifier) notify(err error) {
	select {
	case nc <- err:
	default:
	}
}

func (nc notifier) wait(ctx context.Context, stopc <-chan struct{}) error {
	select {
	case err := <-nc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-stopc:
		return ErrStopped
	}
}

type readIndexNotifier struct {
	ch  chan struct{}
	err error
}

func newReadIndexNotifier() *readIndexNotifier {
	return &readIndexNotifier{
		ch: make(chan struct{}),
	}
}

func (nr *readIndexNotifier) Notify(err error) {
	nr.err = err
	close(nr.ch)
}

func (nr *readIndexNotifier) Wait(ctx context.Context, stopc <-chan struct{}) error {
	select {
	case <-nr.ch:
		return nr.err
	case <-ctx.Done():
		return ctx.Err()
	case <-stopc:
		return ErrStopped
	}
}

func normalEntryEncode(id uint64, data []byte) []byte {
	b := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(b[0:8], id)
	copy(b[8:], data)
	return b
}

func normalEntryDecode(data []byte) (uint64, []byte) {
	id := binary.BigEndian.Uint64(data[0:8])
	return id, data[8:]
}
