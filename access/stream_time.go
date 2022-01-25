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

package access

import (
	"fmt"
	"sync/atomic"
	"time"
)

type times struct {
	putN     int64 // PUT bytes
	putRead  int64 // PUT read from client
	putWrite int64 // PUT write to blobnode
	getN     int64 // GET bytes
	getRead  int64 // GET read from blobnode
	getWrite int64 // GET write to client
}

func (t *times) AddPutN(n int) {
	atomic.AddInt64(&t.putN, int64(n))
}

func (t *times) AddPutRead(start time.Time) {
	atomic.AddInt64(&t.putRead, int64(time.Since(start)))
}

func (t *times) AddPutWrite(start time.Time) {
	atomic.AddInt64(&t.putWrite, int64(time.Since(start)))
}

func (t *times) PutLogs() []string {
	n := atomic.LoadInt64(&t.putN)
	r := atomic.LoadInt64(&t.putRead)
	w := atomic.LoadInt64(&t.putWrite)
	return []string{
		fmt.Sprintf("put_%d_r_%d_w_%d", n, r/1e6, w/1e6),
	}
}

func (t *times) AddGetN(n int) {
	atomic.AddInt64(&t.getN, int64(n))
}

func (t *times) AddGetRead(start time.Time) {
	atomic.AddInt64(&t.getRead, int64(time.Since(start)))
}

func (t *times) AddGetWrite(start time.Time) {
	atomic.AddInt64(&t.getWrite, int64(time.Since(start)))
}

func (t *times) GetLogs() []string {
	n := atomic.LoadInt64(&t.getN)
	r := atomic.LoadInt64(&t.getRead)
	w := atomic.LoadInt64(&t.getWrite)
	return []string{
		fmt.Sprintf("get_%d_r_%d_w_%d", n, r/1e6, w/1e6),
	}
}
