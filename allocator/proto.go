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

package allocator

import (
	"sync"

	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/common/proto"
)

const entryNum = 32

type volume struct {
	clustermgr.AllocVolumeInfo
	isDeleted bool
	mu        sync.RWMutex
}

type volumes struct {
	num   uint32
	m     map[uint32]map[proto.Vid]*volume
	locks map[uint32]*sync.RWMutex
}

func (s *volumes) Get(vid proto.Vid) (*volume, bool) {
	idx := uint32(vid) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	if _, ok := s.m[idx][vid]; !ok {
		return nil, false
	}
	return s.m[idx][vid], true
}

func (s *volumes) Put(vol *volume) {
	idx := uint32(vol.Vid) % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	_, ok := s.m[idx][vol.Vid]
	// volume already exist
	if !ok {
		s.m[idx][vol.Vid] = vol
	}
}

func (s *volumes) Delete(vid proto.Vid) {
	idx := uint32(vid) % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	_, ok := s.m[idx][vid]
	// volume already exist
	if ok {
		delete(s.m[idx], vid)
	}
}

func (s *volumes) Range(f func(vol *volume)) {
	for i := uint32(0); i < s.num; i++ {
		s.locks[i].RLock()
		for vid := range s.m[i] {
			f(s.m[i][vid])
		}
		s.locks[i].RUnlock()
	}
}
