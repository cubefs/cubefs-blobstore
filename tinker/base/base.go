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
	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/tinker/client"
)

// IVolumeCache define the interface used for volume cache manager
type IVolumeCache interface {
	Update(vid proto.Vid) (*client.VolInfo, error)
	Get(vid proto.Vid) (*client.VolInfo, error)
	Load() error
}

// IBaseMgr define the base interface used for delete and repair manager
type IBaseMgr interface {
	Enabled() bool
	RunTask()
	GetTaskStats() (success [counter.SLOT]int, failed [counter.SLOT]int)
	GetErrorStats() (errStats []string, totalErrCnt uint64)
}
