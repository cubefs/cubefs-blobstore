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

	"github.com/cubefs/blobstore/tinker/db"
)

// IOffsetAccessor define the interface of offsetAccessor
type IOffsetAccessor interface {
	Put(topic string, partition int32, off int64) error
	Get(topic string, partition int32) (int64, error)
}

type mgoOffsetAccessor struct {
	offsetTbl db.IKafkaOffsetTbl
}

// NewMgoOffAccessor returns MgoOffAccessor
func NewMgoOffAccessor(coll db.IKafkaOffsetTbl) IOffsetAccessor {
	return mgoOffsetAccessor{offsetTbl: coll}
}

// Put put consume offset
func (a mgoOffsetAccessor) Put(topic string, partition int32, offset int64) error {
	return a.offsetTbl.UpdateOffset(context.Background(), topic, partition, offset)
}

// Get returns consume offset
func (a mgoOffsetAccessor) Get(topic string, partition int32) (int64, error) {
	return a.offsetTbl.GetOffset(context.Background(), topic, partition)
}
