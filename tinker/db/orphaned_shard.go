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

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/cubefs/blobstore/common/proto"
)

// IOrphanedShardTbl define the interface of save orphaned shard record
type IOrphanedShardTbl interface {
	SaveOrphanedShard(ctx context.Context, info ShardInfo) error
}

// OrphanedShardTbl orphaned shard table
type OrphanedShardTbl struct {
	coll *mongo.Collection
}

// ShardInfo shard info
type ShardInfo struct {
	ClusterID proto.ClusterID `bson:"cluster_id"`
	Vid       proto.Vid       `bson:"vid"`
	Bid       proto.BlobID    `bson:"bid"`
}

func openOrphanedShardTbl(coll *mongo.Collection) (IOrphanedShardTbl, error) {
	return &OrphanedShardTbl{coll: coll}, nil
}

// SaveOrphanedShard save orphaned shard info
func (t *OrphanedShardTbl) SaveOrphanedShard(ctx context.Context, info ShardInfo) error {
	_, err := t.coll.InsertOne(ctx, info)
	return err
}
