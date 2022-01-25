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

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// IKafkaOffsetTbl define interface of kafka offset table use by delete or repair message consume
type IKafkaOffsetTbl interface {
	UpdateOffset(ctx context.Context, topic string, partition int32, off int64) error
	GetOffset(ctx context.Context, topic string, partition int32) (int64, error)
}

// KafkaOffsetTbl records consumer offset
type KafkaOffsetTbl struct {
	coll *mongo.Collection
}

// OffsetInfo offset info with topic partition and offset
type OffsetInfo struct {
	Topic     string `bson:"topic"`
	Partition int32  `bson:"partition"`
	Offset    int64  `bson:"offset"`
}

func openKafkaOffsetTbl(coll *mongo.Collection) (IKafkaOffsetTbl, error) {
	return &KafkaOffsetTbl{coll: coll}, nil
}

// UpdateOffset update consume offset
func (t *KafkaOffsetTbl) UpdateOffset(ctx context.Context, topic string, partition int32, off int64) error {
	info := OffsetInfo{Topic: topic, Partition: partition, Offset: off}
	selector := bson.M{"topic": topic, "partition": partition}

	update := bson.M{
		"$set": info,
	}
	opts := options.Update().SetUpsert(true)
	_, err := t.coll.UpdateOne(ctx, selector, update, opts)
	return err
}

// GetOffset get consume offset
func (t *KafkaOffsetTbl) GetOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	infos := OffsetInfo{}
	selector := bson.M{"topic": topic, "partition": partition}
	err := t.coll.FindOne(ctx, &selector).Decode(&infos)
	return infos.Offset, err
}
