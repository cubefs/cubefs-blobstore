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
	"encoding/json"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
)

// IRepairTaskTbl define the interface of db used by disk repair
type IRepairTaskTbl interface {
	Insert(ctx context.Context, t *proto.VolRepairTask) error
	Update(ctx context.Context, t *proto.VolRepairTask) error
	Find(ctx context.Context, taskID string) (task *proto.VolRepairTask, err error)
	FindByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.VolRepairTask, err error)
	FindAll(ctx context.Context) (tasks []*proto.VolRepairTask, err error)
	MarkDeleteByDiskID(ctx context.Context, diskID proto.DiskID) error
}

// RepairTaskTbl disk repair task table
type RepairTaskTbl struct {
	coll *mongo.Collection
	name string
}

// OpenRepairTaskTbl open disk repair task table
func OpenRepairTaskTbl(coll *mongo.Collection, name string) (IRepairTaskTbl, error) {
	tbl := &RepairTaskTbl{
		coll: coll,
		name: name,
	}
	err := ArchiveStoreInst().registerArchiveStore(name, tbl)
	return tbl, err
}

// Insert insert task
func (tbl *RepairTaskTbl) Insert(ctx context.Context, t *proto.VolRepairTask) error {
	t.Ctime = time.Now().String()
	t.MTime = time.Now().String()
	_, err := tbl.coll.InsertOne(ctx, t)
	return err
}

// Update update task
func (tbl *RepairTaskTbl) Update(ctx context.Context, t *proto.VolRepairTask) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("update repair task tbl task %+v", *t)

	t.MTime = time.Now().String()
	return tbl.coll.FindOneAndReplace(ctx, bson.M{"_id": t.TaskID}, t).Err()
}

// Find find task by taskID
func (tbl *RepairTaskTbl) Find(ctx context.Context, taskID string) (task *proto.VolRepairTask, err error) {
	err = tbl.coll.FindOne(ctx, bson.M{"_id": taskID, DeleteMark: bson.M{"$ne": true}}).Decode(&task)
	return
}

// FindByDiskID find task by diskID
func (tbl *RepairTaskTbl) FindByDiskID(ctx context.Context, diskID proto.DiskID) (tasks []*proto.VolRepairTask, err error) {
	cursor, err := tbl.coll.Find(ctx, bson.M{"repair_disk_id": diskID, DeleteMark: bson.M{"$ne": true}})
	if err != nil {
		return nil, err
	}
	err = cursor.All(ctx, &tasks)
	return tasks, err
}

// FindAll return all tasks
func (tbl *RepairTaskTbl) FindAll(ctx context.Context) (tasks []*proto.VolRepairTask, err error) {
	cursor, err := tbl.coll.Find(ctx, bson.M{DeleteMark: bson.M{"$ne": true}})
	if err != nil {
		return nil, err
	}
	err = cursor.All(ctx, &tasks)
	return tasks, err
}

// MarkDeleteByDiskID mark delete task by diskID
func (tbl *RepairTaskTbl) MarkDeleteByDiskID(ctx context.Context, diskID proto.DiskID) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("mark delete by disk_id %d", diskID)

	_, err := tbl.coll.UpdateMany(ctx, bson.M{"repair_disk_id": diskID}, deleteBson())
	return err
}

// QueryMarkDeleteTasks find mark delete tasks
func (tbl *RepairTaskTbl) QueryMarkDeleteTasks(ctx context.Context, delayMin int) (records []*ArchiveRecord, err error) {
	span := trace.SpanFromContextSafe(ctx)

	type VolRepairTaskEx struct {
		proto.VolRepairTask `bson:",inline"`
		DelTime             int64 `bson:"del_time"`
	}
	var tasks []*VolRepairTaskEx
	cursor, err := tbl.coll.Find(ctx, bson.M{DeleteMark: true})
	if err != nil {
		return nil, err
	}
	err = cursor.All(ctx, &tasks)
	if err != nil {
		return nil, err
	}

	for _, task := range tasks {
		content, err := json.MarshalIndent(task, "", "\t")
		if err != nil {
			span.Warnf("task_id %s marshal fail err:%+v", task.TaskID, err)
			continue
		}

		if inDelayTime(task.DelTime, delayMin) {
			span.Debugf("task_id %s is in delay time", task.TaskID)
			continue
		}

		r := &ArchiveRecord{
			TaskID:   task.TaskID,
			TaskType: tbl.Name(),
			Content:  string(content),
		}
		records = append(records, r)
	}
	return records, nil
}

// RemoveMarkDelete remove mark delete task by taskID
func (tbl *RepairTaskTbl) RemoveMarkDelete(ctx context.Context, taskID string) error {
	_, err := tbl.coll.DeleteOne(ctx, bson.M{"_id": taskID, DeleteMark: true})
	return err
}

// Name return repair table name
func (tbl *RepairTaskTbl) Name() string {
	return tbl.name
}
