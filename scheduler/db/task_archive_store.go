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
	"sync"
	"time"

	"github.com/globalsign/mgo/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/cubefs/blobstore/common/mongoutil"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/blobstore/util/errors"
)

//duties:transfer the deleted records to the archive table

// IArchiveTbl define the interface of db use by archive
type IArchiveTbl interface {
	Insert(ctx context.Context, record *ArchiveRecord) error
	FindTask(ctx context.Context, taskID string) (record *ArchiveRecord, err error)
}

// IRecordSrcTbl define the interface of source record table used by archive
type IRecordSrcTbl interface {
	QueryMarkDeleteTasks(ctx context.Context, delayMin int) (records []*ArchiveRecord, err error)
	RemoveMarkDelete(ctx context.Context, taskID string) error
	Name() string
}

// ArchiveRecord archive record
type ArchiveRecord struct {
	TaskID      string `bson:"_id"`
	TaskType    string `bson:"task_type"`
	Content     string `bson:"content"`
	ArchiveTime string `bson:"archive_time"`
}

type archiveTbl struct {
	coll *mongo.Collection
}

func openArchiveTbl(coll *mongo.Collection) (IArchiveTbl, error) {
	return &archiveTbl{
		coll: coll,
	}, nil
}

// Insert insert record
func (tbl *archiveTbl) Insert(ctx context.Context, record *ArchiveRecord) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("archiveTbl:insert task %s", record.TaskID)

	record.ArchiveTime = time.Now().String()
	_, err := tbl.coll.InsertOne(ctx, record)
	return err
}

// FindTask find task by taskID
func (tbl *archiveTbl) FindTask(ctx context.Context, taskID string) (record *ArchiveRecord, err error) {
	err = tbl.coll.FindOne(ctx, bson.M{"_id": taskID}).Decode(&record)
	return
}

// ArchiveStoreConfig archive store config
type ArchiveStoreConfig struct {
	Mongo              mongoutil.Config `json:"mongo"`
	DBName             string           `json:"db_name"`
	TblName            string           `json:"tbl_name"`
	ArchiveIntervalMin int              `json:"archive_interval_min"`
	// recode will archive util delete ArchiveDelayMin
	ArchiveDelayMin int `json:"archive_delay_min"`
}

// ArchiveStore archive store
type ArchiveStore struct {
	archTbl IArchiveTbl

	mu      sync.Mutex
	srcTbls map[string]IRecordSrcTbl

	archiveDelayMin int
}

// InitAndStart init archive service
func (store *ArchiveStore) InitAndStart(cfg *ArchiveStoreConfig) error {
	client, err := mongoutil.GetClient(cfg.Mongo)
	if err != nil {
		return err
	}

	store.archTbl, err = openArchiveTbl(mustCreateCollection(client.Database(cfg.DBName), cfg.TblName))
	if err != nil {
		return err
	}

	store.archiveDelayMin = cfg.ArchiveDelayMin

	go func() {
		for {
			store.run()
			time.Sleep(time.Duration(cfg.ArchiveIntervalMin) * time.Minute)
		}
	}()
	return nil
}

func (store *ArchiveStore) run() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "ArchiveStore")

	store.mu.Lock()
	srcTbls := store.srcTbls
	store.mu.Unlock()

	for _, src := range srcTbls {
		store.store(ctx, src)
	}
}

func (store *ArchiveStore) store(ctx context.Context, src IRecordSrcTbl) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("archive store src %s", src.Name())

	deleteTasks, err := src.QueryMarkDeleteTasks(ctx, store.archiveDelayMin)
	if err != nil {
		span.Errorf("%s query delete tasks fail err:%+v", src.Name(), err)
		return
	}

	var shouldRemove []*ArchiveRecord
	for _, task := range deleteTasks {
		archivedRecord, err := store.archTbl.FindTask(ctx, task.TaskID)
		if archivedRecord != nil {
			span.Infof("task_id %s has archived", task.TaskID)
			shouldRemove = append(shouldRemove, task)
			continue
		}

		if err != nil && err != base.ErrNoDocuments {
			span.Errorf("task_id %s find in archTbl fail err:%+v", task.TaskID, err)
			continue
		}

		err = store.archTbl.Insert(ctx, task)
		if err != nil {
			span.Errorf("task_id %s insert into archive table fail err:%+v", task.TaskID, err)
			continue
		}

		shouldRemove = append(shouldRemove, task)
	}

	for _, task := range shouldRemove {
		archivedRecord, err := store.archTbl.FindTask(ctx, task.TaskID)
		if archivedRecord == nil {
			span.Warnf("task_id %s not find in archTbl fail err:%+v", task.TaskID, err)
			continue
		}

		err = src.RemoveMarkDelete(ctx, task.TaskID)
		if err != nil {
			span.Errorf("remove task_id %s fail err:%+v", task.TaskID, err)
			continue
		}
		span.Infof("task_id %s has archive store success!", task.TaskID)
	}
}

func (store *ArchiveStore) registerArchiveStore(key string, src IRecordSrcTbl) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, ok := store.srcTbls[key]; ok {
		return errors.New("key has exist")
	}
	store.srcTbls[key] = src
	return nil
}

var (
	store        *ArchiveStore
	newStoreOnce sync.Once
)

// ArchiveStoreInst make sure only one instance in global
func ArchiveStoreInst() *ArchiveStore {
	newStoreOnce.Do(func() {
		store = &ArchiveStore{
			srcTbls: make(map[string]IRecordSrcTbl),
		}
	})
	return store
}
