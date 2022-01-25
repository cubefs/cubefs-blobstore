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
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"

	"github.com/cubefs/blobstore/common/mongoutil"
	"github.com/cubefs/blobstore/common/proto"
)

const (
	// DeleteMark for mark delete
	DeleteMark = "delete_mark"
)

// Config mongo config
type Config struct {
	Mongo                    mongoutil.Config `json:"mongo"`
	DBName                   string           `json:"db_name"`
	BalanceTblName           string           `json:"balance_tbl_name"`
	DiskDropTblName          string           `json:"disk_drop_tbl_name"`
	ManualMigrateTblName     string           `json:"manual_migrate_tbl_name"`
	RepairTblName            string           `json:"repair_tbl_name"`
	InspectCheckPointTblName string           `json:"inspect_checkpoint_tbl_name"`
	SvrRegisterTblName       string           `json:"svr_register_tbl_name"`
}

// Database used for database operate
type Database struct {
	DB *mongo.Database

	BalanceTbl           IMigrateTaskTbl
	DiskDropTbl          IMigrateTaskTbl
	ManualMigrateTbl     IMigrateTaskTbl
	RepairTaskTbl        IRepairTaskTbl
	InspectCheckPointTbl IInspectCheckPointTbl
	SvrRegisterTbl       ISvrRegisterTbl
}

// OpenDatabase open database
func OpenDatabase(conf *Config, archiveCfg *ArchiveStoreConfig) (*Database, error) {
	db, err := openTaskDataBase(conf)
	if err != nil {
		return db, err
	}

	if archiveCfg == nil {
		return db, nil
	}
	err = ArchiveStoreInst().InitAndStart(archiveCfg)

	return db, err
}

func openTaskDataBase(conf *Config) (*Database, error) {
	client, err := mongoutil.GetClient(conf.Mongo)
	if err != nil {
		return nil, err
	}
	db0 := client.Database(conf.DBName)

	db := new(Database)
	db.DB = db0

	db.BalanceTbl, err = OpenMigrateTbl(
		mustCreateCollection(db0, conf.BalanceTblName),
		proto.BalanceTaskType)
	if err != nil {
		return nil, err
	}

	db.DiskDropTbl, err = OpenMigrateTbl(
		mustCreateCollection(db0, conf.DiskDropTblName),
		proto.DiskDropTaskType)
	if err != nil {
		return nil, err
	}

	db.ManualMigrateTbl, err = OpenMigrateTbl(
		mustCreateCollection(db0, conf.ManualMigrateTblName),
		proto.ManualMigrateType)
	if err != nil {
		return nil, err
	}

	db.RepairTaskTbl, err = OpenRepairTaskTbl(
		mustCreateCollection(db0, conf.RepairTblName),
		proto.RepairTaskType)
	if err != nil {
		return nil, err
	}

	db.InspectCheckPointTbl, err = OpenInspectCheckPointTbl(mustCreateCollection(db0, conf.InspectCheckPointTblName))
	if err != nil {
		return nil, err
	}

	db.SvrRegisterTbl, err = OpenSvrRegisterTbl(mustCreateCollection(db0, conf.SvrRegisterTblName))
	if err != nil {
		return nil, err
	}

	return db, nil
}

func mustCreateCollection(db *mongo.Database, collName string) *mongo.Collection {
	err := db.RunCommand(context.Background(), bsonx.Doc{{Key: "create", Value: bsonx.String(collName)}}).Err()
	if err == nil || strings.Contains(err.Error(), "already exists") {
		return db.Collection(collName)
	}
	panic(fmt.Sprintf("create collection error: %v", err))
}

func deleteBson() bson.M {
	return bson.M{"$set": bson.M{DeleteMark: true, "del_time": time.Now().Unix()}}
}

func inDelayTime(delTime int64, delayMin int) bool {
	now := time.Now()
	return now.Sub(time.Unix(delTime, 0)) <= time.Duration(delayMin)*time.Minute
}
