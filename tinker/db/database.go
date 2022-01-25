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

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"

	"github.com/cubefs/blobstore/common/mongoutil"
)

// Database database with table and client
type Database struct {
	DB                 *mongo.Database
	OrphanedShardTable IOrphanedShardTbl
	KafkaOffsetTable   IKafkaOffsetTbl
}

// Config database config
type Config struct {
	Mongo                mongoutil.Config `json:"mongo"`
	DBName               string           `json:"db_name"`
	OrphanedShardTblName string           `json:"orphaned_shard_tbl_name"`
	KafkaOffsetTblName   string           `json:"kafka_offset_tbl_name"`
}

// OpenDatabase open database wit config
func OpenDatabase(cfg Config) (*Database, error) {
	client, err := mongoutil.GetClient(cfg.Mongo)
	if err != nil {
		return nil, err
	}
	db0 := client.Database(cfg.DBName)

	db := new(Database)
	db.DB = db0

	db.OrphanedShardTable, err = openOrphanedShardTbl(mustCreateCollection(db0, cfg.OrphanedShardTblName))
	if err != nil {
		return nil, err
	}

	db.KafkaOffsetTable, err = openKafkaOffsetTbl(mustCreateCollection(db0, cfg.KafkaOffsetTblName))
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
