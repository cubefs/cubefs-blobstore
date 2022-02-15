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
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"

	"github.com/cubefs/blobstore/common/mongoutil"
)

// IDatabase all table database.
type IDatabase interface {
	IKafkaOffsetTable
	IOrphanShardTable
}

type database struct {
	db *mongo.Database
	IKafkaOffsetTable
	IOrphanShardTable
}

// Config database config
type Config struct {
	Mongo            mongoutil.Config `json:"mongo"`
	DBName           string           `json:"db_name"`
	OrphanShardTable string           `json:"orphaned_shard_tbl_name"` // TODO: orphan
	KafkaOffsetTable string           `json:"kafka_offset_tbl_name"`
}

// OpenDatabase open database with all table.
func OpenDatabase(cfg Config) (IDatabase, error) {
	client, err := mongoutil.GetClient(cfg.Mongo)
	if err != nil {
		return nil, err
	}
	db := client.Database(cfg.DBName)

	tables := &database{db: db}
	tables.IKafkaOffsetTable = openKafkaOffsetTable(mustCreateCollection(db, cfg.KafkaOffsetTable))
	tables.IOrphanShardTable = openOrphanedShardTable(mustCreateCollection(db, cfg.OrphanShardTable))
	return tables, nil
}

func mustCreateCollection(db *mongo.Database, collName string) *mongo.Collection {
	err := db.RunCommand(context.Background(), bsonx.Doc{{Key: "create", Value: bsonx.String(collName)}}).Err()
	if err == nil || strings.Contains(err.Error(), "already exists") {
		return db.Collection(collName)
	}
	panic("create collection error: " + err.Error())
}
