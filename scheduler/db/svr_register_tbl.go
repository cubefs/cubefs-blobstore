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
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/cubefs/blobstore/common/proto"
)

// ISvrRegisterTbl define the interface of db use for service register
type ISvrRegisterTbl interface {
	Register(ctx context.Context, info *proto.SvrInfo) error
	Delete(ctx context.Context, host string) error
	Find(ctx context.Context, host string) (svr *proto.SvrInfo, err error)
	FindAll(ctx context.Context, module, idc string) (svrs []*proto.SvrInfo, err error)
}

// SvrRegisterTbl service register table
type SvrRegisterTbl struct {
	coll *mongo.Collection
}

// OpenSvrRegisterTbl open service register table
func OpenSvrRegisterTbl(coll *mongo.Collection) (ISvrRegisterTbl, error) {
	return &SvrRegisterTbl{
		coll: coll,
	}, nil
}

// Register register service
func (tbl *SvrRegisterTbl) Register(ctx context.Context, info *proto.SvrInfo) error {
	info.Ctime = time.Now().String()
	opts := options.Update().SetUpsert(true)

	_, err := tbl.coll.UpdateOne(ctx, bson.M{"_id": info.Host}, bson.M{"$set": info}, opts)
	return err
}

// Find find service by host
func (tbl *SvrRegisterTbl) Find(ctx context.Context, host string) (svr *proto.SvrInfo, err error) {
	err = tbl.coll.FindOne(ctx, bson.M{"_id": host}).Decode(&svr)
	return
}

// Delete delete service by host
func (tbl *SvrRegisterTbl) Delete(ctx context.Context, host string) error {
	_, err := tbl.coll.DeleteOne(ctx, bson.M{"_id": host})
	return err
}

// FindAll returns all service wit module and idc
func (tbl *SvrRegisterTbl) FindAll(ctx context.Context, module, idc string) (svrs []*proto.SvrInfo, err error) {
	filter := bson.M{}
	if module != "" {
		filter["module"] = module
	}
	if idc != "" {
		filter["idc"] = idc
	}

	cursor, err := tbl.coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	err = cursor.All(ctx, &svrs)
	return svrs, err
}
