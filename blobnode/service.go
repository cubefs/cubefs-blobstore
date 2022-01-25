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

package blobnode

import (
	"net/http"

	bnapi "github.com/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/blobstore/cmd"
	"github.com/cubefs/blobstore/common/config"
	"github.com/cubefs/blobstore/common/fileutil"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/util/log"
)

var (
	gService *Service
	conf     Config
)

func init() {
	mod := &cmd.Module{
		Name:       "BLOBNODE",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterModule(mod)
}

func initConfig(args []string) (cfg *cmd.Config, err error) {
	config.Init("f", "", "blobnode.conf")

	if err = config.Load(&conf); err != nil {
		return nil, err
	}
	if conf.FlockFilename == "" {
		conf.FlockFilename = "./blobnode.flock"
	}

	if _, err = fileutil.TryLockFile(conf.FlockFilename); err != nil {
		log.Errorf("Failed to flock, err: %v", err)
		return nil, err
	}

	return &conf.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	var err error
	gService, err = NewService(conf)
	if err != nil {
		log.Fatalf("Failed to new blobnode service, err: %v", err)
	}
	// register all self functions of service
	return NewHandler(gService), nil
}

func tearDown() {
	gService.Close()
}

func NewHandler(service *Service) *rpc.Router {
	r := rpc.New()

	rpc.RegisterArgsParser(&bnapi.DiskStatArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.DiskProbeArgs{}, "json")

	rpc.RegisterArgsParser(&bnapi.CreateChunkArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.ChangeChunkStatusArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.ListChunkArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.StatChunkArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.CompactChunkArgs{}, "json")

	rpc.RegisterArgsParser(&bnapi.GetShardArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.ListShardsArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.GetShardsArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.StatShardArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.DeleteShardArgs{}, "json")
	rpc.RegisterArgsParser(&bnapi.PutShardArgs{}, "json")

	rpc.Use(service.requestCounter) // first interceptor
	r.Handle(http.MethodGet, "/stat", service.Stat, rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/debug/stat", service.DebugStat, rpc.OptArgsQuery())

	r.Handle(http.MethodGet, "/disk/stat/diskid/:diskid", service.DiskStat_, rpc.OptArgsURI())
	r.Handle(http.MethodPost, "/disk/probe", service.DiskProbe, rpc.OptArgsBody())

	r.Handle(http.MethodPost, "/chunk/create/diskid/:diskid/vuid/:vuid", service.ChunkCreate_, rpc.OptArgsURI(), rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/chunk/release/diskid/:diskid/vuid/:vuid", service.ChunkRelease_, rpc.OptArgsURI(), rpc.OptArgsQuery())
	r.Handle(http.MethodPost, "/chunk/readonly/diskid/:diskid/vuid/:vuid", service.ChunkReadonly_, rpc.OptArgsURI())
	r.Handle(http.MethodPost, "/chunk/readwrite/diskid/:diskid/vuid/:vuid", service.ChunkReadwrite_, rpc.OptArgsURI())
	r.Handle(http.MethodGet, "/chunk/list/diskid/:diskid", service.ChunkList_, rpc.OptArgsURI())
	r.Handle(http.MethodGet, "/chunk/stat/diskid/:diskid/vuid/:vuid", service.ChunkStat_, rpc.OptArgsURI())
	r.Handle(http.MethodPost, "/chunk/compact/diskid/:diskid/vuid/:vuid", service.ChunkCompact_, rpc.OptArgsURI())

	r.Handle(http.MethodGet, "/shard/get/diskid/:diskid/vuid/:vuid/bid/:bid", service.ShardGet_, rpc.OptArgsURI(), rpc.OptArgsQuery())
	r.Handle(http.MethodGet, "/shard/list/diskid/:diskid/vuid/:vuid/startbid/:startbid/status/:status/count/:count", service.ShardList_, rpc.OptArgsURI())
	r.Handle(http.MethodPost, "/shards", service.GetShards, rpc.OptArgsBody())
	r.Handle(http.MethodGet, "/shard/stat/diskid/:diskid/vuid/:vuid/bid/:bid", service.ShardStat_, rpc.OptArgsURI())
	r.Handle(http.MethodPost, "/shard/markdelete/diskid/:diskid/vuid/:vuid/bid/:bid", service.ShardMarkdelete_, rpc.OptArgsURI())
	r.Handle(http.MethodPost, "/shard/delete/diskid/:diskid/vuid/:vuid/bid/:bid", service.ShardDelete_, rpc.OptArgsURI())
	r.Handle(http.MethodPost, "/shard/put/diskid/:diskid/vuid/:vuid/bid/:bid/size/:size", service.ShardPut_, rpc.OptArgsURI(), rpc.OptArgsQuery())

	return r
}
