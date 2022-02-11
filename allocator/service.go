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

package allocator

import (
	"context"
	"net/http"

	"github.com/cubefs/blobstore/api/allocator"
	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/cmd"
	"github.com/cubefs/blobstore/common/config"
	apierrors "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/trace"
)

var (
	service *Service
	conf    Config
)

const (
	TickInterval   = 1
	HeartbeatTicks = 30
	ExpiresTicks   = 60
)

type Config struct {
	cmd.Config
	BlobConfig
	VolConfig
	Cluster clustermgr.Config `json:"clustermgr"`
}

type Service struct {
	volumeMgr VolumeMgr
}

func init() {
	mod := &cmd.Module{
		Name:       "ALLOCATOR",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterGracefulModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "allocator.conf")

	if err := config.Load(&conf); err != nil {
		return nil, err
	}

	return &conf.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	cmcli := clustermgr.New(&conf.Cluster)
	service = New(cmcli, conf)
	return NewHandler(service), nil
}

func tearDown() {
}

func NewHandler(service *Service) *rpc.Router {
	router := rpc.New()
	rpc.RegisterArgsParser(&allocator.ListVolsArgs{}, "json")

	// POST /volume/alloc
	// request  body:  json
	// response body:  json
	router.Handle(http.MethodPost, "/volume/alloc", service.Alloc, rpc.OptArgsBody())

	// GET /volume/list?code_mode={code_mode}
	router.Handle(http.MethodGet, "/volume/list", service.List, rpc.OptArgsQuery())

	return router
}

func New(client clustermgr.APIAllocator, cfg Config) *Service {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	node := clustermgr.ServiceNode{
		ClusterID: cfg.ClusterID,
		Name:      proto.ServiceNameAllocator,
		Host:      cfg.Host,
		Idc:       cfg.Idc,
	}
	err := client.RegisterService(ctx, node, TickInterval, HeartbeatTicks, ExpiresTicks)
	if err != nil {
		span.Fatalf("allocator register to CMClient error:%v", err)
	}
	volumeMgr, err := NewVolumeMgr(ctx, cfg.BlobConfig, cfg.VolConfig, client)
	if err != nil {
		span.Fatalf("fail to new volumeMgr, error:%v", err)
	}
	s := &Service{
		volumeMgr: volumeMgr,
	}
	return s
}

// Alloc vids and bids from allocator
func (s *Service) Alloc(c *rpc.Context) {
	args := new(allocator.AllocVolsArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if args.BidCount == 0 || args.Fsize == 0 {
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	resp, err := s.volumeMgr.Alloc(ctx, args)
	if err != nil {
		c.RespondError(err)
		return
	}
	c.RespondJSON(resp)
	span.Infof("alloc request args: %v, resp: %v", args, resp)
}

// List the volumes in this allocator
func (s *Service) List(c *rpc.Context) {
	args := new(allocator.ListVolsArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if !args.CodeMode.IsValid() {
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	vids, volumes, err := s.volumeMgr.List(ctx, args.CodeMode)
	if err != nil {
		c.RespondError(err)
		return
	}
	resp := &allocator.VolumeList{
		Vids:    vids,
		Volumes: volumes,
	}
	c.RespondJSON(resp)
	span.Infof("list request args: %v, resp: %v", args, resp)
}
