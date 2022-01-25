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

	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/util/errors"
)

type Api interface {
	VolumeAlloc(ctx context.Context, host string, args *AllocVolsArgs) (ret []AllocRet, err error)
}

type Config struct {
	rpc.Config
}

type client struct {
	rpc.Client
}

func New(cfg *Config) Api {
	return &client{rpc.NewClient(&cfg.Config)}
}

type ListVolsArgs struct {
	CodeMode codemode.CodeMode `json:"code_mode"`
}

type VolumeList struct {
	Vids    []proto.Vid                  `json:"vids"`
	Volumes []clustermgr.AllocVolumeInfo `json:"volumes"`
}

type AllocVols struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	AllocRets []AllocRet      `json:"alloc_ret"`
}

type AllocRet struct {
	BidStart proto.BlobID `json:"bid_start"`
	BidEnd   proto.BlobID `json:"bid_end"`
	Vid      proto.Vid    `json:"vid"`
}

type AllocVolsArgs struct {
	Fsize    uint64            `json:"fsize"`
	CodeMode codemode.CodeMode `json:"code_mode"`
	BidCount uint64            `json:"bid_count"`
	Excludes []proto.Vid       `json:"excludes"`
	Discards []proto.Vid       `json:"discards"`
}

func (c *client) VolumeAlloc(ctx context.Context, host string, args *AllocVolsArgs) (ret []AllocRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	ret = make([]AllocRet, 0)
	err = c.PostWith(ctx, host+"/volume/alloc", &ret, args)
	if err != nil {
		span.Error(errors.Detail(err))
	}
	return
}
