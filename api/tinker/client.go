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

package tinker

import (
	"context"
	"fmt"

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
)

type VolInfo struct {
	Vid proto.Vid `json:"vid"`
}

type Config struct {
	rpc.Config
}

type VolInfoUpdater interface {
	UpdateVolInfo(ctx context.Context, host string, vid proto.Vid) error
}

type Client struct {
	rpc.Client
}

func New(cfg *Config) *Client {
	return &Client{rpc.NewClient(&cfg.Config)}
}

func (c *Client) UpdateVolInfo(ctx context.Context, host string, vid proto.Vid) error {
	urlStr := fmt.Sprintf("%v/update/vol", host)
	err := c.PostWith(ctx, urlStr, nil, VolInfo{Vid: vid})

	return err
}

type StatGetter interface {
	Stats(ctx context.Context, host string) (ret Stats, err error)
}

type Stat struct {
	Switch        string   `json:"switch"`
	SuccessPerMin string   `json:"success_per_min"`
	FailedPerMin  string   `json:"failed_per_min"`
	TotalErrCnt   uint64   `json:"total_err_cnt"`
	ErrStats      []string `json:"err_stats"`
}

type Stats struct {
	ShardRepair Stat `json:"shard_repair"`
	BlobDelete  Stat `json:"blob_delete"`
}

func (c *Client) Stats(ctx context.Context, host string) (ret Stats, err error) {
	err = c.GetWith(ctx, host+"/stats", &ret)
	return
}
