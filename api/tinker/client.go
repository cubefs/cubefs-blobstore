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

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
)

// defined http server path.
const (
	PathUpdateVolume = "/update/vol"
	PathStats        = "/stats"
)

// UpdateVolumeArgs argument of volume to update.
type UpdateVolumeArgs struct {
	Vid proto.Vid `json:"vid"`
}

// Config tinker client configuration.
type Config struct {
	rpc.Config
}

// ITinker api of tinker service.
type ITinker interface {
	UpdateVolume(ctx context.Context, host string, vid proto.Vid) error
	Stats(ctx context.Context, host string) (Stats, error)
}

type client struct {
	rpc.Client
}

// New returns client of tinker.
func New(cfg *Config) ITinker {
	return &client{rpc.NewClient(&cfg.Config)}
}

func (c *client) UpdateVolume(ctx context.Context, host string, vid proto.Vid) error {
	return c.PostWith(ctx, host+PathUpdateVolume, nil, UpdateVolumeArgs{Vid: vid})
}

// Stat stat
type Stat struct {
	Switch        string   `json:"switch"`
	SuccessPerMin string   `json:"success_per_min"`
	FailedPerMin  string   `json:"failed_per_min"`
	TotalErrCnt   uint64   `json:"total_err_cnt"`
	ErrStats      []string `json:"err_stats"`
}

// Stats all stat
type Stats struct {
	ShardRepair Stat `json:"shard_repair"`
	BlobDelete  Stat `json:"blob_delete"`
}

func (c *client) Stats(ctx context.Context, host string) (stats Stats, err error) {
	err = c.GetWith(ctx, host+PathStats, &stats)
	return
}
