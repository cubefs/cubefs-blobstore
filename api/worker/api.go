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

package worker

import (
	"context"
	"fmt"

	"github.com/cubefs/blobstore/common/proto"
)

type ShardRepairArgs struct {
	Task proto.ShardRepairTask `json:"task"`
}

func (c *client) RepairShard(ctx context.Context, host string, args *ShardRepairArgs) (err error) {
	urlStr := fmt.Sprintf("%v/shard/repair", host)
	err = c.PostWith(ctx, urlStr, nil, args)
	return
}

type Stats struct {
	CancelCount  string `json:"cancel_count"`
	ReclaimCount string `json:"reclaim_count"`
}

func (c *client) Stats(ctx context.Context, host string) (ret Stats, err error) {
	err = c.GetWith(ctx, host+"/stats", &ret)
	return
}
