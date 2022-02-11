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

package client

import (
	"context"

	"github.com/cubefs/blobstore/api/worker"
	"github.com/cubefs/blobstore/common/proto"
)

// IWorker define the interface of worker used by tinker
type IWorker interface {
	RepairShard(ctx context.Context, host string, task proto.ShardRepairTask) error
}

// workerClient worker client
type workerClient struct {
	client worker.IWorker
}

// NewWorkerClient returns worker client
func NewWorkerClient(config *worker.Config) IWorker {
	return &workerClient{client: worker.New(config)}
}

// RepairShard send repair shard message to worker
func (c *workerClient) RepairShard(ctx context.Context, host string, task proto.ShardRepairTask) error {
	return c.client.RepairShard(ctx, host, &worker.ShardRepairArgs{
		Task: task,
	})
}
