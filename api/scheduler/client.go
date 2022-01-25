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

package scheduler

import (
	"context"

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
)

type IScheduler interface {
	AcquireTask(ctx context.Context, args *AcquireArgs) (ret *WorkerTask, err error)
	AcquireInspectTask(ctx context.Context) (ret *WorkerInspectTask, err error)
	// report alive tasks
	RenewalTask(ctx context.Context, args *TaskRenewalArgs) (ret *TaskRenewalRet, err error)
	ReportTask(ctx context.Context, args *TaskReportArgs) (err error)
	ReclaimTask(ctx context.Context, args *ReclaimTaskArgs) (err error)
	CancelTask(ctx context.Context, args *CancelTaskArgs) (err error)
	CompleteTask(ctx context.Context, args *CompleteTaskArgs) (err error)
	CompleteInspect(ctx context.Context, args *CompleteInspectArgs) (err error)
	RegisterService(ctx context.Context, args *RegisterServiceArgs) (err error)
	DeleteService(ctx context.Context, args *DeleteServiceArgs) (err error)
	GetService(ctx context.Context, host string) (ret *proto.SvrInfo, err error)
	ListServices(ctx context.Context, args *ListServicesArgs) (ret []*proto.SvrInfo, err error)

	// stats
	RepairTaskDetail(ctx context.Context, args *TaskStatArgs) (ret RepairTaskDetail, err error)
	BalanceTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error)
	DropTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error)
	ManualMigrateTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error)
	Stats(ctx context.Context) (ret TasksStat, err error)

	// add manual migrate task
	AddManualMigrateTask(ctx context.Context, args *AddManualMigrateArgs) (err error)
}

type Config struct {
	Host string `json:"host"`
	rpc.Config
}

type client struct {
	Host string
	rpc.Client
}

func New(cfg *Config) IScheduler {
	return &client{
		Host:   cfg.Host,
		Client: rpc.NewClient(&cfg.Config),
	}
}
