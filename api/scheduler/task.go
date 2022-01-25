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

	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/proto"
)

type AcquireArgs struct {
	IDC string `json:"idc"`
}

type WorkerTask struct {
	TaskType      string               `json:"task_type"`      // task type
	Repair        *proto.VolRepairTask `json:"repair"`         // repair task
	Balance       *proto.MigrateTask   `json:"balance"`        // balance task
	DiskDrop      *proto.MigrateTask   `json:"disk_drop"`      // disk drop task
	ManualMigrate *proto.MigrateTask   `json:"manual_migrate"` // manual migrate task
}

func (task *WorkerTask) IsValid() bool {
	var (
		mode        codemode.CodeMode
		destination proto.VunitLocation
		srcs        []proto.VunitLocation
	)
	switch task.TaskType {
	case proto.RepairTaskType:
		mode = task.Repair.CodeMode
		destination = task.Repair.Destination
		srcs = task.Repair.Sources
	case proto.BalanceTaskType:
		mode = task.Balance.CodeMode
		destination = task.Balance.Destination
		srcs = task.Balance.Sources
	case proto.DiskDropTaskType:
		mode = task.DiskDrop.CodeMode
		destination = task.DiskDrop.Destination
		srcs = task.DiskDrop.Sources
	case proto.ManualMigrateType:
		mode = task.ManualMigrate.CodeMode
		destination = task.ManualMigrate.Destination
		srcs = task.ManualMigrate.Sources
	default:
		return false
	}

	if !mode.IsValid() {
		return false
	}
	// check destination
	if !proto.CheckVunitLocations([]proto.VunitLocation{destination}) {
		return false
	}
	// check sources
	if !proto.CheckVunitLocations(srcs) {
		return false
	}

	return true
}

func (c *client) AcquireTask(ctx context.Context, args *AcquireArgs) (ret *WorkerTask, err error) {
	err = c.GetWith(ctx, c.Host+"/task/acquire?idc="+args.IDC, &ret)
	return
}

type WorkerInspectTask struct {
	Task *proto.InspectTask `json:"task"`
}

func (task *WorkerInspectTask) IsValid() bool {
	if !task.Task.Mode.IsValid() {
		return false
	}

	if !proto.CheckVunitLocations(task.Task.Replicas) {
		return false
	}

	return true
}

func (c *client) AcquireInspectTask(ctx context.Context) (*WorkerInspectTask, error) {
	ret := WorkerInspectTask{
		Task: &proto.InspectTask{},
	}
	err := c.GetWith(ctx, c.Host+"/inspect/acquire", &ret)
	return &ret, err
}

type TaskRenewalArgs struct {
	IDC           string              `json:"idc"`
	Repair        map[string]struct{} `json:"repair"`
	Balance       map[string]struct{} `json:"balance"`
	DiskDrop      map[string]struct{} `json:"disk_drop"`
	ManualMigrate map[string]struct{} `json:"manual_migrate"`
}

type TaskRenewalRet struct {
	Repair        map[string]string `json:"repair"`
	Balance       map[string]string `json:"balance"`
	DiskDrop      map[string]string `json:"disk_drop"`
	ManualMigrate map[string]string `json:"manual_migrate"`
}

func (c *client) RenewalTask(ctx context.Context, args *TaskRenewalArgs) (ret *TaskRenewalRet, err error) {
	err = c.PostWith(ctx, c.Host+"/task/renewal", &ret, args)
	return
}

type TaskReportArgs struct {
	TaskType string `json:"task_type"`
	TaskId   string `json:"task_id"`

	TaskStats            proto.TaskStatistics `json:"task_stats"`
	IncreaseDataSizeByte int                  `json:"increase_data_size_byte"`
	IncreaseShardCnt     int                  `json:"increase_shard_cnt"`
}

func (c *client) ReportTask(ctx context.Context, args *TaskReportArgs) (err error) {
	return c.PostWith(ctx, c.Host+"/task/report", nil, args)
}

type ReclaimTaskArgs struct {
	TaskId   string                `json:"task_id"`
	IDC      string                `json:"idc"`
	TaskType string                `json:"task_type"`
	Src      []proto.VunitLocation `json:"src"`
	Dest     proto.VunitLocation   `json:"dest"`
	Reason   string                `json:"reason"`
}

func (c *client) ReclaimTask(ctx context.Context, args *ReclaimTaskArgs) (err error) {
	return c.PostWith(ctx, c.Host+"/task/reclaim", nil, args)
}

type CancelTaskArgs struct {
	TaskId   string                `json:"task_id"`
	IDC      string                `json:"idc"`
	TaskType string                `json:"task_type"`
	Src      []proto.VunitLocation `json:"src"`
	Dest     proto.VunitLocation   `json:"dest"`
	Reason   string                `json:"reason"`
}

func (c *client) CancelTask(ctx context.Context, args *CancelTaskArgs) (err error) {
	return c.PostWith(ctx, c.Host+"/task/cancel", nil, args)
}

type CompleteTaskArgs struct {
	TaskId   string                `json:"task_id"`
	IDC      string                `json:"idc"`
	TaskType string                `json:"task_type"`
	Src      []proto.VunitLocation `json:"src"`
	Dest     proto.VunitLocation   `json:"dest"`
}

func (c *client) CompleteTask(ctx context.Context, args *CompleteTaskArgs) (err error) {
	return c.PostWith(ctx, c.Host+"/task/complete", nil, args)
}

type CompleteInspectArgs struct {
	*proto.InspectRet
}

func (c *client) CompleteInspect(ctx context.Context, args *CompleteInspectArgs) (err error) {
	return c.PostWith(ctx, c.Host+"/inspect/complete", nil, args)
}

type AddManualMigrateArgs struct {
	Vuid           proto.Vuid `json:"vuid"`
	DirectDownload bool       `json:"direct_download"`
}

func (args *AddManualMigrateArgs) Valid() bool {
	return args.Vuid.IsValid()
}

func (c *client) AddManualMigrateTask(ctx context.Context, args *AddManualMigrateArgs) (err error) {
	return c.PostWith(ctx, c.Host+"/manual/migrate/task/add", nil, args)
}

// for task stat
type TaskStatArgs struct {
	TaskId string `json:"task_id"`
}

type RepairTaskDetail struct {
	TaskInfo proto.VolRepairTask  `json:"task_info"`
	RunStats proto.TaskStatistics `json:"run_stats"`
}

type MigrateTaskDetail struct {
	TaskInfo proto.MigrateTask    `json:"task_info"`
	RunStats proto.TaskStatistics `json:"run_stats"`
}

type PerMinStats struct {
	FinishedCnt    string `json:"finished_cnt"`
	ShardCnt       string `json:"shard_cnt"`
	DataAmountByte string `json:"data_amount_byte"`
}

type RepairTasksStat struct {
	Switch           string       `json:"switch"`
	RepairingDiskId  proto.DiskID `json:"repairing_disk_id"`
	TotalTasksCnt    int          `json:"total_tasks_cnt"`
	RepairedTasksCnt int          `json:"repaired_tasks_cnt"`
	PreparingCnt     int          `json:"preparing_cnt"`
	WorkerDoingCnt   int          `json:"worker_doing_cnt"`
	FinishingCnt     int          `json:"finishing_cnt"`
	StatsPerMin      PerMinStats  `json:"stats_per_min"`
}

type MigrateTasksStat struct {
	PreparingCnt   int         `json:"preparing_cnt"`
	WorkerDoingCnt int         `json:"worker_doing_cnt"`
	FinishingCnt   int         `json:"finishing_cnt"`
	StatsPerMin    PerMinStats `json:"stats_per_min"`
}

type DiskDropTasksStat struct {
	Switch          string       `json:"switch"`
	DroppingDiskId  proto.DiskID `json:"dropping_disk_id"`
	TotalTasksCnt   int          `json:"total_tasks_cnt"`
	DroppedTasksCnt int          `json:"dropped_tasks_cnt"`
	MigrateTasksStat
}

type BalanceTasksStat struct {
	Switch string `json:"switch"`
	MigrateTasksStat
}

type ManualMigrateTasksStat struct {
	MigrateTasksStat
}

type InspectTasksStats struct {
	Switch         string `json:"switch"`
	FinishedPerMin string `json:"finished_per_min"`
	TimeOutPerMin  string `json:"time_out_per_min"`
}

type TasksStat struct {
	Repair        RepairTasksStat        `json:"repair"`
	Drop          DiskDropTasksStat      `json:"drop"`
	Balance       BalanceTasksStat       `json:"balance"`
	ManualMigrate ManualMigrateTasksStat `json:"manual_migrate"`
	Inspect       InspectTasksStats      `json:"inspect"`
}

func (c *client) RepairTaskDetail(ctx context.Context, args *TaskStatArgs) (ret RepairTaskDetail, err error) {
	err = c.PostWith(ctx, c.Host+"/repair/task/detail", &ret, args)
	return
}

func (c *client) BalanceTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error) {
	err = c.PostWith(ctx, c.Host+"/balance/task/detail", &ret, args)
	return
}

func (c *client) DropTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error) {
	err = c.PostWith(ctx, c.Host+"/drop/task/detail", &ret, args)
	return
}

func (c *client) ManualMigrateTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error) {
	err = c.PostWith(ctx, c.Host+"/manual/migrate/task/detail", &ret, args)
	return
}

func (c *client) Stats(ctx context.Context) (ret TasksStat, err error) {
	err = c.GetWith(ctx, c.Host+"/stats", &ret)
	return
}
