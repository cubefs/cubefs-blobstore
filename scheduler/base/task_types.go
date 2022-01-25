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

package base

import (
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

const (
	defaultPrepareQueueRetryDelayS = 10
	defaultCancelPunishDurationS   = 20
	defaultFinishQueueRetryDelayS  = 10
	defaultCollectIntervalS        = 5
	defaultCheckTaskIntervalS      = 5

	defaultWorkQueueSize = 20
)

const (
	// EmptyDiskID empty diskID
	EmptyDiskID = 0
	// CollectIntervalS collect interval second
	CollectIntervalS = 5 * time.Second
)

// err use for task
var (
	ErrNoTaskInQueue         = errors.New("no task in queue")
	ErrVolNotOnlyOneTask     = errors.New("vol not only one task running")
	ErrNotifyTinkerUpdateVol = errors.New("notify tinker update volume err")
	ErrNoDocuments           = mongo.ErrNoDocuments
)

// TaskCommonConfig task common config
type TaskCommonConfig struct {
	PrepareQueueRetryDelayS int `json:"prepare_queue_retry_delay_s"`
	FinishQueueRetryDelayS  int `json:"finish_queue_retry_delay_s"`
	CancelPunishDurationS   int `json:"cancel_punish_duration_s"`
	WorkQueueSize           int `json:"work_queue_size"`
	CollectTaskIntervalS    int `json:"collect_task_interval_s"`
	CheckTaskIntervalS      int `json:"check_task_interval_s"`
}

// CheckAndFix check and fix task common config
func (conf *TaskCommonConfig) CheckAndFix() {
	if conf.PrepareQueueRetryDelayS <= 0 {
		conf.PrepareQueueRetryDelayS = defaultPrepareQueueRetryDelayS
	}

	if conf.FinishQueueRetryDelayS <= 0 {
		conf.FinishQueueRetryDelayS = defaultFinishQueueRetryDelayS
	}

	if conf.CancelPunishDurationS <= 0 {
		conf.CancelPunishDurationS = defaultCancelPunishDurationS
	}

	if conf.WorkQueueSize <= 0 {
		conf.WorkQueueSize = defaultWorkQueueSize
	}

	if conf.CollectTaskIntervalS <= 0 {
		conf.CollectTaskIntervalS = defaultCollectIntervalS
	}

	if conf.CheckTaskIntervalS <= 0 {
		conf.CheckTaskIntervalS = defaultCheckTaskIntervalS
	}
}
