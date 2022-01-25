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
	"os"

	cmapi "github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/blobstore/cmd"
	"github.com/cubefs/blobstore/util/log"
)

const (
	DefaultHeartbeatIntervalSec        = 30           // 30 s
	DefaultChunkReportIntervalSec      = 1*60 - 3     // 1 min
	DefaultCleanExpiredStatIntervalSec = 60 * 60      // 60 min
	DefaultChunkGcIntervalSec          = 30 * 60      // 30 min
	DefaultChunkProtectionPeriodSec    = 48 * 60 * 60 // 48 hour
	DefaultDiskStatusCheckIntervalSec  = 2 * 60       // 2 min

	DefaultPutQpsLimitPerDisk    = 128
	DefaultGetQpsLimitPerDisk    = 512
	DefaultGetQpsLimitPerKey     = 64
	DefaultDeleteQpsLimitPerDisk = 128
)

type Config struct {
	cmd.Config
	core.HostInfo
	Disks         []core.Config      `json:"disks"`
	DiskConfig    core.RuntimeConfig `json:"disk_config"`
	MetaConfig    db.MetaConfig      `json:"meta_config"`
	FlockFilename string             `json:"flock_filename"`

	Clustermgr *cmapi.Config `json:"clustermgr"`

	HeartbeatIntervalSec        int `json:"heartbeat_interval_S"`
	ChunkReportIntervalSec      int `json:"chunk_report_interval_S"`
	ChunkGcIntervalSec          int `json:"chunk_gc_interval_S"`
	ChunkProtectionPeriodSec    int `json:"chunk_protection_period_S"`
	CleanExpiredStatIntervalSec int `json:"clean_expired_stat_interval_S"`
	DiskStatusCheckIntervalSec  int `json:"disk_status_check_interval_S"`

	PutQpsLimitPerDisk    int `json:"put_qps_limit_per_disk"`
	GetQpsLimitPerDisk    int `json:"get_qps_limit_per_disk"`
	GetQpsLimitPerKey     int `json:"get_qps_limit_per_key"`
	DeleteQpsLimitPerDisk int `json:"delete_qps_limit_per_disk"`
}

func configInit(config *Config) {
	if len(config.Disks) == 0 {
		log.Fatalf("disk list is empty")
		os.Exit(1)
	}

	if config.HeartbeatIntervalSec <= 0 {
		config.HeartbeatIntervalSec = DefaultHeartbeatIntervalSec
	}

	if config.ChunkGcIntervalSec <= 0 {
		config.ChunkGcIntervalSec = DefaultChunkGcIntervalSec
	}

	if config.ChunkProtectionPeriodSec <= 0 {
		config.ChunkProtectionPeriodSec = DefaultChunkProtectionPeriodSec
	}

	if config.DiskStatusCheckIntervalSec <= 0 {
		config.DiskStatusCheckIntervalSec = DefaultDiskStatusCheckIntervalSec
	}

	if config.ChunkReportIntervalSec <= 0 {
		config.ChunkReportIntervalSec = DefaultChunkReportIntervalSec
	}

	if config.CleanExpiredStatIntervalSec <= 0 {
		config.CleanExpiredStatIntervalSec = DefaultCleanExpiredStatIntervalSec
	}

	if config.PutQpsLimitPerDisk <= 0 {
		config.PutQpsLimitPerDisk = DefaultPutQpsLimitPerDisk
	}

	if config.GetQpsLimitPerDisk == 0 {
		config.GetQpsLimitPerDisk = DefaultGetQpsLimitPerDisk
	}

	if config.GetQpsLimitPerKey == 0 {
		config.GetQpsLimitPerKey = DefaultGetQpsLimitPerKey
	}

	if config.DeleteQpsLimitPerDisk <= 0 {
		config.DeleteQpsLimitPerDisk = DefaultDeleteQpsLimitPerDisk
	}
}
