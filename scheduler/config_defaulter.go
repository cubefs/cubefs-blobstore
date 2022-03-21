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
	"github.com/cubefs/blobstore/common/mongoutil"
)

const (
	defaultTopologyUpdateIntervalMin = 1
	defaultArchiveDelayMin           = 5
	defaultArchiveIntervalMin        = 5
	defaultClientTimeoutMs           = int64(1000)
	defaultMongoTimeoutMs            = int64(3000)

	defaultBalanceDiskCntLimit = 100
	defaultMaxDiskFreeChunkCnt = int64(1024)
	defaultMinDiskFreeChunkCnt = int64(20)

	defaultInspectTimeoutMs  = 10000
	defaultListVolStep       = 100
	defaultListVolIntervalMs = 10
	defaultInspectBatch      = 1000

	defaultBalanceTable           = "balance_tbl"
	defaultDiskDropTable          = "disk_drop_tbl"
	defaultRepairTable            = "repair_tbl"
	defaultInspectCheckPointTable = "inspect_checkpoint_tbl"
	defaultManualMigrateTable     = "manual_migrate_tbl"
	defaultSvrRegisterTable       = "svr_register_tbl"
	defaultArchiveTasksTable      = "archive_tasks_tbl"
)

var defaultWriteConfig = mongoutil.DefaultWriteConfig
