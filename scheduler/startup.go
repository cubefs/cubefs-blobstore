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
	"time"

	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/api/mqproxy"
	api "github.com/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/blobstore/api/tinker"
	"github.com/cubefs/blobstore/cmd"
	"github.com/cubefs/blobstore/common/config"
	"github.com/cubefs/blobstore/common/mongoutil"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/log"
)

var (
	service *Service
	conf    Config
)

func init() {
	mod := &cmd.Module{
		Name:       "SCHEDULER",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "scheduler.conf")

	if err := config.Load(&conf); err != nil {
		return nil, err
	}

	return &conf.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	var err error
	service, err = NewService(&conf)
	if err != nil {
		log.Fatalf("new service failed, err: %v", err)
	}
	return NewHandler(service), nil
}

func tearDown() {
	// close record file safety
	service.Close()
}

// ErrIllegalClusterID illegal cluster_id
var ErrIllegalClusterID = errors.New("illegal cluster_id")

// Config service config
type Config struct {
	cmd.Config

	ClusterID                 proto.ClusterID        `json:"cluster_id"`
	Database                  db.Config              `json:"database"`
	TaskArchiveStoreDB        *db.ArchiveStoreConfig `json:"task_archive_store_db"`
	TopologyUpdateIntervalMin int                    `json:"topology_update_interval_min"`
	FreeChunkCounterBuckets   []float64              `json:"free_chunk_counter_buckets"`
	ClusterMgr                *clustermgr.Config     `json:"clustermgr"`
	MqProxy                   *mqproxy.LbConfig      `json:"mqproxy"`
	Tinker                    *tinker.Config         `json:"tinker"`
	BalanceTask               *BalanceMgrConfig      `json:"balance_task"`
	DiskDropTask              *DiskDropMgrConfig     `json:"disk_drop_task"`
	RepairTask                *RepairMgrCfg          `json:"repair_task"`
	InspectTask               *InspectMgrCfg         `json:"inspect_task"`

	// inspect may be unavailable if NotMustNeedMqProxy is true
	NotMustNeedMqProxy bool `json:"not_must_need_mq_proxy"`
}

func (c *Config) isMqProxyNecessary() bool {
	return !c.NotMustNeedMqProxy
}

func (c *Config) checkAndFix() (err error) {
	if c.ClusterID == 0 {
		return ErrIllegalClusterID
	}
	if c.TopologyUpdateIntervalMin <= 0 {
		c.TopologyUpdateIntervalMin = 1
	}

	c.checkAndFixClientCfg()
	c.checkAndFixDataBaseCfg()
	c.checkAndFixArchiveStoreCfg()
	c.checkAndFixBalanceCfg()
	c.checkAndFixDiskDropCfg()
	c.checkAndFixRepairCfg()
	c.checkAndFixInspectCfg()

	return nil
}

func (c *Config) checkAndFixClientCfg() {
	if c.MqProxy == nil {
		c.MqProxy = &mqproxy.LbConfig{}
	}
	if c.MqProxy.ClientTimeoutMs <= 0 {
		c.MqProxy.ClientTimeoutMs = 1000
	}

	if c.Tinker == nil {
		c.Tinker = &tinker.Config{}
	}
	if c.Tinker.ClientTimeoutMs <= 0 {
		c.MqProxy.ClientTimeoutMs = 1000
	}
}

func (c *Config) checkAndFixDataBaseCfg() {
	if c.Database.Mongo.TimeoutMs <= 0 {
		c.Database.Mongo.TimeoutMs = 3000
	}
	if c.Database.Mongo.WriteConcern == nil {
		c.Database.Mongo.WriteConcern = &mongoutil.WriteConcernConfig{TimeoutMs: 3000, Majority: true}
	}

	if c.Database.BalanceTblName == "" {
		c.Database.BalanceTblName = "balance_tbl"
	}
	if c.Database.DiskDropTblName == "" {
		c.Database.DiskDropTblName = "disk_drop_tbl"
	}
	if c.Database.RepairTblName == "" {
		c.Database.RepairTblName = "repair_tbl"
	}
	if c.Database.InspectCheckPointTblName == "" {
		c.Database.InspectCheckPointTblName = "inspect_checkpoint_tbl"
	}
	if c.Database.ManualMigrateTblName == "" {
		c.Database.ManualMigrateTblName = "manual_migrate_tbl"
	}
	if c.Database.SvrRegisterTblName == "" {
		c.Database.SvrRegisterTblName = "svr_register_tbl"
	}
}

func (c *Config) checkAndFixArchiveStoreCfg() {
	if c.TaskArchiveStoreDB == nil {
		c.TaskArchiveStoreDB = &db.ArchiveStoreConfig{}
	}

	if c.TaskArchiveStoreDB.ArchiveDelayMin == 0 {
		c.TaskArchiveStoreDB.ArchiveDelayMin = 5
	}
	if c.TaskArchiveStoreDB.ArchiveIntervalMin == 0 {
		c.TaskArchiveStoreDB.ArchiveIntervalMin = c.TaskArchiveStoreDB.ArchiveDelayMin
	}
	if c.TaskArchiveStoreDB.Mongo.TimeoutMs <= 0 {
		c.TaskArchiveStoreDB.Mongo.TimeoutMs = 3000
	}
	if c.TaskArchiveStoreDB.Mongo.WriteConcern == nil {
		c.TaskArchiveStoreDB.Mongo.WriteConcern = &mongoutil.WriteConcernConfig{TimeoutMs: 3000, Majority: true}
	}
	if c.TaskArchiveStoreDB.TblName == "" {
		c.TaskArchiveStoreDB.TblName = "tasks_tbl"
	}
}

func (c *Config) checkAndFixRepairCfg() {
	if c.RepairTask == nil {
		c.RepairTask = &RepairMgrCfg{}
	}
	c.RepairTask.ClusterID = c.ClusterID
	c.RepairTask.CheckAndFix()
}

func (c *Config) checkAndFixBalanceCfg() {
	if c.BalanceTask == nil {
		c.BalanceTask = &BalanceMgrConfig{}
	}
	c.BalanceTask.ClusterID = c.ClusterID

	if c.BalanceTask.BalanceDiskCntLimit <= 0 {
		c.BalanceTask.BalanceDiskCntLimit = 100
	}
	if c.BalanceTask.MaxDiskFreeChunkCnt <= 0 {
		c.BalanceTask.MaxDiskFreeChunkCnt = 100
	}
	if c.BalanceTask.MinDiskFreeChunkCnt <= 0 {
		c.BalanceTask.MinDiskFreeChunkCnt = 20
	}

	c.BalanceTask.CheckAndFix()
}

func (c *Config) checkAndFixDiskDropCfg() {
	if c.DiskDropTask == nil {
		c.DiskDropTask = &DiskDropMgrConfig{}
	}
	c.DiskDropTask.ClusterID = c.ClusterID
	c.DiskDropTask.CheckAndFix()
}

func (c *Config) checkAndFixInspectCfg() {
	if c.InspectTask == nil {
		c.InspectTask = &InspectMgrCfg{}
	}
	if c.InspectTask.TimeoutMs <= 0 {
		c.InspectTask.TimeoutMs = 10000
	}
	if c.InspectTask.ListVolStep <= 0 {
		c.InspectTask.ListVolStep = 100
	}
	if c.InspectTask.ListVolIntervalMs <= 0 {
		c.InspectTask.ListVolIntervalMs = 10
	}
	if c.InspectTask.InspectBatch <= 0 {
		c.InspectTask.InspectBatch = 1000
	}

	if c.InspectTask.InspectBatch < c.InspectTask.ListVolStep {
		c.InspectTask.InspectBatch = c.InspectTask.ListVolStep
	}
}

// NewService returns scheduler service
func NewService(conf *Config) (svr *Service, err error) {
	if err := conf.checkAndFix(); err != nil {
		log.Errorf("scheduler service config check error: %v", err)
		return nil, err
	}

	// init db
	database, err := db.OpenDatabase(&conf.Database, conf.TaskArchiveStoreDB)
	if err != nil {
		log.Errorf("open database fail err %+v", err)
		return nil, err
	}

	clusterMgrCli := client.CmCliInst()
	err = clusterMgrCli.Init(conf.ClusterMgr)
	if err != nil {
		log.Errorf("new cm client fail err:%+v", err)
		return nil, err
	}

	tinkerCli := client.NewTinkerClient(conf.Tinker)

	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)

	// init cluster topology
	topoConf := &clusterTopoConf{
		ClusterID:               conf.ClusterID,
		UpdateIntervalMin:       time.Duration(conf.TopologyUpdateIntervalMin) * time.Minute,
		FreeChunkCounterBuckets: conf.FreeChunkCounterBuckets,
	}
	topologyMgr := NewClusterTopologyMgr(clusterMgrCli, topoConf)

	// new balance manager
	balanceMgr, err := NewBalanceMgr(
		clusterMgrCli,
		tinkerCli,
		switchMgr,
		topologyMgr,
		database.SvrRegisterTbl,
		database.BalanceTbl,
		conf.BalanceTask)
	if err != nil {
		log.Errorf("new balance mgr fail err %+v", err)
		return nil, err
	}

	// new disk drop manager
	diskDropMgr, err := NewDiskDropMgr(
		clusterMgrCli,
		tinkerCli,
		switchMgr,
		database.SvrRegisterTbl,
		database.DiskDropTbl,
		conf.DiskDropTask)
	if err != nil {
		log.Errorf("new disk drop mgr fail err %+v", err)
		return nil, err
	}

	// ner manual migrate manager
	manualMigMgr := NewManualMigrateMgr(
		clusterMgrCli,
		tinkerCli,
		database.SvrRegisterTbl,
		database.ManualMigrateTbl,
		conf.ClusterID)

	// new disk repair manager
	repairMgr, err := NewRepairMgr(
		conf.RepairTask,
		switchMgr,
		database.RepairTaskTbl,
		clusterMgrCli)
	if err != nil {
		log.Errorf("new RepairMgr fail err %+v", err)
		return nil, err
	}

	// new inspect manger
	var inspectMgr *InspectMgr
	mqProxy, err := client.NewMqProxyClient(conf.MqProxy, conf.ClusterMgr, conf.ClusterID)
	if err != nil {
		log.Errorf("new proxy client fail err %+v", err)
		if conf.isMqProxyNecessary() {
			return nil, errors.New("mq proxy:" + err.Error())
		}
	} else {
		inspectMgr, err = NewInspectMgr(
			conf.InspectTask,
			database.InspectCheckPointTbl,
			clusterMgrCli,
			mqProxy,
			switchMgr)
		if err != nil {
			log.Errorf("new inspect mgr fail err %+v", err)
			return nil, err
		}
		log.Infof("new inspect mgr success")
	}

	svr = &Service{
		ClusterID: conf.ClusterID,

		clusterTopoMgr: topologyMgr,
		balanceMgr:     balanceMgr,
		diskDropMgr:    diskDropMgr,
		manualMigMgr:   manualMigMgr,
		repairMgr:      repairMgr,
		inspectMgr:     inspectMgr,
		svrTbl:         database.SvrRegisterTbl,

		cmCli: clusterMgrCli,
	}

	err = svr.waitAndLoad()
	if err != nil {
		log.Errorf("load task from database failed, err:%v", err)
		return nil, err
	}

	go svr.Run()
	return svr, nil
}

func (svr *Service) waitAndLoad() error {
	//why:service stop a task lease period to make sure all worker release task
	//so there will not a task run on multiple worker
	log.Infof("start waitAndLoad")
	time.Sleep(proto.TaskLeaseExpiredS * time.Second)
	return svr.load()
}

func (svr *Service) load() (err error) {
	err = svr.repairMgr.Load()
	if err != nil {
		return
	}

	err = svr.balanceMgr.Load()
	if err != nil {
		return
	}

	err = svr.diskDropMgr.Load()
	if err != nil {
		return
	}

	err = svr.manualMigMgr.Load()
	if err != nil {
		return
	}

	return
}

// Run run task
func (svr *Service) Run() {
	svr.repairMgr.Run()
	svr.balanceMgr.Run()
	svr.diskDropMgr.Run()
	svr.manualMigMgr.Run()

	if svr.inspectMgr != nil {
		svr.inspectMgr.Run()
	}
}

// Close close service safe
func (svr *Service) Close() {
	svr.balanceMgr.Close()
}

// NewHandler returns app server handler
func NewHandler(service *Service) *rpc.Router {
	rpc.RegisterArgsParser(&api.AcquireArgs{}, "json")
	rpc.RegisterArgsParser(&api.ReclaimTaskArgs{}, "json")
	rpc.RegisterArgsParser(&api.CancelTaskArgs{}, "json")
	rpc.RegisterArgsParser(&api.CompleteTaskArgs{}, "json")
	rpc.RegisterArgsParser(&api.AddManualMigrateArgs{}, "json")

	rpc.RegisterArgsParser(&api.CompleteInspectArgs{}, "json")

	rpc.RegisterArgsParser(&api.TaskReportArgs{}, "json")
	rpc.RegisterArgsParser(&api.TaskRenewalArgs{}, "json")

	rpc.RegisterArgsParser(&api.TaskStatArgs{}, "json")

	rpc.RegisterArgsParser(&api.ListServicesArgs{}, "json")
	rpc.RegisterArgsParser(&api.RegisterServiceArgs{}, "json")
	rpc.RegisterArgsParser(&api.FindServiceArgs{}, "json")
	rpc.RegisterArgsParser(&api.DeleteServiceArgs{}, "json")

	// rpc http svr interface
	rpc.GET("/task/acquire", service.HTTPTaskAcquire, rpc.OptArgsQuery())
	rpc.POST("/task/reclaim", service.HTTPTaskReclaim, rpc.OptArgsBody())
	rpc.POST("/task/cancel", service.HTTPTaskCancel, rpc.OptArgsBody())
	rpc.POST("/task/complete", service.HTTPTaskComplete, rpc.OptArgsBody())
	rpc.POST("/manual/migrate/task/add", service.HTTPManualMigrateTaskAdd, rpc.OptArgsBody())

	rpc.GET("/inspect/acquire", service.HTTPInspectAcquire, rpc.OptArgsQuery())
	rpc.POST("/inspect/complete", service.HTTPInspectComplete, rpc.OptArgsBody())

	rpc.POST("/task/report", service.HTTPTaskReport, rpc.OptArgsBody())
	rpc.POST("/task/renewal", service.HTTPTaskRenewal, rpc.OptArgsBody())

	rpc.POST("/balance/task/detail", service.HTTPBalanceTaskDetail, rpc.OptArgsBody())
	rpc.POST("/repair/task/detail", service.HTTPRepairTaskDetail, rpc.OptArgsBody())
	rpc.POST("/drop/task/detail", service.HTTPDropTaskDetail, rpc.OptArgsBody())
	rpc.POST("/manual/migrate/task/detail", service.HTTPManualMigrateTaskDetail, rpc.OptArgsBody())
	rpc.GET("/stats", service.HTTPStats, rpc.OptArgsQuery())

	rpc.GET("/service/list", service.HTTPServiceList, rpc.OptArgsQuery())
	rpc.POST("/service/register", service.HTTPServiceRegister, rpc.OptArgsBody())
	rpc.GET("/service/get", service.HTTPServiceGet, rpc.OptArgsQuery())
	rpc.POST("/service/delete", service.HTTPServiceDelete, rpc.OptArgsBody())

	return rpc.DefaultRouter
}
