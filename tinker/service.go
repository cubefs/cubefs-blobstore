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
	"time"

	"github.com/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/api/scheduler"
	api "github.com/cubefs/blobstore/api/tinker"
	"github.com/cubefs/blobstore/api/worker"
	"github.com/cubefs/blobstore/cmd"
	"github.com/cubefs/blobstore/common/config"
	"github.com/cubefs/blobstore/common/mongoutil"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/tinker/base"
	"github.com/cubefs/blobstore/tinker/client"
	"github.com/cubefs/blobstore/tinker/db"
	"github.com/cubefs/blobstore/util/log"
	"github.com/cubefs/blobstore/util/retry"
)

var (
	service *Service
	conf    Config
)

func init() {
	mod := &cmd.Module{
		Name:       "TINKER",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "tinker.conf")

	if err := config.Load(&conf); err != nil {
		return nil, err
	}
	return &conf.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	var err error
	service, err = NewService(conf)
	if err != nil {
		log.Fatalf("new service failed, err: %v", err)
	}
	return NewHandler(service), nil
}

func tearDown() {
}

const (
	defaultClientTimeoutMs          = 1000
	defaultMongoTimeoutMs           = 3000
	defaultUpdateDurationS          = 10
	defaultTaskPoolSize             = 10
	defaultHandleBatchCnt           = 100
	defaultFailMsgConsumeIntervalMs = 10000
	defaultAuditLogChunkSize        = 29
)

// ServiceRegisterConfig is service register info
type ServiceRegisterConfig struct {
	Idc  string `json:"idc"`
	Host string `json:"host"`
}

// Config is service config
type Config struct {
	cmd.Config

	ClusterID       proto.ClusterID       `json:"cluster_id"`
	ServiceRegister ServiceRegisterConfig `json:"service_register"`
	ShardRepair     ShardRepairConfig     `json:"shard_repair"`
	BlobDelete      BlobDeleteConfig      `json:"blob_delete"`

	Database db.Config `json:"database"`

	ClusterMgr clustermgr.Config `json:"clustermgr"`
	Worker     worker.Config     `json:"worker"`
	BlobNode   blobnode.Config   `json:"blobnode"`
	Scheduler  scheduler.Config  `json:"scheduler"`

	ListVolIntervalMs       int64 `json:"list_vol_interval_ms"`
	ListVolCount            int   `json:"list_vol_count"`
	VolCacheUpdateDurationS int   `json:"vol_cache_update_duration_s"`
}

func (cfg *Config) checkAndFix() (err error) {
	if cfg.VolCacheUpdateDurationS <= 0 {
		cfg.VolCacheUpdateDurationS = defaultUpdateDurationS
	}

	cfg.ShardRepair.ClusterID = cfg.ClusterID
	cfg.ShardRepair.Idc = cfg.ServiceRegister.Idc

	cfg.BlobDelete.ClusterID = cfg.ClusterID

	if cfg.ClusterMgr.Config.ClientTimeoutMs <= 0 {
		cfg.ClusterMgr.Config.ClientTimeoutMs = defaultClientTimeoutMs
	}
	if cfg.BlobNode.ClientTimeoutMs <= 0 {
		cfg.BlobNode.ClientTimeoutMs = defaultClientTimeoutMs
	}
	if cfg.Worker.ClientTimeoutMs <= 0 {
		cfg.Worker.ClientTimeoutMs = defaultClientTimeoutMs
	}
	if cfg.Scheduler.ClientTimeoutMs <= 0 {
		cfg.Scheduler.ClientTimeoutMs = defaultClientTimeoutMs
	}
	if cfg.Database.Mongo.TimeoutMs <= 0 {
		cfg.Database.Mongo.TimeoutMs = defaultMongoTimeoutMs
	}
	if cfg.Database.KafkaOffsetTblName == "" {
		cfg.Database.KafkaOffsetTblName = "kafka_offset_tbl"
	}
	if cfg.Database.OrphanedShardTblName == "" {
		cfg.Database.OrphanedShardTblName = "orphaned_shard_tbl"
	}
	if cfg.Database.Mongo.WriteConcern == nil {
		cfg.Database.Mongo.WriteConcern = &mongoutil.WriteConcernConfig{TimeoutMs: defaultMongoTimeoutMs, Majority: true}
	}

	cfg.fixShardRepairConfig()
	cfg.fixBlobDeleteConfig()

	return
}

func (cfg *Config) fixShardRepairConfig() {
	if cfg.ShardRepair.TaskPoolSize <= 0 {
		cfg.ShardRepair.TaskPoolSize = defaultTaskPoolSize
	}
	if cfg.ShardRepair.NormalHandleBatchCnt <= 0 {
		cfg.ShardRepair.NormalHandleBatchCnt = defaultHandleBatchCnt
	}
	if cfg.ShardRepair.FailHandleBatchCnt <= 0 {
		cfg.ShardRepair.FailHandleBatchCnt = defaultHandleBatchCnt
	}
	if cfg.ShardRepair.FailMsgConsumeIntervalMs <= 0 {
		cfg.ShardRepair.FailMsgConsumeIntervalMs = defaultFailMsgConsumeIntervalMs
	}
	if cfg.ShardRepair.FailMsgSender.TimeoutMs <= 0 {
		cfg.ShardRepair.FailMsgSender.TimeoutMs = defaultClientTimeoutMs
	}
	for k := range cfg.ShardRepair.PriorityTopics {
		cfg.ShardRepair.PriorityTopics[k].BrokerList = cfg.ShardRepair.BrokerList
	}
	cfg.ShardRepair.FailTopic.BrokerList = cfg.ShardRepair.BrokerList
	cfg.ShardRepair.FailMsgSender.BrokerList = cfg.ShardRepair.BrokerList
}

func (cfg *Config) fixBlobDeleteConfig() {
	if cfg.BlobDelete.TaskPoolSize <= 0 {
		cfg.BlobDelete.TaskPoolSize = defaultTaskPoolSize
	}
	if cfg.BlobDelete.NormalHandleBatchCnt <= 0 {
		cfg.BlobDelete.NormalHandleBatchCnt = defaultHandleBatchCnt
	}
	if cfg.BlobDelete.FailHandleBatchCnt <= 0 {
		cfg.BlobDelete.FailHandleBatchCnt = defaultHandleBatchCnt
	}
	if cfg.BlobDelete.FailMsgConsumeIntervalMs <= 0 {
		cfg.BlobDelete.FailMsgConsumeIntervalMs = defaultFailMsgConsumeIntervalMs
	}
	if cfg.BlobDelete.FailMsgSender.TimeoutMs <= 0 {
		cfg.BlobDelete.FailMsgSender.TimeoutMs = defaultClientTimeoutMs
	}
	if cfg.BlobDelete.DelLog.ChunkBits <= 0 {
		cfg.BlobDelete.DelLog.ChunkBits = defaultAuditLogChunkSize
	}
	cfg.BlobDelete.NormalTopic.BrokerList = cfg.BlobDelete.BrokerList
	cfg.BlobDelete.FailTopic.BrokerList = cfg.BlobDelete.BrokerList
	cfg.BlobDelete.FailMsgSender.BrokerList = cfg.BlobDelete.BrokerList
}

// Service rpc service
type Service struct {
	Config
	clusterMgrClient client.ClusterMgrAPI

	switchMgr      *taskswitch.SwitchMgr
	shardRepairMgr base.IBaseMgr
	deleteMgr      base.IBaseMgr

	volCache base.IVolumeCache
	database *db.Database
}

// NewService returns a tinker service
func NewService(cfg Config) (*Service, error) {
	if err := cfg.checkAndFix(); err != nil {
		return nil, fmt.Errorf("check config: cfg:[%+v], err:[%w]", cfg, err)
	}

	// init db
	database, err := db.OpenDatabase(cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("open database: cfg[%+v], err[%w]", cfg.Database, err)
	}

	cmCli := client.NewCmClient(&cfg.ClusterMgr)
	schedulerCli := client.NewSchedulerClient(&cfg.Scheduler)
	blobNodeCli := client.NewBlobNodeClient(&cfg.BlobNode)
	workerCli := client.NewWorkerCli(&cfg.Worker)

	switchMgr := taskswitch.NewSwitchMgr(cmCli)
	vc := NewVolCache(cmCli, cfg.VolCacheUpdateDurationS)

	offAccessor := base.NewMgoOffAccessor(database.KafkaOffsetTable)

	shardRepairMgr, err := NewShardRepairMgr(&cfg.ShardRepair, vc, switchMgr, offAccessor, schedulerCli, database.OrphanedShardTable, workerCli)
	if err != nil {
		return nil, fmt.Errorf("new shard repair mgr: cfg[%+v], err[%w]", cfg.ShardRepair, err)
	}

	deleteMgr, err := NewDeleteMgr(&cfg.BlobDelete, vc, offAccessor, blobNodeCli, switchMgr)
	if err != nil {
		return nil, fmt.Errorf("new blob delete mgr: cfg[%+v], err[%w]", cfg.BlobDelete, err)
	}

	service := &Service{
		Config:           cfg,
		clusterMgrClient: cmCli,
		switchMgr:        switchMgr,
		shardRepairMgr:   shardRepairMgr,
		deleteMgr:        deleteMgr,
		volCache:         vc,
		database:         database,
	}

	err = service.Register(schedulerCli)
	if err != nil {
		return nil, fmt.Errorf("register: err[%w]", err)
	}

	err = service.runKafkaMonitor(offAccessor)
	if err != nil {
		return nil, fmt.Errorf("run kafka monitor: err[%w]", err)
	}

	go service.RunTask()

	return service, nil
}

// NewHandler returns app server handler
func NewHandler(service *Service) *rpc.Router {
	rpc.RegisterArgsParser(&api.VolInfo{}, "json")

	// POST /update/vol
	// request body: json
	// response body: json
	rpc.POST("/update/vol", service.HTTPUpdateVol, rpc.OptArgsBody())

	// GET /stats
	rpc.GET("/stats", service.HTTPStats)

	return rpc.DefaultRouter
}

// HTTPUpdateVol updates volume
func (s *Service) HTTPUpdateVol(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(api.VolInfo)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	_, err := s.volCache.Update(args.Vid)
	if err != nil {
		span.Errorf("volume cache update failed: vid[%d], err[%+v]", args.Vid, err)
		c.RespondError(err)
		return
	}

	c.Respond()
}

// HTTPStats returns service stats
func (s *Service) HTTPStats(c *rpc.Context) {
	// delete stats
	deleteSuccessCounter, deleteFailedCounter := s.deleteMgr.GetTaskStats()
	delErrStats, delTotalErrCnt := s.deleteMgr.GetErrorStats()

	var switchStatus string
	if s.deleteMgr.Enabled() {
		switchStatus = taskswitch.SwitchOpen
	} else {
		switchStatus = taskswitch.SwitchClose
	}
	deleteStat := api.Stat{
		Switch:        switchStatus,
		SuccessPerMin: fmt.Sprint(deleteSuccessCounter),
		FailedPerMin:  fmt.Sprint(deleteFailedCounter),
		TotalErrCnt:   delTotalErrCnt,
		ErrStats:      delErrStats,
	}

	// stats balance tasks
	repairSuccessCounter, repairFailedCounter := s.shardRepairMgr.GetTaskStats()
	repairErrStats, repairTotalErrCnt := s.shardRepairMgr.GetErrorStats()

	if s.shardRepairMgr.Enabled() {
		switchStatus = taskswitch.SwitchOpen
	} else {
		switchStatus = taskswitch.SwitchClose
	}
	repairStat := api.Stat{
		Switch:        switchStatus,
		SuccessPerMin: fmt.Sprint(repairSuccessCounter),
		FailedPerMin:  fmt.Sprint(repairFailedCounter),
		TotalErrCnt:   repairTotalErrCnt,
		ErrStats:      repairErrStats,
	}

	taskStats := api.Stats{
		ShardRepair: repairStat,
		BlobDelete:  deleteStat,
	}

	c.RespondJSON(taskStats)
}

// RunTask run shard repair and blob delete tasks
func (s *Service) RunTask() {
	err := s.LoadVolInfo()
	if err != nil {
		log.Panicf("load volume info failed: err[%+v]", err)
	}
	s.shardRepairMgr.RunTask()
	s.deleteMgr.RunTask()
}

// Register registers self service to scheduler
func (s *Service) Register(cli client.IScheduler) (err error) {
	if err := retry.Timed(3, 200).On(func() error {
		return cli.Register(context.Background(), s.ClusterID, proto.ServiceNameTinker, s.ServiceRegister.Host, s.ServiceRegister.Idc)
	}); err != nil {
		log.Errorf("register failed: err[%+v]", err)
		return err
	}
	return
}

// LoadVolInfo load volume info
func (s *Service) LoadVolInfo() error {
	return s.volCache.Load(time.Millisecond*time.Duration(s.ListVolIntervalMs), s.ListVolCount)
}

func (s *Service) runKafkaMonitor(access base.IOffsetAccessor) error {
	// collect cfg
	var topicCfgs []*base.KafkaConfig
	topicCfgs = append(topicCfgs, &s.BlobDelete.NormalTopic)
	topicCfgs = append(topicCfgs, &s.BlobDelete.FailTopic)
	for _, topicCfg := range s.ShardRepair.PriorityTopics {
		topicCfgs = append(topicCfgs, &topicCfg.KafkaConfig)
	}
	topicCfgs = append(topicCfgs, &s.ShardRepair.FailTopic)

	// start topic monitor
	monitorIntervalS := 1
	for _, topicCfg := range topicCfgs {
		m, err := base.NewKafkaTopicMonitor(topicCfg, access, monitorIntervalS)
		if err != nil {
			log.Errorf("new kafka topic monitor topic failed: topic[%s], err[%+v]", topicCfg.Topic, err)
			return err
		}
		go m.Run()
	}
	return nil
}
