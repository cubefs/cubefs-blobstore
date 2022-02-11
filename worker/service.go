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
	"math/rand"
	"time"

	blobnodeapi "github.com/cubefs/blobstore/api/blobnode"
	schedulerapi "github.com/cubefs/blobstore/api/scheduler"
	workerapi "github.com/cubefs/blobstore/api/worker"
	"github.com/cubefs/blobstore/cmd"
	"github.com/cubefs/blobstore/common/config"
	comErr "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/limit"
	"github.com/cubefs/blobstore/util/limit/count"
	"github.com/cubefs/blobstore/util/log"
	"github.com/cubefs/blobstore/worker/base"
	"github.com/cubefs/blobstore/worker/client"
)

var (
	service *Service
	conf    Config
)

func init() {
	mod := &cmd.Module{
		Name:       "WORKER",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "worker.conf")

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
	base.DroppedBidRecorderInst().Close()
	service.Close()
}

var errHostEmpty = errors.New("my_hosts should not be empty")

// ServiceRegisterConfig service register config
type ServiceRegisterConfig struct {
	Idc  string `json:"idc"`
	Host string `json:"host"`
}

// Config service config
type Config struct {
	cmd.Config

	ClusterID       proto.ClusterID       `json:"cluster_id"`
	ServiceRegister ServiceRegisterConfig `json:"service_register"`
	// max task run count of disk repair & balance & disk drop
	MaxTaskRunnerCnt int `json:"max_task_runner_cnt"`
	// tasklet concurrency of single repair task
	RepairConcurrency int `json:"repair_concurrency"`
	// tasklet concurrency of single balance task
	BalanceConcurrency int `json:"balance_concurrency"`
	// tasklet concurrency of single disk drop task
	DiskDropConcurrency int `json:"disk_drop_concurrency"`
	// tasklet concurrency of single manual migrate task
	ManualMigrateConcurrency int `json:"manual_migrate_concurrency"`
	// shard repair concurrency
	ShardRepairConcurrency int `json:"shard_repair_concurrency"`
	// volume inspect concurrency
	InspectConcurrency int `json:"inspect_concurrency"`

	// batch download concurrency of single tasklet
	DownloadShardConcurrency int `json:"download_shard_concurrency"`

	// small buffer pool use for shard repair
	SmallBufPool base.BufPoolConfig `json:"small_buf_pool"`
	// bid buffer pool use for disk repair & balance & disk drop task
	BigBufPool base.BufPoolConfig `json:"big_buf_pool"`

	// acquire task period
	AcquireIntervalMs int `json:"acquire_interval_ms"`

	// scheduler client config
	Scheduler schedulerapi.Config `json:"scheduler"`
	// blbonode client config
	BlobNode blobnodeapi.Config `json:"blobnode"`

	DroppedBidRecord *recordlog.Config `json:"dropped_bid_record"`
}

// Service worker service
type Service struct {
	taskRunnerMgr  *TaskRunnerMgr
	inspectTaskMgr *InspectTaskMgr
	taskRenter     *TaskRenter

	shardRepairLimit limit.Limiter
	shardRepairer    *ShardRepairer

	closeCh   chan struct{}
	acquireCh chan struct{}

	schedulerCli client.IScheduler
	blobNodeCli  client.IBlobNode
	Config
}

func (cfg *Config) checkAndFix() (err error) {
	if cfg.ServiceRegister.Host == "" {
		return errHostEmpty
	}

	fixConfigItemInt(&cfg.AcquireIntervalMs, 500)
	fixConfigItemInt(&cfg.MaxTaskRunnerCnt, 1)
	fixConfigItemInt(&cfg.RepairConcurrency, 1)
	fixConfigItemInt(&cfg.BalanceConcurrency, 1)
	fixConfigItemInt(&cfg.DiskDropConcurrency, 1)
	fixConfigItemInt(&cfg.ManualMigrateConcurrency, 10)
	fixConfigItemInt(&cfg.ShardRepairConcurrency, 1)
	fixConfigItemInt(&cfg.InspectConcurrency, 1)
	fixConfigItemInt(&cfg.DownloadShardConcurrency, 10)
	fixConfigItemInt(&cfg.SmallBufPool.PoolSize, 5)
	fixConfigItemInt(&cfg.SmallBufPool.BufSizeByte, 1048576)
	fixConfigItemInt(&cfg.BigBufPool.PoolSize, 5)
	fixConfigItemInt(&cfg.BigBufPool.BufSizeByte, 16777216)

	fixConfigItemInt64(&cfg.Scheduler.ClientTimeoutMs, 1000)
	fixConfigItemInt64(&cfg.BlobNode.ClientTimeoutMs, 1000)
	return nil
}

func fixConfigItemInt(actual *int, defaultVal int) {
	if *actual <= 0 {
		*actual = defaultVal
	}
}

func fixConfigItemInt64(actual *int64, defaultVal int64) {
	if *actual <= 0 {
		*actual = defaultVal
	}
}

// NewService returns rpc service
func NewService(cfg *Config) (*Service, error) {
	if err := cfg.checkAndFix(); err != nil {
		return nil, fmt.Errorf("check config: err[%w]", err)
	}

	base.BigBufPool = base.NewByteBufferPool(cfg.BigBufPool.BufSizeByte, cfg.BigBufPool.PoolSize)
	base.SmallBufPool = base.NewByteBufferPool(cfg.SmallBufPool.BufSizeByte, cfg.SmallBufPool.PoolSize)

	schedulerCli := client.NewSchedulerClient(&cfg.Scheduler)

	blobNodeCli := client.NewBlobNodeClient(&cfg.BlobNode)
	taskRunnerMgr := NewTaskRunnerMgr(
		cfg.DownloadShardConcurrency,
		cfg.RepairConcurrency,
		cfg.BalanceConcurrency,
		cfg.DiskDropConcurrency,
		cfg.ManualMigrateConcurrency,
		schedulerCli,
		&TaskWorkerCreator{})

	inspectTaskMgr := NewInspectTaskMgr(cfg.InspectConcurrency, blobNodeCli, schedulerCli)

	renewalCli := newRenewalCli(cfg.Scheduler)
	taskRenter := NewTaskRenter(cfg.ServiceRegister.Idc, renewalCli, taskRunnerMgr)

	shardRepairLimit := count.New(cfg.ShardRepairConcurrency)
	shardRepairer := NewShardRepairer(blobNodeCli, base.SmallBufPool)

	// init dropped bid record
	bidRecord := base.DroppedBidRecorderInst()
	err := bidRecord.Init(cfg.DroppedBidRecord, cfg.ClusterID)
	if err != nil {
		return nil, err
	}

	svr := &Service{
		schedulerCli:   schedulerCli,
		blobNodeCli:    blobNodeCli,
		taskRunnerMgr:  taskRunnerMgr,
		inspectTaskMgr: inspectTaskMgr,

		shardRepairLimit: shardRepairLimit,
		shardRepairer:    shardRepairer,

		taskRenter: taskRenter,
		acquireCh:  make(chan struct{}, 1),
		closeCh:    make(chan struct{}, 1),
		Config:     *cfg,
	}

	go svr.Run()

	return svr, nil
}

// NewHandler returns app server handler
func NewHandler(service *Service) *rpc.Router {
	rpc.RegisterArgsParser(&workerapi.ShardRepairArgs{}, "json")

	// POST /shard/repair
	// repair bid
	rpc.POST("/shard/repair", service.HTTPShardRepair, rpc.OptArgsBody())

	// GET /stats
	rpc.GET("/stats", service.HTTPStats)
	return rpc.DefaultRouter
}

// HTTPShardRepair repair shard
func (s *Service) HTTPShardRepair(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	ctx := trace.ContextWithSpan(c.Request.Context(), span)

	args := new(workerapi.ShardRepairArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	err := s.shardRepairLimit.Acquire()
	if err != nil {
		c.RespondError(err)
		return
	}
	defer s.shardRepairLimit.Release()

	err = s.shardRepairer.RepairShard(ctx, args.Task)
	c.RespondError(err)
}

// HTTPStats returns service stats
func (s *Service) HTTPStats(c *rpc.Context) {
	cancelCount, reclaimCount := base.WorkerStatsInst().Stats()
	ret := workerapi.Stats{
		CancelCount:  fmt.Sprint(cancelCount),
		ReclaimCount: fmt.Sprint(reclaimCount),
	}
	c.RespondJSON(ret)
}

func newRenewalCli(cfg schedulerapi.Config) client.IScheduler {
	// The timeout period must be strictly controlled
	cfg.ClientTimeoutMs = proto.RenewalTimeoutS * 1000
	return client.NewSchedulerClient(&cfg)
}

// Run runs backend task
func (s *Service) Run() {
	// service register
	go s.autoRegister()
	// task lease
	go s.taskRenter.RenewalTaskLoop()

	s.loopAcquireTask()
}

func (s *Service) loopAcquireTask() {
	go func() {
		ticker := time.NewTicker(time.Duration(s.AcquireIntervalMs) * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				s.notifyAcquire()
			case <-s.closeCh:
				return
			}
		}
	}()

	for {
		select {
		case <-s.acquireCh:
			s.tryAcquireTask()
		case <-s.closeCh:
			return
		}
	}
}

func (s *Service) notifyAcquire() {
	select {
	case s.acquireCh <- struct{}{}:
	default:
	}
}

// Close close service
func (s *Service) Close() {
	close(s.closeCh)
}

func (s *Service) tryAcquireTask() {
	if s.hasTaskRunnerResource() {
		s.acquireTask()
	}

	if s.hasInspectTaskResource() {
		s.acquireInspectTask()
	}
}

func (s *Service) hasTaskRunnerResource() bool {
	repair, balance, drop, manualMig := s.taskRunnerMgr.RunningTaskCnt()
	log.Infof("task count:repair %d balance %d drop %d manualMig %d max %d",
		repair, balance, drop, manualMig, s.MaxTaskRunnerCnt)
	return (repair + balance + drop + manualMig) < s.MaxTaskRunnerCnt
}

func (s *Service) hasInspectTaskResource() bool {
	inspectCnt := s.inspectTaskMgr.RunningTaskSize()
	log.Infof("inspect task count:inspectCnt %d max %d", inspectCnt, s.InspectConcurrency)
	return inspectCnt < s.InspectConcurrency
}

// acquire:disk repair & balance & disk drop task
func (s *Service) acquireTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "acquireTask")

	t, err := s.schedulerCli.AcquireTask(ctx, &schedulerapi.AcquireArgs{IDC: s.ServiceRegister.Idc})
	if err != nil {
		code := rpc.DetectStatusCode(err)
		if code != comErr.CodeNotingTodo {
			span.Errorf("acquire task failed: code[%d], err[%v]", code, err)
		}
		return
	}

	if !t.IsValid() {
		span.Errorf("task is illegal: task type[%s], disk drop[%+v], balance[%+v], repair[%+v], manual[%+v]",
			t.TaskType, t.DiskDrop, t.Balance, t.Repair, t.ManualMigrate)
		return
	}

	var taskID string
	switch t.TaskType {
	case proto.RepairTaskType:
		taskID = t.Repair.TaskID
		err = s.taskRunnerMgr.AddRepairTask(ctx, VolRepairTaskEx{
			taskInfo:                 t.Repair,
			downloadShardConcurrency: s.DownloadShardConcurrency,
			blobNodeCli:              s.blobNodeCli,
		})

	case proto.BalanceTaskType:
		taskID = t.Balance.TaskID
		err = s.taskRunnerMgr.AddBalanceTask(ctx, MigrateTaskEx{
			taskInfo:                 t.Balance,
			taskType:                 proto.BalanceTaskType,
			blobNodeCli:              s.blobNodeCli,
			downloadShardConcurrency: s.DownloadShardConcurrency,
		})

	case proto.DiskDropTaskType:
		taskID = t.DiskDrop.TaskID
		err = s.taskRunnerMgr.AddDiskDropTask(ctx, MigrateTaskEx{
			taskInfo:                 t.DiskDrop,
			taskType:                 proto.DiskDropTaskType,
			blobNodeCli:              s.blobNodeCli,
			downloadShardConcurrency: s.DownloadShardConcurrency,
		})
	case proto.ManualMigrateType:
		taskID = t.ManualMigrate.TaskID
		err = s.taskRunnerMgr.AddManualMigrateTask(ctx, MigrateTaskEx{
			taskInfo:                 t.ManualMigrate,
			taskType:                 proto.ManualMigrateType,
			blobNodeCli:              s.blobNodeCli,
			downloadShardConcurrency: s.DownloadShardConcurrency,
		})
	default:
		span.Fatalf("can not support task: type[%+v]", t.TaskType)
	}

	if err != nil {
		span.Errorf("add task failed: taskID[%s], err[%v]", taskID, err)
		return
	}
	span.Infof("acquire task success: task_type[%s], taskID[%s]", t.TaskType, taskID)
}

// acquire inspect task
func (s *Service) acquireInspectTask() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "acquireInspectTask")

	t, err := s.schedulerCli.AcquireInspectTask(ctx)
	if err != nil {
		code := rpc.DetectStatusCode(err)
		if code != comErr.CodeNotingTodo {
			span.Errorf("acquire inspect task failed: code[%d], err[%v]", code, err)
		}
		return
	}

	if !t.IsValid() {
		span.Errorf("inspect task is illegal: task[%+v]", t.Task)
		return
	}

	err = s.inspectTaskMgr.AddTask(ctx, t.Task)
	if err != nil {
		span.Errorf("add inspect task failed: taskID[%s], err[%v]", t.Task.TaskId, err)
		return
	}

	span.Infof("acquire inspect task success: taskID[%s]", t.Task.TaskId)
}

func (s *Service) autoRegister() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "autoRegister")

	args := schedulerapi.RegisterServiceArgs{
		ClusterID: s.ClusterID,
		Module:    proto.ServiceNameWorker,
		Host:      s.ServiceRegister.Host,
		Idc:       s.ServiceRegister.Idc,
	}

	var sleepTimeS time.Duration
	for {
		time.Sleep(sleepTimeS * time.Second)
		err := s.schedulerCli.RegisterService(ctx, &args)
		if err == nil {
			return
		}
		// 随机5-10分钟
		sleepTimeS = time.Duration(rand.Intn(5*60) + 5*60)
	}
}
