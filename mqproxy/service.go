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

package mqproxy

import (
	"context"
	"fmt"

	"github.com/cubefs/blobstore/api/clustermgr"
	api "github.com/cubefs/blobstore/api/mqproxy"
	"github.com/cubefs/blobstore/cmd"
	"github.com/cubefs/blobstore/common/config"
	comerrs "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/kafka"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/mqproxy/client"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/log"
)

var (
	service *Service
	conf    Config
)

func init() {
	mod := &cmd.Module{
		Name:       "MQPROXY",
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterGracefulModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "mqproxy.conf")
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
	defaultHeartbeatIntervalS = 3
	defaultHeartbeatTicks     = 1
	defaultExpiresTicks       = 2
	defaultTimeoutMS          = 1000
)

// ErrIllegalTopic illegal topic
var ErrIllegalTopic = errors.New("illegal topic")

// MQConfig is mq config
type MQConfig struct {
	BlobDeleteTopic          string            `json:"blob_delete_topic"`
	ShardRepairTopic         string            `json:"shard_repair_topic"`
	ShardRepairPriorityTopic string            `json:"shard_repair_priority_topic"`
	MsgSender                kafka.ProducerCfg `json:"msg_sender"`
}

// ServiceRegisterConfig is service register info
type ServiceRegisterConfig struct {
	HeartbeatIntervalS uint32 `json:"heartbeat_interval_s"` // mqProxy heartbeat interval to ClusterManager
	HeartbeatTicks     uint32 `json:"heartbeat_ticks"`
	ExpiresTicks       uint32 `json:"expires_ticks"`
	Idc                string `json:"idc"`
	Host               string `json:"host"`
}

// Config is mqproxy service config
type Config struct {
	cmd.Config

	ClusterID       proto.ClusterID       `json:"cluster_id"`
	Clustermgr      clustermgr.Config     `json:"clustermgr"`
	MQ              MQConfig              `json:"mq"`
	ServiceRegister ServiceRegisterConfig `json:"service_register"`
}

func (c *Config) blobDeleteCfg() BlobDeleteConfig {
	return BlobDeleteConfig{
		Topic:        c.MQ.BlobDeleteTopic,
		MsgSenderCfg: c.MQ.MsgSender,
	}
}

func (c *Config) shardRepairCfg() ShardRepairConfig {
	return ShardRepairConfig{
		Topic:         c.MQ.ShardRepairTopic,
		PriorityTopic: c.MQ.ShardRepairPriorityTopic,
		MsgSenderCfg:  c.MQ.MsgSender,
	}
}

func (c *Config) checkAndFix() (err error) {
	// check topic cfg
	if c.MQ.BlobDeleteTopic == "" || c.MQ.ShardRepairTopic == "" || c.MQ.ShardRepairPriorityTopic == "" {
		return ErrIllegalTopic
	}

	if c.MQ.BlobDeleteTopic == c.MQ.ShardRepairTopic || c.MQ.BlobDeleteTopic == c.MQ.ShardRepairPriorityTopic {
		return ErrIllegalTopic
	}

	if c.ServiceRegister.HeartbeatIntervalS == 0 {
		c.ServiceRegister.HeartbeatIntervalS = defaultHeartbeatIntervalS
	}

	if c.ServiceRegister.HeartbeatTicks == 0 {
		c.ServiceRegister.HeartbeatTicks = defaultHeartbeatTicks
	}

	if c.ServiceRegister.ExpiresTicks == 0 {
		c.ServiceRegister.ExpiresTicks = defaultExpiresTicks
	}

	if c.Clustermgr.Config.ClientTimeoutMs <= 0 {
		c.Clustermgr.Config.ClientTimeoutMs = defaultTimeoutMS
	}

	if c.MQ.MsgSender.TimeoutMs <= 0 {
		c.MQ.MsgSender.TimeoutMs = defaultTimeoutMS
	}

	return nil
}

// Service is mqproxy service
type Service struct {
	Config
	clusterMgrClient client.Register
	shardRepairMgr   ShardRepairHandler
	blobDeleteMgr    BlobDeleteHandler
}

// NewService returns mqproxy service
func NewService(conf Config) (*Service, error) {
	if err := conf.checkAndFix(); err != nil {
		return nil, fmt.Errorf("config check: cfg[%+v], err:[%w]", conf, err)
	}

	clusterMgrClient := client.NewCmClient(&conf.Clustermgr)

	blobDeleteMgr, err := NewBlobDeleteMgr(conf.blobDeleteCfg())
	if err != nil {
		return nil, fmt.Errorf("new blob delete mgr: cfg[%+v], err:[%w]", conf.blobDeleteCfg(), err)
	}
	shardRepairMgr, err := NewShardRepairMgr(conf.shardRepairCfg())
	if err != nil {
		return nil, fmt.Errorf("new shard repair mgr: cfg[%+v], err:[%w]", conf.shardRepairCfg(), err)
	}

	service := &Service{
		Config:           conf,
		clusterMgrClient: clusterMgrClient,
		blobDeleteMgr:    blobDeleteMgr,
		shardRepairMgr:   shardRepairMgr,
	}

	err = service.register()
	if err != nil {
		return nil, fmt.Errorf("service register: err:[%w]", err)
	}
	return service, nil
}

// NewHandler returns app server handler
func NewHandler(service *Service) *rpc.Router {
	rpc.RegisterArgsParser(&api.ShardRepairArgs{}, "json")
	rpc.RegisterArgsParser(&api.DeleteArgs{}, "json")

	// POST /repairmsg
	// request body: json
	// response body: json
	rpc.POST("/repairmsg", service.SendRepairMessage, rpc.OptArgsBody())

	// POST /deletemsg
	// request body: json
	// response body: json
	rpc.POST("/deletemsg", service.SendDeleteMessage, rpc.OptArgsBody())

	return rpc.DefaultRouter
}

func (s *Service) register() error {
	info := client.RegisterInfo{
		ClusterID:          uint64(s.ClusterID),
		Name:               proto.MqProxySvrName,
		Host:               s.ServiceRegister.Host,
		Idc:                s.ServiceRegister.Idc,
		HeartbeatIntervalS: s.ServiceRegister.HeartbeatIntervalS,
		HeartbeatTicks:     s.ServiceRegister.HeartbeatTicks,
		ExpiresTicks:       s.ServiceRegister.ExpiresTicks,
	}
	return s.clusterMgrClient.Register(context.Background(), info)
}

// SendRepairMessage send repair message to kafka
// 1. message from access
// 2. message from scheduler
func (s *Service) SendRepairMessage(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	ctx := trace.ContextWithSpan(c.Request.Context(), span)

	args := new(api.ShardRepairArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if args.ClusterID != s.ClusterID {
		span.Errorf("clusterID not match: info[%+v], self clusterID[%d]", args, s.ClusterID)
		c.RespondError(comerrs.ErrClusterIDNotMatch)
		return
	}

	err := s.shardRepairMgr.SendShardRepairMsg(ctx, args)
	if err != nil {
		span.Errorf("send shard repair message failed: %+v", err)
		c.RespondError(err)
		return
	}

	c.Respond()
}

// SendDeleteMessage send delete message to kafka
// 1. message from access because of put object fail
// 2. message from access because of business side delete
func (s *Service) SendDeleteMessage(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	ctx := trace.ContextWithSpan(c.Request.Context(), span)

	args := new(api.DeleteArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if args.ClusterID != s.ClusterID {
		span.Errorf("clusterID not match: info[%+v], self clusterID[%d]", args, s.ClusterID)
		c.RespondError(comerrs.ErrClusterIDNotMatch)
		return
	}

	err := s.blobDeleteMgr.SendDeleteMsg(ctx, args)
	if err != nil {
		span.Errorf("send delete message failed: %+v", err)
		c.RespondError(err)
		return
	}

	c.Respond()
}
