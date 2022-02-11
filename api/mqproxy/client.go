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
	"errors"
	"fmt"

	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/util/selector"
)

type LbMsgSender interface {
	SendDeleteMsg(ctx context.Context, args *DeleteArgs) error
	SendShardRepairMsg(ctx context.Context, args *ShardRepairArgs) error
}

type DeleteArgs struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Blobs     []BlobDelete    `json:"blobs"`
}

type BlobDelete struct {
	Bid proto.BlobID `json:"bid"`
	Vid proto.Vid    `json:"vid"`
}

type ShardRepairArgs struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Bid       proto.BlobID    `json:"bid"`
	Vid       proto.Vid       `json:"vid"`
	BadIdxes  []uint8         `json:"bad_idxes"`
	Reason    string          `json:"reason"`
}

type LbConfig struct {
	Config

	RetryHostsCnt      int   `json:"retry_hosts_cnt"`
	HostSyncIntervalMs int64 `json:"host_sync_interval_ms"`
}

type LbClient struct {
	MsgSender
	selector      selector.Selector
	retryHostsCnt int
}

func NewLbClient(cfg *LbConfig, cmClient *clustermgr.Client, clusterId proto.ClusterID) (LbMsgSender, error) {
	hostGetter := func() ([]string, error) {
		svrInfos, err := cmClient.GetService(context.Background(), clustermgr.GetServiceArgs{Name: proto.ServiceNameMQProxy})
		if err != nil {
			return nil, err
		}

		var hosts []string
		for _, s := range svrInfos.Nodes {
			if clusterId == proto.ClusterID(s.ClusterID) {
				hosts = append(hosts, s.Host)
			}
		}
		if len(hosts) == 0 {
			return nil, errors.New("service unavailable")
		}

		return hosts, nil
	}

	if cfg.HostSyncIntervalMs == 0 {
		cfg.HostSyncIntervalMs = 1000
	}

	if cfg.RetryHostsCnt == 0 {
		cfg.RetryHostsCnt = 1
	}
	hostSelector, err := selector.NewSelector(cfg.HostSyncIntervalMs, hostGetter)
	if err != nil {
		return nil, err
	}

	msgSender := &LbClient{
		retryHostsCnt: cfg.RetryHostsCnt,
		MsgSender:     NewClient(&cfg.Config),
		selector:      hostSelector,
	}
	return msgSender, nil
}

func (c *LbClient) SendDeleteMsg(ctx context.Context, args *DeleteArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	hosts := c.selector.GetRandomN(c.retryHostsCnt)
	for _, h := range hosts {
		err = c.MsgSender.SendDeleteMsg(ctx, h, args)
		if err == nil || !ShouldRetry(err) {
			return err
		}
		span.Errorf("send delete message failed, host: %s, args: %+v, err:%+v", h, args, err)
	}

	return err
}

func (c *LbClient) SendShardRepairMsg(ctx context.Context, args *ShardRepairArgs) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	ctx = trace.ContextWithSpan(ctx, span)

	hosts := c.selector.GetRandomN(c.retryHostsCnt)
	for _, h := range hosts {
		err = c.MsgSender.SendShardRepairMsg(ctx, h, args)
		if err == nil || !ShouldRetry(err) {
			return err
		}
		span.Errorf("seed shard repair failed, host: %s, args: %+v, err:%+v", h, args, err)
	}

	return err
}

var ShouldRetry = func(err error) bool {
	if err == nil {
		return false // success
	}
	_, ok := err.(rpc.HTTPError)
	return ok
}

type MsgSender interface {
	SendDeleteMsg(ctx context.Context, host string, info *DeleteArgs) error
	SendShardRepairMsg(ctx context.Context, host string, info *ShardRepairArgs) error
}

type Config struct {
	rpc.Config
}

type client struct {
	rpc.Client
}

func NewClient(cfg *Config) MsgSender {
	return &client{
		rpc.NewClient(&cfg.Config),
	}
}

func (m *client) SendShardRepairMsg(ctx context.Context, host string, args *ShardRepairArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	ctx = trace.ContextWithSpan(ctx, span)

	urlStr := fmt.Sprintf("%v/repairmsg", host)
	return m.PostWith(ctx, urlStr, nil, args)
}

func (m *client) SendDeleteMsg(ctx context.Context, host string, args *DeleteArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	ctx = trace.ContextWithSpan(ctx, span)

	urlStr := fmt.Sprintf("%v/deletemsg", host)
	return m.PostWith(ctx, urlStr, nil, args)
}
