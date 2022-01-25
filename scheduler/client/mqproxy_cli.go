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

package client

import (
	"context"

	cmapi "github.com/cubefs/blobstore/api/clustermgr"
	api "github.com/cubefs/blobstore/api/mqproxy"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
)

// IMqProxy define the interface of mqproxy used by scheduler
type IMqProxy interface {
	SendShardRepairMsg(ctx context.Context, vid proto.Vid, bid proto.BlobID, badIdx []uint8) error
}

// mqProxyClient mqproxy client
type mqProxyClient struct {
	cli       api.LbMsgSender
	clusterID proto.ClusterID
}

// NewMqProxyClient returns mqproxy client
func NewMqProxyClient(proxyConf *api.LbConfig, cmConf *cmapi.Config, clusterID proto.ClusterID) (IMqProxy, error) {
	cmcli := cmapi.New(cmConf)
	cli, err := api.NewLbClient(proxyConf, cmcli, clusterID)
	if err != nil {
		return nil, err
	}

	return &mqProxyClient{cli: cli, clusterID: clusterID}, nil
}

// SendShardRepairMsg send shard repair message
func (c *mqProxyClient) SendShardRepairMsg(ctx context.Context, vid proto.Vid, bid proto.BlobID, badIdx []uint8) error {
	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "SendShardRepairMsg", pSpan.TraceID())
	span.Debugf("send shard repair msg vid %d bid %d badIdx %+v", vid, bid, badIdx)

	err := c.cli.SendShardRepairMsg(ctx, &api.ShardRepairArgs{
		ClusterID: c.clusterID,
		Bid:       bid,
		Vid:       vid,
		BadIdxes:  badIdx,
		Reason:    "inspect",
	})

	span.Debugf("send shard repair msg ret err %+v", err)
	return err
}
