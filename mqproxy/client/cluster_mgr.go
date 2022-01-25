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

	"github.com/cubefs/blobstore/api/clustermgr"
)

// RegisterInfo register info use for clustermgr
type RegisterInfo struct {
	ClusterID          uint64 `json:"cluster_id"`
	Name               string `json:"name"`
	Host               string `json:"host"`
	Idc                string `json:"idc"`
	HeartbeatIntervalS uint32 `json:"heartbeat_interval_s"`
	HeartbeatTicks     uint32 `json:"heartbeat_ticks"`
	ExpiresTicks       uint32 `json:"expires_ticks"`
}

// Register register to clustermgr
type Register interface {
	Register(ctx context.Context, info RegisterInfo) error
}

// ClusterMgrClient clustermgr client used to register
type ClusterMgrClient struct {
	client *clustermgr.Client
}

// NewCmClient returns clustermgr client
func NewCmClient(cfg *clustermgr.Config) Register {
	return &ClusterMgrClient{clustermgr.New(cfg)}
}

// Register register service to clustermgr
func (c *ClusterMgrClient) Register(ctx context.Context, info RegisterInfo) error {
	node := clustermgr.ServiceNode{
		ClusterID: info.ClusterID,
		Name:      info.Name,
		Host:      info.Host,
		Idc:       info.Idc,
	}
	return c.client.RegisterService(ctx, node, info.HeartbeatIntervalS, info.HeartbeatTicks, info.ExpiresTicks)
}
