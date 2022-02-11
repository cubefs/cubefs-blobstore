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
	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/proto"
)

// ClusterMgrAPI defines the interface of clustermgr use by tinker
type ClusterMgrAPI interface {
	GetConfig(ctx context.Context, key string) (string, error)
	GetVolInfo(ctx context.Context, vid proto.Vid) (VolInfo, error)
	ListVolume(ctx context.Context, marker proto.Vid, count int) ([]VolInfo, proto.Vid, error)
}

// VolInfo volume info
type VolInfo struct {
	CodeMode       codemode.CodeMode     `json:"code_mode"`
	Status         proto.VolumeStatus    `json:"status"`
	Vid            proto.Vid             `json:"vid"`
	VunitLocations []proto.VunitLocation `json:"vunit_locations"`
}

// EqualWith returns whether equal with another.
func (v VolInfo) EqualWith(volInfo VolInfo) bool {
	if len(v.VunitLocations) != len(volInfo.VunitLocations) {
		return false
	}
	if v.Vid != volInfo.Vid ||
		v.CodeMode != volInfo.CodeMode ||
		v.Status != volInfo.Status {
		return false
	}
	for i := range v.VunitLocations {
		if v.VunitLocations[i] != volInfo.VunitLocations[i] {
			return false
		}
	}
	return true
}

// clusterMgrClient clustermgr client
type clusterMgrClient struct {
	client *clustermgr.Client
}

// NewClusterMgrClient returns clustermgr client
func NewClusterMgrClient(cfg *clustermgr.Config) ClusterMgrAPI {
	return &clusterMgrClient{client: clustermgr.New(cfg)}
}

// GetVolInfo returns volume info with volume id
func (c *clusterMgrClient) GetVolInfo(ctx context.Context, vid proto.Vid) (vol VolInfo, err error) {
	info, err := c.client.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid})
	if err != nil {
		return
	}
	vol = parseFrom(info)
	return
}

// ListVolume lists volume info
func (c *clusterMgrClient) ListVolume(ctx context.Context, marker proto.Vid, count int) ([]VolInfo, proto.Vid, error) {
	ret, err := c.client.ListVolume(ctx, &clustermgr.ListVolumeArgs{Marker: marker, Count: count})
	if err != nil {
		return nil, 0, err
	}
	vols := make([]VolInfo, 0, len(ret.Volumes))
	for _, info := range ret.Volumes {
		vols = append(vols, parseFrom(info))
	}
	return vols, ret.Marker, nil
}

// GetConfig returns config with config key
func (c *clusterMgrClient) GetConfig(ctx context.Context, key string) (ret string, err error) {
	config, err := c.client.GetConfig(ctx, key)
	if err != nil {
		return "", err
	}
	return string(config), nil
}

// parse volinfo from clustermgr volume
func parseFrom(info *clustermgr.VolumeInfo) (vol VolInfo) {
	// assert volume info
	tactic := info.CodeMode.Tactic()
	if len(info.Units) != tactic.N+tactic.M+tactic.L {
		panic("unexpected volume info from clustermgr")
	}

	vol = VolInfo{
		CodeMode:       info.CodeMode,
		Status:         info.Status,
		Vid:            info.Vid,
		VunitLocations: make([]proto.VunitLocation, len(info.Units)),
	}
	for i, unit := range info.Units {
		vol.VunitLocations[i] = proto.VunitLocation{
			Vuid:   unit.Vuid,
			Host:   unit.Host,
			DiskID: unit.DiskID,
		}
	}
	return
}
