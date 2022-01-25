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
	cmapi "github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/proto"
)

// ClusterMgrAPI defines the interface of clustermgr use by tinker
type ClusterMgrAPI interface {
	GetVolInfo(ctx context.Context, vid proto.Vid) (*VolInfo, error)
	ListVolume(ctx context.Context, afterVid proto.Vid, count int) ([]*VolInfo, proto.Vid, error)
	GetConfig(ctx context.Context, key string) (ret string, err error)
}

// VolInfo volume info
type VolInfo struct {
	Vid            proto.Vid             `json:"vid"`
	VunitLocations []proto.VunitLocation `json:"vunit_locations"`
	CodeMode       codemode.CodeMode     `json:"code_mode"`
	Status         proto.VolumeStatus    `json:"status"`
}

// EqualWith returns true if volInfo if eq. v
func (v VolInfo) EqualWith(volInfo VolInfo) bool {
	if len(v.VunitLocations) != len(volInfo.VunitLocations) {
		return false
	}
	if v.Vid != volInfo.Vid {
		return false
	}
	if v.CodeMode != volInfo.CodeMode {
		return false
	}
	if v.Status != volInfo.Status {
		return false
	}

	for i := range v.VunitLocations {
		if v.VunitLocations[i] != volInfo.VunitLocations[i] {
			return false
		}
	}

	return true
}

// ParseVolInfo parse VolumeInfo to VolInfo
func ParseVolInfo(info *clustermgr.VolumeInfo) (v *VolInfo) {
	v = &VolInfo{}
	v.Vid = info.Vid
	v.CodeMode = info.CodeMode
	v.Status = info.Status
	v.VunitLocations = make([]proto.VunitLocation, len(info.Units))

	// check volume info
	codeModeInfo := info.CodeMode.Tactic()
	vunitCnt := codeModeInfo.N + codeModeInfo.M + codeModeInfo.L
	if len(info.Units) != vunitCnt {
		panic("volume info unexpect")
	}

	for i := 0; i < len(info.Units); i++ {
		v.VunitLocations[i] = proto.VunitLocation{
			Vuid:   info.Units[i].Vuid,
			Host:   info.Units[i].Host,
			DiskID: info.Units[i].DiskID,
		}
	}
	return v
}

// IClusterManager define the interface of clustermgr used for manager ops
type IClusterManager interface {
	GetConfig(ctx context.Context, key string) (val string, err error)
	GetVolumeInfo(ctx context.Context, args *cmapi.GetVolumeArgs) (ret *cmapi.VolumeInfo, err error)
	ListVolume(ctx context.Context, args *cmapi.ListVolumeArgs) (ret cmapi.ListVolumes, err error)
}

// ClusterMgrClient clustermgr client
type ClusterMgrClient struct {
	client IClusterManager
}

// NewCmClient returns clustermgr client
func NewCmClient(cfg *clustermgr.Config) ClusterMgrAPI {
	return &ClusterMgrClient{client: clustermgr.New(cfg)}
}

// GetVolInfo returns volume info with volume id
func (c *ClusterMgrClient) GetVolInfo(ctx context.Context, vid proto.Vid) (*VolInfo, error) {
	v, err := c.client.GetVolumeInfo(ctx, &clustermgr.GetVolumeArgs{Vid: vid})
	if err != nil {
		return nil, err
	}
	ret := ParseVolInfo(v)
	return ret, nil
}

// ListVolume lists volume info
func (c *ClusterMgrClient) ListVolume(ctx context.Context, afterVid proto.Vid, count int) ([]*VolInfo, proto.Vid, error) {
	// todo cluster manager need change arg and return
	vols, err := c.client.ListVolume(ctx, &clustermgr.ListVolumeArgs{Count: count, Marker: afterVid})
	if err != nil {
		return nil, 0, err
	}
	ret := make([]*VolInfo, 0, len(vols.Volumes))
	for _, v := range vols.Volumes {
		ret = append(ret, ParseVolInfo(v))
	}
	return ret, vols.Marker, nil
}

// GetConfig returns config with config key
func (c *ClusterMgrClient) GetConfig(ctx context.Context, key string) (ret string, err error) {
	config, err := c.client.GetConfig(ctx, key)
	if err != nil {
		return "", err
	}
	return string(config), nil
}
