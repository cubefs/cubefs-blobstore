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
	"sync"

	"github.com/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/common/codemode"
	comErr "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/log"
)

var (
	defaultListDiskNum    = 1000
	defaultListDiskMarker = proto.DiskID(0)
	diskStatusAll         = proto.DiskStatus(0)
)

// VolumeInfoSimple volume info used by scheduler
type VolumeInfoSimple struct {
	Vid            proto.Vid             `json:"vid"`
	CodeMode       codemode.CodeMode     `json:"code_mode"`
	Status         proto.VolumeStatus    `json:"status"`
	VunitLocations []proto.VunitLocation `json:"vunit_locations"`
}

// IsIdle returns true if volume is idle
func (vol *VolumeInfoSimple) IsIdle() bool {
	return vol.Status == proto.VolumeStatusIdle
}

// IsActive returns true if volume is active
func (vol *VolumeInfoSimple) IsActive() bool {
	return vol.Status == proto.VolumeStatusActive
}

func (vol *VolumeInfoSimple) set(info *cmapi.VolumeInfo) {
	vol.Vid = info.Vid
	vol.CodeMode = info.CodeMode
	vol.Status = info.Status
	vol.VunitLocations = make([]proto.VunitLocation, len(info.Units))

	// check volume info
	codeModeInfo := info.CodeMode.Tactic()
	vunitCnt := codeModeInfo.N + codeModeInfo.M + codeModeInfo.L
	if len(info.Units) != vunitCnt {
		log.Panicf("volume %d info unexpect", info.Vid)
	}

	diskIDMap := make(map[proto.DiskID]struct{}, vunitCnt)
	for _, repl := range info.Units {
		if _, ok := diskIDMap[repl.DiskID]; ok {
			log.Panicf("vid %d many chunks on same disk", info.Vid)
		}
		diskIDMap[repl.DiskID] = struct{}{}
	}
	////enc check

	for i := 0; i < len(info.Units); i++ {
		vol.VunitLocations[i] = proto.VunitLocation{
			Vuid:   info.Units[i].Vuid,
			Host:   info.Units[i].Host,
			DiskID: info.Units[i].DiskID,
		}
	}
}

// AllocVunitInfo volume unit info for alloc
type AllocVunitInfo struct {
	proto.VunitLocation
}

// Location returns volume unit location
func (vunit *AllocVunitInfo) Location() proto.VunitLocation {
	return vunit.VunitLocation
}

func (vunit *AllocVunitInfo) set(info *cmapi.AllocVolumeUnit, host string) {
	vunit.Vuid = info.Vuid
	vunit.DiskID = info.DiskID
	vunit.Host = host
}

// VunitInfoSimple volume unit simple info
type VunitInfoSimple struct {
	Vuid   proto.Vuid   `json:"vuid"`
	DiskID proto.DiskID `json:"disk_id"`
	Host   string       `json:"host"`
	Used   uint64       `json:"used"`
}

func (vunit *VunitInfoSimple) set(info *cmapi.VolumeUnitInfo, host string) {
	vunit.Vuid = info.Vuid
	vunit.DiskID = info.DiskID
	vunit.Host = host
	vunit.Used = info.Used
}

// DiskInfoSimple disk simple info
type DiskInfoSimple struct {
	ClusterID    proto.ClusterID  `json:"cluster_id"`
	DiskID       proto.DiskID     `json:"disk_id"`
	Idc          string           `json:"idc"`
	Rack         string           `json:"rack"`
	Host         string           `json:"host"`
	Status       proto.DiskStatus `json:"status"`
	Readonly     bool             `json:"readonly"`
	UsedChunkCnt int64            `json:"used_chunk_cnt"`
	MaxChunkCnt  int64            `json:"max_chunk_cnt"`
	FreeChunkCnt int64            `json:"free_chunk_cnt"`
}

// IsHealth return true if disk is health
func (disk *DiskInfoSimple) IsHealth() bool {
	return disk.Status == proto.DiskStatusNormal
}

// IsBroken return true if disk is broken
func (disk *DiskInfoSimple) IsBroken() bool {
	return disk.Status == proto.DiskStatusBroken
}

// IsDropped return true if disk is dropped
func (disk *DiskInfoSimple) IsDropped() bool {
	return disk.Status == proto.DiskStatusDropped
}

// CanDropped  disk can drop when disk is normal or has repaired or has dropped
// for simplicity we not allow to set disk status dropped
// when disk is repairing
func (disk *DiskInfoSimple) CanDropped() bool {
	if disk.Status == proto.DiskStatusNormal ||
		disk.Status == proto.DiskStatusRepaired ||
		disk.Status == proto.DiskStatusDropped {
		return true
	}
	return false
}

func (disk *DiskInfoSimple) set(info *blobnode.DiskInfo) {
	disk.ClusterID = info.ClusterID
	disk.Idc = info.Idc
	disk.Rack = info.Rack
	disk.Host = info.Host
	disk.DiskID = info.DiskID
	disk.Status = info.Status
	disk.Readonly = info.Readonly
	disk.UsedChunkCnt = info.UsedChunkCnt
	disk.MaxChunkCnt = info.MaxChunkCnt
	disk.FreeChunkCnt = info.FreeChunkCnt
}

// IClusterMgr define the interface of clustermgr used by scheduler
type IClusterMgr interface {
	GetConfig(ctx context.Context, key string) (val string, err error)

	// volume
	GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *VolumeInfoSimple, err error)
	LockVolume(ctx context.Context, Vid proto.Vid) (err error)
	UnlockVolume(ctx context.Context, Vid proto.Vid) (err error)
	UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error)
	AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (ret *AllocVunitInfo, err error)
	ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID) (err error)
	ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (ret []*VunitInfoSimple, err error)
	ListVolume(ctx context.Context, vid proto.Vid, count int) (volInfo []*VolumeInfoSimple, retVid proto.Vid, err error)

	// disk
	ListClusterDisks(ctx context.Context) (disks []*DiskInfoSimple, err error)
	ListBrokenDisks(ctx context.Context, count int) (disks []*DiskInfoSimple, err error)
	ListRepairingDisks(ctx context.Context) (disks []*DiskInfoSimple, err error)
	ListDropDisks(ctx context.Context) (disks []*DiskInfoSimple, err error)
	SetDiskRepairing(ctx context.Context, diskID proto.DiskID) (err error)
	SetDiskRepaired(ctx context.Context, diskID proto.DiskID) (err error)
	SetDiskDropped(ctx context.Context, diskID proto.DiskID) (err error)
	GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *DiskInfoSimple, err error)
}

// IClusterManager define the interface of clustermgr
type IClusterManager interface {
	GetConfig(ctx context.Context, key string) (ret string, err error)
	GetVolumeInfo(ctx context.Context, args *cmapi.GetVolumeArgs) (ret *cmapi.VolumeInfo, err error)
	LockVolume(ctx context.Context, args *cmapi.LockVolumeArgs) (err error)
	UnlockVolume(ctx context.Context, args *cmapi.UnlockVolumeArgs) (err error)
	UpdateVolume(ctx context.Context, args *cmapi.UpdateVolumeArgs) (err error)
	AllocVolumeUnit(ctx context.Context, args *cmapi.AllocVolumeUnitArgs) (ret *cmapi.AllocVolumeUnit, err error)
	ReleaseVolumeUnit(ctx context.Context, args *cmapi.ReleaseVolumeUnitArgs) (err error)
	ListVolumeUnit(ctx context.Context, args *cmapi.ListVolumeUnitArgs) ([]*cmapi.VolumeUnitInfo, error)
	ListVolume(ctx context.Context, args *cmapi.ListVolumeArgs) (ret cmapi.ListVolumes, err error)
	ListDisk(ctx context.Context, args *cmapi.ListOptionArgs) (ret cmapi.ListDiskRet, err error)
	ListDroppingDisk(ctx context.Context) (ret []*blobnode.DiskInfo, err error)
	SetDisk(ctx context.Context, id proto.DiskID, status proto.DiskStatus) (err error)
	DiskInfo(ctx context.Context, id proto.DiskID) (ret *blobnode.DiskInfo, err error)
	DroppedDisk(ctx context.Context, id proto.DiskID) (err error)
}

// ClusterMgrClient clustermgr client
type ClusterMgrClient struct {
	cli    IClusterManager
	rwLock sync.RWMutex
}

// Init init cluster client
func (c *ClusterMgrClient) Init(conf *cmapi.Config) error {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	if c.cli != nil {
		return errors.New("client has init")
	}
	c.cli = cmapi.New(conf)
	return nil
}

// GetConfig returns config by config key
func (c *ClusterMgrClient) GetConfig(ctx context.Context, key string) (val string, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "GetConfig", pSpan.TraceID())

	span.Debugf("GetConfig args key %s", key)
	ret, err := c.cli.GetConfig(ctx, key)
	if err != nil {
		span.Errorf("Get fail err %+v", err)
		return
	}
	span.Debugf("Get ret %s", ret)
	return ret, err
}

// GetVolumeInfo returns volume info
func (c *ClusterMgrClient) GetVolumeInfo(ctx context.Context, vid proto.Vid) (*VolumeInfoSimple, error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "GetVolumeInfo", pSpan.TraceID())

	span.Debugf("GetVolumeInfo args vid %d", vid)
	info, err := c.cli.GetVolumeInfo(ctx, &cmapi.GetVolumeArgs{Vid: vid})
	if err != nil {
		span.Errorf("GetVolumeInfo fail err %+v", err)
		return nil, err
	}
	span.Debugf("GetVolumeInfo ret %+v", *info)
	ret := &VolumeInfoSimple{}
	ret.set(info)
	return ret, nil
}

// LockVolume lock volume
func (c *ClusterMgrClient) LockVolume(ctx context.Context, vid proto.Vid) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "LockVolume", pSpan.TraceID())

	span.Debugf("LockVolume args vid %d", vid)
	err = c.cli.LockVolume(ctx, &cmapi.LockVolumeArgs{Vid: vid})
	span.Debugf("LockVolume ret err %+v", err)
	return
}

// UnlockVolume unlock volume
func (c *ClusterMgrClient) UnlockVolume(ctx context.Context, vid proto.Vid) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "UnlockVolume", pSpan.TraceID())

	span.Debugf("UnlockVolume args vid %d", vid)
	err = c.cli.UnlockVolume(ctx, &cmapi.UnlockVolumeArgs{Vid: vid})
	span.Debugf("UnlockVolume ret err %+v", err)
	if rpc.DetectStatusCode(err) == comErr.CodeUnlockNotAllow {
		span.Infof("lock fail but deem lock success,err %+v ", err)
		return nil
	}

	return
}

// UpdateVolume update volume
func (c *ClusterMgrClient) UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "UpdateVolume", pSpan.TraceID())

	span.Infof("UpdateVolume args newVuid %d oldVuid %d newDiskID %d", newVuid, oldVuid, newDiskID)
	err = c.cli.UpdateVolume(ctx, &cmapi.UpdateVolumeArgs{NewVuid: newVuid, OldVuid: oldVuid, NewDiskID: newDiskID})
	span.Infof("UpdateVolume ret err %+v", err)
	return
}

// AllocVolumeUnit alloc volume unit
func (c *ClusterMgrClient) AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (*AllocVunitInfo, error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "AllocVolumeUnit", pSpan.TraceID())

	span.Debugf("AllocVolumeUnit args vuid %d", vuid)
	ret := &AllocVunitInfo{}
	info, err := c.cli.AllocVolumeUnit(ctx, &cmapi.AllocVolumeUnitArgs{Vuid: vuid})
	if err != nil {
		span.Errorf("AllocVolumeUnit fail err %+v", err)
		return nil, err
	}
	span.Debugf("AllocVolumeUnit ret %+v", *info)

	diskInfo, err := c.cli.DiskInfo(ctx, info.DiskID)
	if err != nil {
		return nil, err
	}
	span.Debugf("AllocVolumeUnit DiskInfo ret %+v", diskInfo)

	ret.set(info, diskInfo.Host)
	return ret, err
}

// ReleaseVolumeUnit release volume unit
func (c *ClusterMgrClient) ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "ReleaseVolumeUnit", pSpan.TraceID())

	span.Debugf("ReleaseVolumeUnit args vuid %d diskID %d", vuid, diskID)
	err = c.cli.ReleaseVolumeUnit(ctx, &cmapi.ReleaseVolumeUnitArgs{Vuid: vuid, DiskID: diskID})
	span.Debugf("ReleaseVolumeUnit ret err %+v", err)

	return
}

// ListDiskVolumeUnits list disk volume units
func (c *ClusterMgrClient) ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (rets []*VunitInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "ListDiskVolumeUnits", pSpan.TraceID())

	span.Debugf("ListDiskVolumeUnits.ListVolumeUnit args diskID %d", diskID)
	infos, err := c.cli.ListVolumeUnit(ctx, &cmapi.ListVolumeUnitArgs{DiskID: diskID})
	if err != nil {
		span.Errorf("ListDiskVolumeUnits.ListVolumeUnit fail err %+v", err)
		return nil, err
	}

	for idx, info := range infos {
		span.Debugf("ListDiskVolumeUnits.ListVolumeUnit ret idx %d info %+v", idx, *info)
	}

	span.Debugf("ListDiskVolumeUnits.DiskInfo args diskID %d", diskID)
	diskInfo, err := c.cli.DiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("ListDiskVolumeUnits.DiskInfo fail err %+v", err)
		return nil, err
	}
	span.Debugf("ListDiskVolumeUnits.DiskInfo ret %+v", *diskInfo)

	for _, info := range infos {
		ele := VunitInfoSimple{}
		ele.set(info, diskInfo.Host)
		rets = append(rets, &ele)
	}
	return rets, nil
}

// ListVolume list volume
func (c *ClusterMgrClient) ListVolume(ctx context.Context, marker proto.Vid, count int) (rets []*VolumeInfoSimple, nextVid proto.Vid, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "ListVolume", pSpan.TraceID())

	vols, err := c.cli.ListVolume(ctx, &cmapi.ListVolumeArgs{Marker: marker, Count: count})
	if err != nil {
		return
	}
	for index := range vols.Volumes {
		ret := &VolumeInfoSimple{}
		ret.set(vols.Volumes[index])
		rets = append(rets, ret)
	}
	nextVid = vols.Marker
	return
}

// ListClusterDisks list all disks
func (c *ClusterMgrClient) ListClusterDisks(ctx context.Context) (disks []*DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	return c.listAllDisks(ctx, proto.DiskStatusNormal)
}

// ListBrokenDisks list all broken disks
func (c *ClusterMgrClient) ListBrokenDisks(ctx context.Context, count int) (disks []*DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	return c.listDisks(ctx, proto.DiskStatusBroken, count)
}

// ListRepairingDisks list repairing disks
func (c *ClusterMgrClient) ListRepairingDisks(ctx context.Context) (disks []*DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	return c.listAllDisks(ctx, proto.DiskStatusRepairing)
}

func (c *ClusterMgrClient) listAllDisks(ctx context.Context, status proto.DiskStatus) (disks []*DiskInfoSimple, err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "listAllDisks", pSpan.TraceID())

	marker := defaultListDiskMarker
	for {
		args := &cmapi.ListOptionArgs{
			Status: status,
			Count:  defaultListDiskNum,
			Marker: marker,
		}
		selectDisks, selectMarker, err := c.listDisk(ctx, args)
		if err != nil {
			span.Errorf("ListDisk fail err %+v", err)
			return nil, err
		}

		marker = selectMarker
		disks = append(disks, selectDisks...)
		if marker == defaultListDiskMarker {
			break
		}
	}
	return
}

func (c *ClusterMgrClient) listDisks(ctx context.Context, status proto.DiskStatus, count int) (disks []*DiskInfoSimple, err error) {
	span := trace.SpanFromContextSafe(ctx)

	marker := defaultListDiskMarker
	needDiskCount := count
	for {
		args := &cmapi.ListOptionArgs{
			Status: status,
			Count:  needDiskCount,
			Marker: marker,
		}
		selectDisks, selectMarker, err := c.listDisk(ctx, args)
		if err != nil {
			span.Errorf("ListDisk fail err %+v", err)
			return nil, err
		}

		marker = selectMarker
		disks = append(disks, selectDisks...)
		needDiskCount -= len(disks)
		if marker == defaultListDiskMarker || needDiskCount <= 0 {
			break
		}
	}
	return
}

func (c *ClusterMgrClient) listDisk(ctx context.Context, args *cmapi.ListOptionArgs) (disks []*DiskInfoSimple, marker proto.DiskID, err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "listDisk", pSpan.TraceID())

	span.Debugf("ListDisk args %+v", *args)
	infos, err := c.cli.ListDisk(ctx, args)
	if err != nil {
		span.Errorf("ListDisk fail err %+v", err)
		return nil, defaultListDiskMarker, err
	}
	marker = infos.Marker
	for _, info := range infos.Disks {
		span.Debugf("ListDisk ret info %+v", *info)
		ele := DiskInfoSimple{}
		ele.set(info)
		disks = append(disks, &ele)
	}
	return
}

// ListDropDisks list drop disks, may contain {DiskStatusNormal,DiskStatusReadOnly,DiskStatusBroken,DiskStatusRepairing,DiskStatusRepaired} disks
func (c *ClusterMgrClient) ListDropDisks(ctx context.Context) (disks []*DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "ListDropDisks", pSpan.TraceID())

	infos, err := c.cli.ListDroppingDisk(ctx)
	if err != nil {
		span.Errorf("ListDropDisks fail err %+v", err)
		return nil, err
	}
	span.Infof("len disk %d", len(infos))
	for _, info := range infos {
		span.Debugf("ListDropDisks ret info %+v", *info)
		disk := DiskInfoSimple{}
		disk.set(info)
		span.Infof("disk status %d %s", disk.Status, disk.Status.String())
		if disk.IsHealth() {
			disks = append(disks, &disk)
		}
	}
	return disks, nil
}

// SetDiskRepairing set disk repairing
func (c *ClusterMgrClient) SetDiskRepairing(ctx context.Context, diskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("SetDiskRepairing args diskID %d status %d", diskID, proto.DiskStatusRepairing)
	err = c.setDiskStatus(ctx, diskID, proto.DiskStatusRepairing)
	span.Debugf("SetDiskRepairing ret err %+v", err)
	return
}

// SetDiskRepaired set disk repaired
func (c *ClusterMgrClient) SetDiskRepaired(ctx context.Context, diskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("SetDiskRepaired args diskID %d diskstatus %d", diskID, proto.DiskStatusRepaired)
	err = c.setDiskStatus(ctx, diskID, proto.DiskStatusRepaired)
	span.Debugf("SetDiskRepaired ret err %+v", err)
	return
}

// SetDiskDropped set disk dropped
func (c *ClusterMgrClient) SetDiskDropped(ctx context.Context, diskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()
	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "SetDiskDropped", pSpan.TraceID())

	info, err := c.cli.DiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("GetDiskInfo fail err %+v", err)
		return err
	}

	disk := &DiskInfoSimple{}
	disk.set(info)
	if disk.IsDropped() {
		return nil
	}

	if !disk.CanDropped() {
		return comErr.ErrCanNotDropped
	}

	span.Debugf("SetDiskDropped args diskID %d status %d", diskID, proto.DiskStatusDropped)
	err = c.cli.DroppedDisk(ctx, diskID)
	span.Debugf("SetDiskDropped ret err %+v", err)
	return
}

func (c *ClusterMgrClient) setDiskStatus(ctx context.Context, diskID proto.DiskID, status proto.DiskStatus) (err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "setDiskStatus", pSpan.TraceID())
	return c.cli.SetDisk(ctx, diskID, status)
}

// GetDiskInfo returns disk info
func (c *ClusterMgrClient) GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	pSpan := trace.SpanFromContextSafe(ctx)
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "GetDiskInfo", pSpan.TraceID())

	span.Debugf("GetDiskInfo args diskID %d", diskID)
	info, err := c.cli.DiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("GetDiskInfo fail err %+v", err)
		return nil, err
	}
	span.Debugf("GetDiskInfo ret info %+v", *info)
	ret = &DiskInfoSimple{}
	ret.set(info)
	return ret, nil
}

var (
	cmCliInst    *ClusterMgrClient
	newCmCliOnce sync.Once
)

// CmCliInst make sure only one instance in global
// only one cm client instance can exist in scheduler progress
func CmCliInst() *ClusterMgrClient {
	newCmCliOnce.Do(func() {
		cmCliInst = &ClusterMgrClient{rwLock: sync.RWMutex{}}
	})
	return cmCliInst
}
