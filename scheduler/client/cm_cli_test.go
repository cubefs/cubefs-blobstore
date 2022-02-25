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
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/common/codemode"
	cmerrors "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/blobstore/util/log"
)

var (
	defaultVolumeListMarker = proto.Vid(0)
	defaultDiskListMarker   = proto.DiskID(0)
)

func init() {
	log.SetOutputLevel(log.Lfatal)
}

type mockCM struct {
	kv   map[string]string
	kvRW sync.RWMutex

	volumeMap   map[proto.Vid]*cmapi.VolumeInfo
	volumeSlice []*cmapi.VolumeInfo
	volRW       sync.RWMutex

	vunitMap map[proto.DiskID][]*cmapi.VolumeUnitInfo
	vunitRW  sync.RWMutex

	diskMap   map[proto.DiskID]*blobnode.DiskInfo
	diskSlice []*blobnode.DiskInfo
	diskRW    sync.RWMutex
}

func newMockCM() *mockCM {
	return &mockCM{
		kv:          make(map[string]string),
		volumeMap:   make(map[proto.Vid]*cmapi.VolumeInfo),
		volumeSlice: make([]*cmapi.VolumeInfo, 0),
		vunitMap:    make(map[proto.DiskID][]*cmapi.VolumeUnitInfo),
		diskMap:     make(map[proto.DiskID]*blobnode.DiskInfo),
		diskSlice:   make([]*blobnode.DiskInfo, 0),
	}
}

func (c *mockCM) GetConfig(ctx context.Context, key string) (ret string, err error) {
	c.kvRW.RLock()
	defer c.kvRW.RUnlock()

	val, ok := c.kv[key]
	if !ok {
		return "", cmerrors.ErrNotFound
	}
	return val, nil
}

func (c *mockCM) SetConfigInfo(ctx context.Context, args *cmapi.ConfigSetArgs) (err error) {
	c.kvRW.Lock()
	defer c.kvRW.Unlock()

	c.kv[args.Key] = args.Value
	return
}

func (c *mockCM) GetVolumeInfo(ctx context.Context, args *cmapi.GetVolumeArgs) (ret *cmapi.VolumeInfo, err error) {
	c.volRW.RLock()
	defer c.volRW.RUnlock()

	vol, ok := c.volumeMap[args.Vid]
	if !ok {
		return nil, cmerrors.ErrVolumeNotExist
	}

	return vol, nil
}

func (c *mockCM) AllocVolume(ctx context.Context, codeMode codemode.CodeMode, baseDiskID proto.DiskID) (ret *cmapi.VolumeInfo, err error) {
	c.volRW.Lock()
	defer c.volRW.Unlock()

	vid := proto.Vid(len(c.volumeMap) + 1)
	modeInfo := codeMode.Tactic()
	replicasNum := modeInfo.N + modeInfo.M + modeInfo.L
	units := make([]cmapi.Unit, replicasNum)
	for i := 0; i < replicasNum; i++ {
		vuid, _ := proto.NewVuid(vid, uint8(i), 1)
		diskInfo, err := c.DiskInfo(ctx, baseDiskID)
		if err != nil {
			return nil, err
		}
		unit := cmapi.Unit{
			Vuid:   vuid,
			DiskID: baseDiskID,
			Host:   diskInfo.Host,
		}
		units[i] = unit
		baseDiskID++
	}
	c.addVunits(units)

	ret = &cmapi.VolumeInfo{
		VolumeInfoBase: cmapi.VolumeInfoBase{
			Vid:      vid,
			CodeMode: codeMode,
		},
		Units: units,
	}
	c.volumeMap[vid] = ret
	c.volumeSlice = append(c.volumeSlice, ret)
	return
}

func (c *mockCM) addVunits(units []cmapi.Unit) {
	c.vunitRW.Lock()
	defer c.vunitRW.Unlock()

	for i := 0; i < len(units); i++ {
		if _, ok := c.vunitMap[units[i].DiskID]; !ok {
			var vunits []*cmapi.VolumeUnitInfo
			c.vunitMap[units[i].DiskID] = vunits
		}
		c.vunitMap[units[i].DiskID] = append(c.vunitMap[units[i].DiskID], &cmapi.VolumeUnitInfo{
			Vuid:   units[i].Vuid,
			DiskID: units[i].DiskID,
		})
	}
}

func (c *mockCM) LockVolume(ctx context.Context, args *cmapi.LockVolumeArgs) (err error) {
	c.volRW.Lock()
	defer c.volRW.Unlock()

	vol, ok := c.volumeMap[args.Vid]
	if !ok {
		return cmerrors.ErrVolumeNotExist
	}
	if vol.Status == proto.VolumeStatusLock {
		return nil
	}
	if vol.Status == proto.VolumeStatusActive {
		return cmerrors.ErrActiveVolume
	}
	vol.Status = proto.VolumeStatusLock
	return
}

func (c *mockCM) UnlockVolume(ctx context.Context, args *cmapi.UnlockVolumeArgs) (err error) {
	c.volRW.Lock()
	defer c.volRW.Unlock()

	vol, ok := c.volumeMap[args.Vid]
	if !ok {
		return cmerrors.ErrVolumeNotExist
	}

	if vol.Status == proto.VolumeStatusIdle {
		return nil
	}

	vol.Status = proto.VolumeStatusIdle
	return
}

func (c *mockCM) UpdateVolume(ctx context.Context, args *cmapi.UpdateVolumeArgs) (err error) {
	c.volRW.Lock()
	defer c.volRW.Unlock()

	idx := args.OldVuid.Index()
	vol, ok := c.volumeMap[args.OldVuid.Vid()]
	if !ok {
		return cmerrors.ErrVolumeNotExist
	}

	vol.Units[idx].Vuid = args.NewVuid
	vol.Units[idx].DiskID = args.NewDiskID

	return
}

func (c *mockCM) AllocVolumeUnit(ctx context.Context, args *cmapi.AllocVolumeUnitArgs) (ret *cmapi.AllocVolumeUnit, err error) {
	vid := args.Vuid.Vid()
	idx := args.Vuid.Index()
	epoch := args.Vuid.Epoch()
	epoch++
	newVuid, _ := proto.NewVuid(vid, idx, epoch)

	vol, ok := c.volumeMap[vid]
	if !ok {
		return nil, cmerrors.ErrVolumeNotExist
	}

	ret = &cmapi.AllocVolumeUnit{
		Vuid:   newVuid,
		DiskID: vol.Units[idx].DiskID,
	}
	return
}

func (c *mockCM) ReleaseVolumeUnit(ctx context.Context, args *cmapi.ReleaseVolumeUnitArgs) (err error) {
	c.volRW.RLock()
	defer c.volRW.RUnlock()

	_, ok := c.volumeMap[args.Vuid.Vid()]
	if !ok {
		return cmerrors.ErrNoSuchVuid
	}
	return
}

func (c *mockCM) ListVolumeUnit(ctx context.Context, args *cmapi.ListVolumeUnitArgs) (ret []*cmapi.VolumeUnitInfo, err error) {
	c.vunitRW.RLock()
	defer c.vunitRW.RUnlock()

	ret, ok := c.vunitMap[args.DiskID]
	if !ok {
		return nil, cmerrors.HTTPError(http.StatusNotFound, "", errors.New("disk not exist"))
	}

	return
}

func (c *mockCM) ListVolume(ctx context.Context, args *cmapi.ListVolumeArgs) (ret cmapi.ListVolumes, err error) {
	if args.Count > len(c.volumeSlice) {
		args.Count = len(c.volumeSlice)
	}
	marker := int(args.Marker)
	if marker > len(c.volumeSlice) {
		return cmapi.ListVolumes{
			Volumes: make([]*cmapi.VolumeInfo, 0),
			Marker:  defaultVolumeListMarker,
		}, nil
	}
	var volumes []*cmapi.VolumeInfo

	for len(volumes) < args.Count && marker < len(c.volumeSlice) {
		volumes = append(volumes, c.volumeSlice[marker])
		marker++
	}
	if marker >= len(c.volumeSlice) {
		marker = int(defaultVolumeListMarker)
	}
	ret.Volumes = volumes
	ret.Marker = proto.Vid(marker)

	return
}

func (c *mockCM) ListDisk(ctx context.Context, args *cmapi.ListOptionArgs) (ret cmapi.ListDiskRet, err error) {
	if args.Count > len(c.diskSlice) {
		args.Count = len(c.diskSlice)
	}

	marker := int(args.Marker)
	if marker > len(c.volumeSlice) {
		return cmapi.ListDiskRet{
			Disks:  make([]*blobnode.DiskInfo, 0),
			Marker: defaultDiskListMarker,
		}, nil
	}

	var volumes []*blobnode.DiskInfo
	if args.Status == diskStatusAll {
		for marker < len(c.diskSlice) {
			volumes = append(volumes, c.diskSlice[marker])
			marker++
			if len(volumes) >= args.Count {
				break
			}
		}
	} else {
		for marker < len(c.diskSlice) {
			if c.diskSlice[marker].Status == args.Status {
				volumes = append(volumes, c.diskSlice[marker])
			}
			marker++
			if len(volumes) >= args.Count {
				break
			}
		}
	}

	if marker >= len(c.volumeSlice) {
		marker = int(defaultVolumeListMarker)
	}
	ret.Disks = volumes
	ret.Marker = proto.DiskID(marker)
	return
}

func (c *mockCM) ListDroppingDisk(ctx context.Context) (ret []*blobnode.DiskInfo, err error) {
	for _, disk := range c.diskSlice {
		if disk.Status != proto.DiskStatusDropped {
			ret = append(ret, disk)
		}
	}
	return
}

func (c *mockCM) SetDisk(ctx context.Context, id proto.DiskID, status proto.DiskStatus) (err error) {
	c.diskRW.Lock()
	defer c.diskRW.Unlock()

	info, ok := c.diskMap[id]
	if !ok {
		return cmerrors.HTTPError(http.StatusNotFound, "", errors.New("disk not exist"))
	}

	info.Status = status
	return
}

func (c *mockCM) DroppedDisk(ctx context.Context, id proto.DiskID) (err error) {
	return c.SetDisk(ctx, id, proto.DiskStatusDropped)
}

func (c *mockCM) DiskInfo(ctx context.Context, id proto.DiskID) (ret *blobnode.DiskInfo, err error) {
	c.diskRW.RLock()
	defer c.diskRW.RUnlock()

	info, ok := c.diskMap[id]
	if !ok {
		return nil, cmerrors.HTTPError(http.StatusNotFound, "", errors.New("disk not exist"))
	}
	return info, nil
}

func (c *mockCM) AddDisk(ctx context.Context, diskID proto.DiskID, disk *blobnode.DiskInfo) (err error) {
	c.diskRW.Lock()
	defer c.diskRW.Unlock()
	if _, ok := c.diskMap[diskID]; !ok {
		c.diskSlice = append(c.diskSlice, disk)
	}
	c.diskMap[diskID] = disk
	return
}

var (
	cli   = newMockCM()
	cmCli = ClusterMgrClient{
		cli: cli,
	}
)

func TestCMClient(t *testing.T) {
	// log.SetOutputLevel(0)
	ctx := context.Background()
	initClusterDisks(ctx, cli)

	_, err := cmCli.GetConfig(ctx, proto.BalanceTaskType)
	require.Error(t, err)

	err = cli.SetConfigInfo(ctx, &cmapi.ConfigSetArgs{
		Key:   proto.BalanceTaskType,
		Value: taskswitch.SwitchOpen,
	})
	require.NoError(t, err)

	val, err := cmCli.GetConfig(ctx, proto.BalanceTaskType)
	require.NoError(t, err)
	require.Equal(t, taskswitch.SwitchOpen, val)

	_, err = cmCli.GetVolumeInfo(ctx, 0)
	require.Error(t, err)

	ret, err := cli.AllocVolume(ctx, codemode.EC6P10L2, 1)
	require.NoError(t, err)
	_, err = cmCli.GetVolumeInfo(ctx, ret.Vid)
	require.NoError(t, err)

	err = cmCli.LockVolume(ctx, ret.Vid)
	require.NoError(t, err)

	err = cmCli.UnlockVolume(ctx, ret.Vid)
	require.NoError(t, err)

	newVuidInfo, err := cmCli.AllocVolumeUnit(ctx, ret.Units[0].Vuid)
	require.NoError(t, err)

	err = cmCli.UpdateVolume(ctx, newVuidInfo.Vuid, ret.Units[0].Vuid, newVuidInfo.DiskID)
	require.NoError(t, err)

	volInfo, err := cmCli.GetVolumeInfo(ctx, ret.Vid)
	require.NoError(t, err)
	require.Equal(t, newVuidInfo.Vuid, volInfo.VunitLocations[0].Vuid)

	err = cmCli.ReleaseVolumeUnit(ctx, ret.Units[0].Vuid, ret.Units[0].DiskID)
	require.NoError(t, err)
	err = cmCli.ReleaseVolumeUnit(ctx, proto.Vuid(111111), ret.Units[0].DiskID)
	require.Error(t, err)

	vunits, err := cmCli.ListDiskVolumeUnits(ctx, ret.Units[1].DiskID)
	require.NoError(t, err)
	require.Equal(t, 1, len(vunits))

	err = cmCli.SetDiskRepairing(ctx, ret.Units[2].DiskID)
	require.NoError(t, err)
	disk, err := cmCli.GetDiskInfo(ctx, ret.Units[2].DiskID)
	require.NoError(t, err)
	require.Equal(t, proto.DiskStatusRepairing, disk.Status)

	err = cmCli.SetDiskRepaired(ctx, ret.Units[2].DiskID)
	require.NoError(t, err)
	disk, err = cmCli.GetDiskInfo(ctx, ret.Units[2].DiskID)
	require.NoError(t, err)
	require.Equal(t, proto.DiskStatusRepaired, disk.Status)

	err = cmCli.SetDiskDropped(ctx, ret.Units[2].DiskID)
	require.NoError(t, err)
	disk, err = cmCli.GetDiskInfo(ctx, ret.Units[2].DiskID)
	require.NoError(t, err)
	require.Equal(t, proto.DiskStatusDropped, disk.Status)
	err = cli.SetDisk(ctx, ret.Units[2].DiskID, proto.DiskStatusNormal)
	require.NoError(t, err)

	volumes, marker, err := cmCli.ListVolume(ctx, defaultVolumeListMarker, 2)
	require.NoError(t, err)
	require.Equal(t, 1, len(volumes))
	require.Equal(t, defaultVolumeListMarker, marker)

	_, err = cli.AllocVolume(ctx, codemode.EC6P6, 20)
	require.NoError(t, err)

	volumes, marker, err = cmCli.ListVolume(ctx, defaultVolumeListMarker, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(volumes))

	_, marker, err = cmCli.ListVolume(ctx, marker, 1)
	require.NoError(t, err)
	require.Equal(t, defaultVolumeListMarker, marker)

	disks, err := cmCli.ListClusterDisks(ctx)
	require.NoError(t, err)
	require.Equal(t, 100, len(disks))

	brokenDisk := disks[50]
	repairDisk := disks[55]
	dropDisk1 := disks[20]
	dropDisk2 := disks[21]
	/*
		dropDisk3 := disks[22]
		dropDisk4 := disks[23]
		dropDisk5 := disks[24]
	*/
	disks, err = cmCli.ListBrokenDisks(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 0, len(disks))
	cli.SetDisk(ctx, brokenDisk.DiskID, proto.DiskStatusBroken)
	disks, err = cmCli.ListBrokenDisks(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(disks))

	disks, err = cmCli.ListRepairingDisks(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(disks))
	cli.SetDisk(ctx, repairDisk.DiskID, proto.DiskStatusRepairing)
	disks, err = cmCli.ListRepairingDisks(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(disks))

	disks, err = cmCli.ListClusterDisks(ctx)
	require.NoError(t, err)
	require.Equal(t, 98, len(disks))

	disks, err = cmCli.ListDropDisks(ctx)
	require.NoError(t, err)
	require.Equal(t, 98, len(disks))

	cli.SetDisk(ctx, dropDisk1.DiskID, proto.DiskStatusDropped)
	cli.SetDisk(ctx, dropDisk2.DiskID, proto.DiskStatusDropped)

	disks, err = cmCli.ListDropDisks(ctx)
	require.NoError(t, err)
	require.Equal(t, 96, len(disks))
}

func initClusterDisks(ctx context.Context, cli *mockCM) {
	for i := 0; i < 100; i++ {
		diskHeartbeat := blobnode.DiskHeartBeatInfo{
			DiskID: proto.DiskID(i + 1),
		}
		disk := &blobnode.DiskInfo{ClusterID: 1, Status: proto.DiskStatusNormal, DiskHeartBeatInfo: diskHeartbeat, Host: uuid.New().String()}
		cli.AddDisk(ctx, disk.DiskID, disk)
	}
}
