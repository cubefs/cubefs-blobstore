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

package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/scheduler/base"
	api "github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/util/log"
)

const (
	defaultUpdateIntervalM = 5
)

type clusterTopoConf struct {
	ClusterID               proto.ClusterID
	UpdateIntervalMin       time.Duration
	FreeChunkCounterBuckets []float64
}

// TopologyCmCli define interface of clustermgr use by cluster topology
type TopologyCmCli interface {
	ListClusterDisks(ctx context.Context) (disks []*api.DiskInfoSimple, err error)
}

// ClusterTopology cluster topology
type ClusterTopology struct {
	clusterID    proto.ClusterID
	idcMap       map[string]*IDC
	diskMap      map[string][]*api.DiskInfoSimple
	FreeChunkCnt int64
	MaxChunkCnt  int64
}

// IDC idc info
type IDC struct {
	name         string
	rackMap      map[string]*Rack
	FreeChunkCnt int64
	MaxChunkCnt  int64
}

// Rack rack info
type Rack struct {
	name         string
	diskMap      map[string]*Host
	FreeChunkCnt int64
	MaxChunkCnt  int64
}

// Host host info
type Host struct {
	host         string                // ip+port
	disks        []*api.DiskInfoSimple // disk list
	FreeChunkCnt int64                 // host free chunk count
	MaxChunkCnt  int64                 // total chunk count
}

// ClusterTopologyMgr cluster topology manager
type ClusterTopologyMgr struct {
	updateInterval time.Duration
	clusterID      proto.ClusterID

	cmCli TopologyCmCli

	clusterTopo  *ClusterTopology
	taskStatsMgr *base.ClusterTopologyStatsMgr

	closeOnce *sync.Once
	done      chan struct{}
}

// NewClusterTopologyMgr returns cluster topology manager
func NewClusterTopologyMgr(client TopologyCmCli, conf *clusterTopoConf) *ClusterTopologyMgr {
	if conf.UpdateIntervalMin <= 0 {
		conf.UpdateIntervalMin = defaultUpdateIntervalM
	}

	mgr := &ClusterTopologyMgr{
		updateInterval: conf.UpdateIntervalMin,
		clusterID:      conf.ClusterID,

		cmCli: client,

		clusterTopo: &ClusterTopology{
			idcMap:  make(map[string]*IDC),
			diskMap: make(map[string][]*api.DiskInfoSimple),
		},
		taskStatsMgr: base.NewClusterTopologyStatisticsMgr(conf.ClusterID, conf.FreeChunkCounterBuckets),

		done:      make(chan struct{}, 1),
		closeOnce: &sync.Once{},
	}
	go mgr.loopUpdate()
	return mgr
}

func (m *ClusterTopologyMgr) loopUpdate() {
	t := time.NewTicker(m.updateInterval)
	for {
		select {
		case <-t.C:
			m.updateClusterTopo()
		case <-m.done:
			t.Stop()
			return
		}
	}
}

func (m *ClusterTopologyMgr) updateClusterTopo() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "updateClusterTopo")

	span.Debugf("update cluster topo")

	disks, err := m.cmCli.ListClusterDisks(ctx)
	if err != nil {
		log.Errorf("update cluster topology failed, err:%v", err)
	}

	m.buildClusterTopo(disks, m.clusterID)
}

// GetIDCs returns IDCs
func (m *ClusterTopologyMgr) GetIDCs() map[string]*IDC {
	return m.clusterTopo.idcMap
}

// GetIDCDisks returns disks with IDC
func (m *ClusterTopologyMgr) GetIDCDisks(idc string) (disks []*api.DiskInfoSimple, ok bool) {
	disks, ok = m.clusterTopo.diskMap[idc]
	return
}

// ReportFreeChunkCnt report free chunk cnt
func (m *ClusterTopologyMgr) ReportFreeChunkCnt(disk *api.DiskInfoSimple) {
	m.taskStatsMgr.ReportFreeChunk(disk)
}

// Close stop cluster topology manager
func (m *ClusterTopologyMgr) Close() {
	m.closeOnce.Do(func() {
		m.done <- struct{}{}
	})
}

func (m *ClusterTopologyMgr) buildClusterTopo(disks []*api.DiskInfoSimple, clusterID proto.ClusterID) {
	cluster := &ClusterTopology{
		clusterID: clusterID,
		idcMap:    make(map[string]*IDC),
		diskMap:   make(map[string][]*api.DiskInfoSimple),
	}

	for i := range disks {
		if cluster.clusterID != disks[i].ClusterID {
			log.Errorf("the disk does not belong to this cluster, clusterID:%d,diskInfo:%+v", cluster.clusterID, disks[i])
			continue
		}
		cluster.addDisk(disks[i])
		m.ReportFreeChunkCnt(disks[i])
	}

	for idc := range cluster.diskMap {
		sortDiskByFreeChunkCnt(cluster.diskMap[idc])
	}
	m.clusterTopo = cluster
}

func (cluster *ClusterTopology) addDisk(disk *api.DiskInfoSimple) {
	cluster.addDiskToCluster(disk)
	cluster.addDiskToDiskMap(disk)
	cluster.addDiskToIdc(disk)
	cluster.addDiskToRack(disk)
	cluster.addDiskToHost(disk)
}

func (cluster *ClusterTopology) addDiskToCluster(disk *api.DiskInfoSimple) {
	// statistics cluster chunk info
	cluster.FreeChunkCnt += disk.FreeChunkCnt
	cluster.MaxChunkCnt += disk.MaxChunkCnt
}

func (cluster *ClusterTopology) addDiskToDiskMap(disk *api.DiskInfoSimple) {
	if _, ok := cluster.diskMap[disk.Idc]; !ok {
		var disks []*api.DiskInfoSimple
		cluster.diskMap[disk.Idc] = disks
	}
	cluster.diskMap[disk.Idc] = append(cluster.diskMap[disk.Idc], disk)
}

func (cluster *ClusterTopology) addDiskToIdc(disk *api.DiskInfoSimple) {
	idcName := disk.Idc
	if _, ok := cluster.idcMap[idcName]; !ok {
		cluster.idcMap[idcName] = &IDC{
			name:    idcName,
			rackMap: make(map[string]*Rack),
		}
	}
	// statistics idc chunk info
	cluster.idcMap[idcName].FreeChunkCnt += disk.FreeChunkCnt
	cluster.idcMap[idcName].MaxChunkCnt += disk.MaxChunkCnt
}

func (cluster *ClusterTopology) addDiskToRack(disk *api.DiskInfoSimple) {
	idc := cluster.idcMap[disk.Idc]
	rackName := disk.Rack
	if _, ok := idc.rackMap[rackName]; !ok {
		idc.rackMap[rackName] = &Rack{
			name:    rackName,
			diskMap: make(map[string]*Host),
		}
	}
	// statistics rack chunk info
	idc.rackMap[rackName].FreeChunkCnt += disk.FreeChunkCnt
	idc.rackMap[rackName].MaxChunkCnt += disk.MaxChunkCnt
}

func (cluster *ClusterTopology) addDiskToHost(disk *api.DiskInfoSimple) {
	rack := cluster.idcMap[disk.Idc].rackMap[disk.Rack]
	if _, ok := rack.diskMap[disk.Host]; !ok {
		var disks []*api.DiskInfoSimple
		rack.diskMap[disk.Host] = &Host{
			host:  disk.Host,
			disks: disks,
		}
	}
	rack.diskMap[disk.Host].disks = append(rack.diskMap[disk.Host].disks, disk)

	// statistics host chunk info
	rack.diskMap[disk.Host].FreeChunkCnt += disk.FreeChunkCnt
	rack.diskMap[disk.Host].MaxChunkCnt += disk.MaxChunkCnt
}

func sortDiskByFreeChunkCnt(disks []*api.DiskInfoSimple) {
	sort.Slice(disks, func(i, j int) bool {
		return disks[i].FreeChunkCnt < disks[j].FreeChunkCnt
	})
}
