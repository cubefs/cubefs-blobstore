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

package allocator

import (
	"context"
	"encoding/json"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/blobstore/api/allocator"
	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/common/codemode"
	apierrors "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/retry"
)

const (
	defaultRetainIntervalS     = int64(180)
	defaultAllocVolsNum        = 1
	defaultTotalThresholdRatio = 0.6
	defaultInitVolumeNum       = 4
	defaultMetricIntervalS     = 60
	allocableFlag              = 1
	notAllocableFlag           = 0
)

type VolConfig struct {
	ClusterID             uint64  `json:"cluster_id"`
	Idc                   string  `json:"idc"`
	RetainIntervalS       int64   `json:"retain_interval_s"`
	DefaultAllocVolsNum   int     `json:"default_alloc_vols_num"`
	InitVolumeNum         int     `json:"init_volume_num"`
	TotalThresholdRatio   float64 `json:"total_threshold_ratio"`
	MetricReportIntervalS int     `json:"metric_report_interval_s"`
	VolumeReserveSize     int     `json:"-"`
}

type ModeInfo struct {
	volumes        *volumes
	totalThreshold uint64
	totalFree      uint64
}

type allocArgs struct {
	isInit   bool
	codeMode codemode.CodeMode
	count    int
}

type volumeMgr struct {
	BlobConfig
	VolConfig

	BidMgr

	clusterMgr clustermgr.APIAllocator
	modeInfos  map[codemode.CodeMode]*ModeInfo
	mu         sync.RWMutex
	allocChs   map[codemode.CodeMode]chan *allocArgs
	allocFlag  int32
	preIdx     uint64
	closed     chan struct{}
}

func volConfCheck(cfg *VolConfig) {
	if cfg.DefaultAllocVolsNum == 0 {
		cfg.DefaultAllocVolsNum = defaultAllocVolsNum
	}
	if cfg.RetainIntervalS == 0 {
		cfg.RetainIntervalS = defaultRetainIntervalS
	}
	if cfg.TotalThresholdRatio == 0 {
		cfg.TotalThresholdRatio = defaultTotalThresholdRatio
	}
	if cfg.InitVolumeNum == 0 {
		cfg.InitVolumeNum = defaultInitVolumeNum
	}
	if cfg.MetricReportIntervalS == 0 {
		cfg.MetricReportIntervalS = defaultMetricIntervalS
	}
}

type VolumeMgr interface {
	// Allocate the required volumes to access module
	Alloc(ctx context.Context, args *allocator.AllocVolsArgs) (allocVols []allocator.AllocRet, err error)
	// List the volumes in the allocator
	List(ctx context.Context, codeMode codemode.CodeMode) (vids []proto.Vid, volumes []clustermgr.AllocVolumeInfo, err error)
}

func NewVolumeMgr(ctx context.Context, blobCfg BlobConfig, volCfg VolConfig, clusterMgr clustermgr.APIAllocator) (VolumeMgr, error) {
	span := trace.SpanFromContextSafe(ctx)
	volConfCheck(&volCfg)
	bidMgr, err := NewBidMgr(ctx, blobCfg, clusterMgr)
	if err != nil {
		span.Fatalf("fail to new bidMgr, error:%v", err)
	}
	rand.Seed(int64(time.Now().Nanosecond()))
	v := &volumeMgr{
		clusterMgr: clusterMgr,
		modeInfos:  make(map[codemode.CodeMode]*ModeInfo),
		allocChs:   make(map[codemode.CodeMode]chan *allocArgs),
		BidMgr:     bidMgr,
		mu:         sync.RWMutex{},
		BlobConfig: blobCfg,
		VolConfig:  volCfg,
		closed:     make(chan struct{}),
	}
	atomic.StoreInt32(&v.allocFlag, allocableFlag)
	atomic.StoreUint64(&v.preIdx, rand.Uint64())
	err = v.initModeInfo(ctx)
	if err != nil {
		return nil, err
	}

	go v.retainTask()
	go v.metricReportTask()

	return v, err
}

func (v *volumeMgr) initModeInfo(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	volumeReserveSize, err := v.clusterMgr.GetConfig(ctx, proto.VolumeReserveSizeKey)
	if err != nil {
		return errors.Info(err, "Get volume_reserve_size config from clusterMgr err").Detail(err)
	}
	v.VolConfig.VolumeReserveSize, err = strconv.Atoi(volumeReserveSize)
	if err != nil {
		return errors.Info(err, "strconv.Atoi volumeReserveSize err").Detail(err)
	}
	volumeChunkSize, err := v.clusterMgr.GetConfig(ctx, proto.VolumeChunkSizeKey)
	if err != nil {
		return errors.Info(err, "Get volume_chunk_size config from clusterMgr err").Detail(err)
	}
	volumeChunkSizeInt, err := strconv.Atoi(volumeChunkSize)
	if err != nil {
		return errors.Info(err, "strconv.Atoi volumeChunkSize err").Detail(err)
	}
	codeModeInfos, err := v.clusterMgr.GetConfig(ctx, proto.CodeModeConfigKey)
	if err != nil {
		return errors.Info(err, "Get code_mode config from clusterMgr err").Detail(err)
	}
	codeModeConfigInfos := make([]codemode.Policy, 0)
	err = json.Unmarshal([]byte(codeModeInfos), &codeModeConfigInfos)
	if err != nil {
		return errors.Info(err, "json.Unmarshal code_mode policy err").Detail(err)
	}
	for _, codeModeConfig := range codeModeConfigInfos {
		allocCh := make(chan *allocArgs)
		codeMode := codeModeConfig.ModeName.GetCodeMode()
		if !codeModeConfig.Enable {
			continue
		}
		v.allocChs[codeMode] = allocCh
		tactic := codeMode.Tactic()
		threshold := float64(v.InitVolumeNum*tactic.N*volumeChunkSizeInt) * v.TotalThresholdRatio
		modeVols := initModeVolumes()
		modeInfo := &ModeInfo{
			volumes:        modeVols,
			totalThreshold: uint64(threshold),
		}
		v.modeInfos[codeMode] = modeInfo
		span.Infof("code_mode:%v,initVolumeNum:%v,threshold:%v", codeModeConfig.ModeName, v.InitVolumeNum, threshold)
	}

	for mode := range v.allocChs {
		applyArg := &allocArgs{
			isInit:   true,
			codeMode: mode,
			count:    v.InitVolumeNum,
		}
		go v.allocVolumeLoop(mode)
		v.allocChs[mode] <- applyArg
	}
	return
}

func initModeVolumes() (modeVol *volumes) {
	modeVol = &volumes{
		num:   entryNum,
		locks: make(map[uint32]*sync.RWMutex),
		m:     make(map[uint32]map[proto.Vid]*volume),
	}

	for i := uint32(0); i < modeVol.num; i++ {
		mu := sync.RWMutex{}
		modeVol.locks[i] = &mu
		entry := make(map[proto.Vid]*volume)
		modeVol.m[i] = entry
	}
	return modeVol
}

func (v *volumeMgr) Alloc(ctx context.Context, args *allocator.AllocVolsArgs) (allocRets []allocator.AllocRet, err error) {
	allocBidScopes, err := v.BidMgr.Alloc(ctx, args.BidCount)
	if err != nil {
		return nil, err
	}
	vid, err := v.allocVid(ctx, args)
	if err != nil {
		return nil, err
	}
	allocRets = make([]allocator.AllocRet, 0, 128)
	for _, bidScope := range allocBidScopes {
		volRet := allocator.AllocRet{
			BidStart: bidScope.StartBid,
			BidEnd:   bidScope.EndBid,
			Vid:      vid,
		}
		allocRets = append(allocRets, volRet)
	}

	return
}

func (v *volumeMgr) List(ctx context.Context, codeMode codemode.CodeMode) (vids []proto.Vid, volumes []clustermgr.AllocVolumeInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)
	modeInfo, ok := v.modeInfos[codeMode]
	if !ok {
		return nil, nil, apierrors.ErrNoAvaliableVolume
	}
	vids = make([]proto.Vid, 0, 128)
	volumes = make([]clustermgr.AllocVolumeInfo, 0, 128)
	modeInfo.volumes.Range(func(vol *volume) {
		vol.mu.RLock()
		vids = append(vids, vol.Vid)
		volumes = append(volumes, vol.AllocVolumeInfo)
		vol.mu.RUnlock()
	})
	span.Debugf("[list]code mode:%v,avaliable volumes:%v,count:%v", codeMode, volumes, len(volumes))
	return
}

func (v *volumeMgr) getNextVid(ctx context.Context, vids []proto.Vid, modeInfo *ModeInfo, args *allocator.AllocVolsArgs) (proto.Vid, error) {
	span := trace.SpanFromContextSafe(ctx)
	curIdx := int(atomic.AddUint64(&v.preIdx, uint64(1)) % uint64(len(vids)))
	span.Debugf("v.preIdx:%v,curIdx:%v", v.preIdx, curIdx)
	l := len(vids) + curIdx
	for i := curIdx; i < l; i++ {
		idx := i % len(vids)
		if v.modifySpace(ctx, vids[idx], modeInfo, args) {
			return vids[idx], nil
		}
	}
	return 0, apierrors.ErrNoAvailableVolume
}

func (v *volumeMgr) modifySpace(ctx context.Context, vid proto.Vid, modeInfo *ModeInfo, args *allocator.AllocVolsArgs) bool {
	span := trace.SpanFromContextSafe(ctx)
	if len(args.Excludes) != 0 {
		for _, id := range args.Excludes {
			if vid == id {
				return false
			}
		}
	}
	volInfo, ok := modeInfo.volumes.Get(vid)
	volInfo.mu.Lock()
	if !ok || volInfo.Free < args.Fsize {
		span.Warnf("reselect vid:%v, free:%v, size:%v", vid, volInfo.Free, args.Fsize)
		volInfo.mu.Unlock()
		return false
	}
	volInfo.Free -= args.Fsize
	volInfo.Used += args.Fsize
	span.Debugf("selectVid:%v,this vid alloced Size:%v,freeSize:%v,reserve size:%v", vid, volInfo.Used,
		volInfo.Free, v.VolumeReserveSize)
	deleteFlag := false
	if volInfo.Free < uint64(v.VolumeReserveSize) {
		span.Infof("volume is full, remove vid:%v", volInfo.Vid)
		volInfo.isDeleted = true
		atomic.AddUint64(&modeInfo.totalFree, -volInfo.Free)
		deleteFlag = true
	}
	volInfo.mu.Unlock()
	if deleteFlag {
		modeInfo.volumes.Delete(proto.Vid(vid))
	}
	return true
}

func (v *volumeMgr) allocVid(ctx context.Context, args *allocator.AllocVolsArgs) (proto.Vid, error) {
	span := trace.SpanFromContextSafe(ctx)
	modeInfo := v.modeInfos[args.CodeMode]
	if modeInfo == nil {
		return 0, apierrors.ErrNoAvaliableVolume
	}
	vids, err := v.getAvailableVols(ctx, args)
	if err != nil {
		return 0, err
	}
	sort.Slice(vids, func(i, j int) bool {
		return int(vids[i]) < int(vids[j])
	})
	span.Debugf("code mode:%v,avaliable volumes:%v", args.CodeMode, vids)
	vid, err := v.getNextVid(ctx, vids, modeInfo, args)
	if err != nil {
		return 0, err
	}
	if atomic.AddUint64(&modeInfo.totalFree, -args.Fsize) < modeInfo.totalThreshold {
		span.Infof("less than threshold")
		v.allocNotify(ctx, args.CodeMode, v.DefaultAllocVolsNum)
	}
	span.Debugf("code_mode:%v,modeInfo.totalFree:%v,modeInfo.totalThreshold:%v", args.CodeMode,
		atomic.LoadUint64(&modeInfo.totalFree), atomic.LoadUint64(&modeInfo.totalThreshold))
	return vid, nil
}

func (v *volumeMgr) getAvailableVols(ctx context.Context, args *allocator.AllocVolsArgs) (vids []proto.Vid, err error) {
	modeInfo := v.modeInfos[args.CodeMode]
	if len(args.Discards) != 0 {
		discards := args.Discards
		for _, vid := range discards {
			if vol, ok := modeInfo.volumes.Get(vid); ok {
				vol.mu.Lock()
				if vol.isDeleted {
					vol.mu.Unlock()
					continue
				}
				atomic.AddUint64(&modeInfo.totalFree, -vol.Free)
				vol.isDeleted = true
				vol.mu.Unlock()
				modeInfo.volumes.Delete(vid)
			}
		}
	}
	vols := make([]proto.Vid, 0, 128)
	modeInfo.volumes.Range(func(vol *volume) {
		vid := vol.Vid
		vol.mu.RLock()
		if vol.Free >= args.Fsize && !vol.isDeleted {
			vols = append(vols, vid)
		}
		vol.mu.RUnlock()
	})

	if len(vols) == 0 {
		v.allocNotify(ctx, args.CodeMode, v.DefaultAllocVolsNum)
		return nil, apierrors.ErrNoAvaliableVolume
	}

	return vols, nil
}

// send message to apply channel, apply volume from CM
func (v *volumeMgr) allocNotify(ctx context.Context, mode codemode.CodeMode, count int) {
	span := trace.SpanFromContextSafe(ctx)
	applyArg := &allocArgs{
		codeMode: mode,
		count:    count,
	}
	if atomic.CompareAndSwapInt32(&v.allocFlag, allocableFlag, notAllocableFlag) {
		v.allocChs[mode] <- applyArg
		span.Infof("allocNotify success:%#v", applyArg)
	}
}

func (v *volumeMgr) allocVolume(ctx context.Context, args *clustermgr.AllocVolumeArgs) (ret []clustermgr.AllocVolumeInfo,
	err error) {
	span := trace.SpanFromContextSafe(ctx)
	err = retry.ExponentialBackoff(2, 200).On(func() error {
		allocVolumes, err := v.clusterMgr.AllocVolume(ctx, args)
		span.Infof("alloc volume from clusterMgr:%#v,err:%v", allocVolumes, err)
		if err == nil && len(allocVolumes.AllocVolumeInfos) != 0 {
			ret = allocVolumes.AllocVolumeInfos
		}
		return err
	})
	if err != nil {
		return nil, errors.New("allocVolume from clusterMgr error")
	}
	return ret, err
}

func (v *volumeMgr) allocVolumeLoop(mode codemode.CodeMode) {
	for {
		args := <-v.allocChs[mode]
		span, ctx := trace.StartSpanFromContext(context.Background(), "")
		requireCount := args.count
		for {
			allocArg := &clustermgr.AllocVolumeArgs{
				IsInit:   args.isInit,
				CodeMode: args.codeMode,
				Count:    requireCount,
			}
			span.Infof("allocVolumeLoop arguments:%#v", allocArg)
			volumeRets, err := v.allocVolume(ctx, allocArg)
			if err != nil || len(volumeRets) == 0 {
				time.Sleep(time.Duration(10) * time.Second)
				args.isInit = false
				continue
			}
			for _, vol := range volumeRets {
				if vol.CodeMode != allocArg.CodeMode {
					continue
				}
				allocVolInfo := &volume{
					AllocVolumeInfo: vol,
				}
				v.modeInfos[allocArg.CodeMode].volumes.Put(allocVolInfo)
				atomic.AddUint64(&v.modeInfos[allocArg.CodeMode].totalFree, vol.Free)
			}
			if len(volumeRets) < requireCount {
				span.Debugf("clusterMgr volume num not enough.code_mode:%v, need:%v, got:%v", allocArg.CodeMode,
					requireCount, len(volumeRets))
				requireCount -= len(volumeRets)
				args.isInit = false
				continue
			}
			atomic.StoreInt32(&v.allocFlag, allocableFlag)
			break
		}
	}
}
