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

package blobnode

import (
	"context"
	"sync"
	"time"

	cmapi "github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
)

func (s *Service) loopGcRubbishChunkFile() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "GcRubbish")
	span.Infof("loop gc rubbish chunk file")

	ticker := time.NewTicker(time.Duration(s.Conf.ChunkGcIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			span.Warnf("loop gc rubbish chunk file done.")
			return
		case <-ticker.C:
			s.GcRubbishChunk()
		}
	}
}

/* monitor chunkfile
 * 1. find miss: chunk file lost. report to ums
 * 2. find rubbish: no metadata, only data
 * 3. find rubbish: old epoch
 */

func (s *Service) GcRubbishChunk() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", base.BackgroudReqID("GcRubbish"))

	span.Debugf("check rubbish chunk file start == ")
	defer span.Debugf("check rubbish chunk file end == ")

	// Step: clean rubbish chunk
	s.checkAndCleanRubbish(ctx)

	// Step: clean old epoch chunk
	s.checkAndCleanGarbageEpoch(ctx)
}

func (s *Service) checkAndCleanRubbish(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("check and clean rubbish start ==")
	defer span.Infof("chunk and clean rubbish end ==")

	disks := s.copyDiskStorages(ctx)

	// Step: check one disk.
	wg := &sync.WaitGroup{}
	for _, ds := range disks {
		if ds.Status() != proto.DiskStatusNormal {
			span.Debugf("disk(%v) is not normal, do not need to check chunk file", ds.ID())
			continue
		}
		wg.Add(1)
		go func(ds core.DiskAPI) {
			defer wg.Done()
			err := s.checkAndCleanDiskRubbish(ctx, ds)
			if err != nil {
				span.Errorf("%v check local chunk file failed: %v", ds.GetConfig().Path, err)
			}
		}(ds)
	}
	wg.Wait()
}

func (s *Service) checkAndCleanDiskRubbish(ctx context.Context, ds core.DiskAPI) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("disk:%v check and clean rubbish", ds.ID())

	// list chunks from meta
	mayBeLost, err := ds.GcRubbishChunk(ctx)
	if err != nil {
		span.Errorf("%s list chunks failed: %v", ds.GetMetaPath(), err)
		return
	}

	if len(mayBeLost) > 0 {
		span.Errorf("such chunk lost data: %v", mayBeLost)
		// lost chunk data, need manual intervention
		// todo: report to ums
		panic(err)
	}

	return
}

func (s *Service) checkAndCleanGarbageEpoch(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("check and clean garbage epoch chunk")

	chunks := s.copyChunkStorages(ctx)

	for _, cs := range chunks {
		disk := cs.Disk()
		vuid := cs.Vuid()
		vid := vuid.Vid()
		index := vuid.Index()
		localEpoch := vuid.Epoch()

		time.Sleep(time.Second)

		volumeInfo, err := s.ClusterMgrClient.GetVolumeInfo(ctx, &cmapi.GetVolumeArgs{Vid: vid})
		if err != nil {
			span.Errorf("get volume(%v) info failed: %v", vid, err)
			continue
		}

		if index >= uint8(len(volumeInfo.Units)) {
			span.Errorf("vuid number does not match, volumeInfo:%v", volumeInfo)
			continue
		}

		clusterMgrEpoch := volumeInfo.Units[index].Vuid.Epoch()
		if clusterMgrEpoch > localEpoch {
			createTime := time.Unix(0, int64(cs.ID().UnixTime()))
			protectionPeriod := time.Duration(s.Conf.ChunkProtectionPeriodSec) * time.Second

			if time.Since(createTime) < protectionPeriod {
				span.Debugf("%s still in ctime protection", cs.ID())
				continue
			}

			span.Warnf("vuid(%v) already expired, local epoch:%v, new epoch:%v", vuid, localEpoch, clusterMgrEpoch)

			// todo: Remove to cm
			// Important note: in the future, the logical consideration will be transferred to cm to judge
			err := disk.ReleaseChunk(ctx, vuid, true)
			if err != nil {
				span.Errorf("release ChunkStorage(%s) form disk(%v) failed: %v", cs.ID(), disk.ID(), err)
			}
			span.Infof("vuid(%v) have been release", vuid)
		}
	}
}
