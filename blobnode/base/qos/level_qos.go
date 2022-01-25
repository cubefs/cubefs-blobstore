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

package qos

import (
	"context"
	"errors"
	"io"

	"github.com/dustin/go-humanize"

	"github.com/cubefs/blobstore/blobnode/base/limitio"
	"github.com/cubefs/blobstore/blobnode/base/priority"
	"github.com/cubefs/blobstore/common/iostat"
)

var ErrWrongConfig = errors.New("qos: wrong config item")

// qos controller. controlling two dimensions:
// - iops
// - bps
type LevelQos interface {
	Writer(ctx context.Context, underlying io.Writer) io.Writer
	WriterAt(ctx context.Context, underlying io.WriterAt) io.WriterAt
	Reader(ctx context.Context, underlying io.Reader) io.Reader
	ReaderAt(ctx context.Context, underlying io.ReaderAt) io.ReaderAt
	Close() error
}

type LevelGetter interface {
	GetLevel(priority.Priority) LevelQos
}

// Manage multiple level qos controllers
// Note: controllers that are not configured, nil
type LevelManager struct {
	levels []LevelQos
}

// Implementation of LevelQos interface
// Note: iops and bps can be left unconfigured.
// levelQos Control adaptive threshold
type levelQos struct {
	bps       limitio.Controller
	iops      limitio.Controller
	diskStat  iostat.IOViewer
	threshold *Threshold
}

type rateReader struct {
	underlying io.Reader
	h          *levelQos
}

type rateReaderAt struct {
	underlying io.ReaderAt
	h          *levelQos
}

type rateWriter struct {
	underlying io.Writer
	h          *levelQos
}

type rateWriterAt struct {
	underlying io.WriterAt
	h          *levelQos
}

func (w *rateWriter) Write(p []byte) (written int, err error) {
	w.h.adjustCapacity()
	written, err = w.underlying.Write(p)
	return
}

func (wt *rateWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	wt.h.adjustCapacity()
	n, err = wt.underlying.WriteAt(p, off)
	return
}

func (r *rateReader) Read(p []byte) (n int, err error) {
	r.h.adjustCapacity()
	n, err = r.underlying.Read(p)
	return
}

func (rt *rateReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	rt.h.adjustCapacity()
	n, err = rt.underlying.ReadAt(p, off)
	return
}

func (h *levelQos) Writer(ctx context.Context, underlying io.Writer) io.Writer {
	w := underlying

	w = NewIOPSWriter(ctx, w, h.iops)
	w = NewBpsWriter(ctx, w, h.bps)

	return &rateWriter{
		underlying: w,
		h:          h,
	}
}

func (h *levelQos) WriterAt(ctx context.Context, underlying io.WriterAt) io.WriterAt {
	w := underlying

	w = NewIOPSWriterAt(ctx, w, h.iops)
	w = NewBpsWriterAt(ctx, w, h.bps)

	return &rateWriterAt{
		underlying: w,
		h:          h,
	}
}

func (h *levelQos) Reader(ctx context.Context, underlying io.Reader) io.Reader {
	r := underlying

	r = NewIOPSReader(ctx, r, h.iops)
	r = NewBpsReader(ctx, r, h.bps)

	return &rateReader{
		underlying: r,
		h:          h,
	}
}

func (h *levelQos) ReaderAt(ctx context.Context, underlying io.ReaderAt) io.ReaderAt {
	r := underlying

	r = NewIOPSReaderAt(ctx, r, h.iops)
	r = NewBpsReaderAt(ctx, r, h.bps)

	return &rateReaderAt{
		underlying: r,
		h:          h,
	}
}

func (h *levelQos) Close() error {
	if h.bps != nil {
		return h.bps.Close()
	}
	if h.iops != nil {
		return h.iops.Close()
	}
	return nil
}

func (h *levelQos) adjustCapacity() {
	if h.diskStat == nil || (h.iops == nil && h.bps == nil) {
		return
	}

	rstat := h.diskStat.ReadStat()
	wstat := h.diskStat.WriteStat()

	bps := rstat.Bps + wstat.Bps
	iops := rstat.Iops + wstat.Iops

	if h.iops != nil {
		capacity := int(h.threshold.Iops)
		if iops > uint64(h.threshold.DiskIOPS) {
			capacity = int(float64(h.threshold.Iops) * h.threshold.Factor)
		}
		h.iops.UpdateCapacity(capacity)
	}

	if h.bps != nil {
		capacity := int(h.threshold.Bandwidth)
		if bps > uint64(h.threshold.DiskBandwidth) {
			capacity = int(float64(h.threshold.Bandwidth) * h.threshold.Factor)
		}
		h.bps.UpdateCapacity(capacity)
	}
}

// get the controller of the specified priority
func (mgr *LevelManager) GetLevel(pri priority.Priority) LevelQos {
	if mgr.levels == nil {
		return nil
	}
	// if qos is not configured for the corresponding level,
	// nil is returned
	return mgr.levels[pri]
}

func NewLevelQos(threshold *Threshold, diskStat iostat.IOViewer) LevelQos {
	qos := &levelQos{
		diskStat:  diskStat,
		threshold: threshold,
	}

	if !isNotSet(threshold.Bandwidth) {
		qos.bps = limitio.NewController(int64(threshold.Bandwidth))
	}
	if !isNotSet(threshold.Iops) {
		qos.iops = limitio.NewController(int64(threshold.Iops))
	}

	return qos
}

func NewLevelQosMgr(conf Config, diskStat iostat.IOViewer) (*LevelManager, error) {
	if err := initConfig(&conf); err != nil {
		return nil, err
	}

	priLevels := priority.GetLevels()

	mgr := &LevelManager{
		levels: make([]LevelQos, len(priLevels)),
	}

	for prio, name := range priLevels {
		para, exist := conf.LevelConfigs[name]
		if !exist {
			continue
		}

		threshold := &Threshold{
			ParaConfig: ParaConfig{
				Iops:      para.Iops,
				Bandwidth: para.Bandwidth,
				Factor:    para.Factor,
			},
			DiskIOPS:      conf.DiskIOPS,
			DiskBandwidth: conf.DiskBandwidthMBPS,
		}
		if !isNotSet(threshold.DiskBandwidth) {
			threshold.DiskBandwidth = threshold.DiskBandwidth * humanize.MiByte
		}
		if !isNotSet(threshold.Bandwidth) {
			threshold.Bandwidth = threshold.Bandwidth * humanize.MiByte
		}

		levelController := NewLevelQos(threshold, diskStat)
		mgr.levels[prio] = levelController
	}

	return mgr, nil
}
