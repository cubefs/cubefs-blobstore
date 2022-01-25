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
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/blobstore/common/trace"
)

var AllocatorVolsStatusMetric = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "blobstore",
		Subsystem: "allocator",
		Name:      "allocator_vols_status",
		Help:      "allocator volume status",
	},
	[]string{"cluster", "idc", "codemode", "type"},
)

func init() {
	prometheus.MustRegister(AllocatorVolsStatusMetric)
}

func (v *volumeMgr) metricReportTask() {
	ticker := time.NewTicker(time.Duration(v.MetricReportIntervalS) * time.Second)
	defer ticker.Stop()
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Debugf("start metric report task.")
	for {
		select {
		case <-ticker.C:
			v.metricReport(ctx)
		case <-v.closed:
			span.Debugf("loop metric report task done.")
			return
		}
	}
}

func (v *volumeMgr) metricReport(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	for codeMode, modeInfo := range v.modeInfos {
		volNums := 0
		modeInfo.volumes.Range(func(vol *volume) {
			volNums += 1
		})
		codeModeStr := strconv.FormatUint(uint64(codeMode), 10)
		AllocatorVolsStatusMetric.With(
			prometheus.Labels{
				"cluster":  strconv.FormatUint(v.ClusterID, 10),
				"idc":      v.Idc,
				"codemode": codeModeStr,
				"type":     "volume_nums",
			}).Set(float64(volNums))
		AllocatorVolsStatusMetric.With(
			prometheus.Labels{
				"cluster":  strconv.FormatUint(v.ClusterID, 10),
				"idc":      v.Idc,
				"codemode": codeModeStr,
				"type":     "total_free_size",
			}).Set(float64(modeInfo.totalFree))
		span.Debugf("metric report total_free_size,idc:%v,codemode:%v,val:%v", v.Idc, codeModeStr,
			float64(modeInfo.totalFree))
	}
}
