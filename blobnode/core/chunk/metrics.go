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

package chunk

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/blobstore/common/trace"
)

var (
	ChunkStatMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "chunk_stat",
			Help:      "blobnode chunk stat",
		},
		[]string{"cluster_id", "idc", "rack", "host", "disk_id", "vuid", "item"},
	)
	FileInfoMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "blobnode",
			Name:      "chunk_file_info",
			Help:      "blobnode chunk stat status",
		},
		[]string{"cluster_id", "idc", "rack", "host", "disk_id", "vuid", "item"},
	)
)

func init() {
	prometheus.MustRegister(ChunkStatMetric)
	prometheus.MustRegister(FileInfoMetric)
}

func (cs *chunk) MetricReport(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	cs.lock.RLock()
	stats := cs.stats
	chunkInfo := cs.fileInfo
	cs.lock.RUnlock()

	statsMetrics, err := base.GenMetric(stats)
	if err != nil {
		span.Errorf("id:%s, err:%v", cs.ID(), err)
		return
	}

	for item, value := range statsMetrics {
		ChunkStatMetric.With(prometheus.Labels{
			"cluster_id": cs.conf.ClusterID.ToString(),
			"idc":        cs.conf.IDC,
			"rack":       cs.conf.Rack,
			"host":       cs.conf.Host,
			"disk_id":    cs.diskID.ToString(),
			"vuid":       cs.vuid.ToString(),
			"item":       item,
		}).Set(value)
	}

	chunkInfoMetrics, err := base.GenMetric(chunkInfo)
	if err != nil {
		span.Errorf("id:%s, err:%v", cs.ID(), err)
		return
	}

	for item, value := range chunkInfoMetrics {
		FileInfoMetric.With(prometheus.Labels{
			"cluster_id": cs.conf.ClusterID.ToString(),
			"idc":        cs.conf.IDC,
			"rack":       cs.conf.Rack,
			"host":       cs.conf.Host,
			"disk_id":    cs.diskID.ToString(),
			"vuid":       cs.vuid.ToString(),
			"item":       item,
		}).Set(value)
	}
}
