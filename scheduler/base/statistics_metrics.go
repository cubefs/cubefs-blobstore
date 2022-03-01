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

package base

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/proto"
	api "github.com/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/blobstore/util/errors"
)

const (
	defaultTaskCntReportIntervalS = 15
)

// Buckets default buckets for stats
var Buckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

// ClusterTopologyStatsMgr cluster topology stats manager
type ClusterTopologyStatsMgr struct {
	freeChunkCntRangeProHis *prometheus.HistogramVec
}

// NewClusterTopologyStatisticsMgr returns cluster topology stats manager
func NewClusterTopologyStatisticsMgr(clusterID proto.ClusterID, buckets []float64) *ClusterTopologyStatsMgr {
	labels := map[string]string{
		"cluster_id": fmt.Sprintf("%d", clusterID),
	}
	namespace := "scheduler"
	if len(buckets) == 0 {
		buckets = Buckets
	}
	freeChunkCntRangeProHis := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Name:        "free_chunk_cnt_range",
			Help:        "free chunk cnt range",
			Buckets:     buckets,
			ConstLabels: labels,
		},
		[]string{"rack", "idc"},
	)
	if err := prometheus.Register(freeChunkCntRangeProHis); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			freeChunkCntRangeProHis = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			panic(err)
		}
	}

	return &ClusterTopologyStatsMgr{
		freeChunkCntRangeProHis: freeChunkCntRangeProHis,
	}
}

// ReportFreeChunk report free chunk
func (statsMgr *ClusterTopologyStatsMgr) ReportFreeChunk(disk *api.DiskInfoSimple) {
	statsMgr.freeChunkCntRangeProHis.WithLabelValues(disk.Rack, disk.Idc).Observe(float64(disk.FreeChunkCnt))
}

// TaskCntStats information of task running on worker
type TaskCntStats interface {
	StatQueueTaskCnt() (preparing, workerDoing, finishing int)
}

// TaskRunDetailInfo task run detail info
type TaskRunDetailInfo struct {
	Statistics   proto.TaskStatistics `json:"statistics"`
	StartTime    time.Time            `json:"start_time"`
	CompleteTime time.Time            `json:"complete_time"`
	Completed    bool                 `json:"completed"`
}

// TaskStatsMgr task stats manager
type TaskStatsMgr struct {
	mu                  sync.Mutex
	TaskRunInfos        map[string]TaskRunDetailInfo
	dataSizeByteCounter counter.Counter
	shardCntCounter     counter.Counter

	dataSizeProCounter prometheus.Counter
	shardCntProCounter prometheus.Counter

	taskCntGauge *prometheus.GaugeVec

	reclaimCounter prometheus.Counter
	cancelCounter  prometheus.Counter

	taskCntStats TaskCntStats
}

// NewTaskStatsMgrAndRun run task stats manager
func NewTaskStatsMgrAndRun(clusterID proto.ClusterID, taskType string, taskCntStats TaskCntStats) *TaskStatsMgr {
	mgr := NewTaskStatsMgr(clusterID, taskType)
	mgr.taskCntStats = taskCntStats
	go mgr.ReportTaskCntLoop()
	return mgr
}

// NewTaskStatsMgr returns task stats manager
func NewTaskStatsMgr(clusterID proto.ClusterID, taskType string) *TaskStatsMgr {
	labels := map[string]string{
		"cluster_id": fmt.Sprintf("%d", clusterID),
		"task_type":  taskType,
	}
	namespace := "scheduler"

	dataSizeProCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   namespace,
		Subsystem:   "task",
		Name:        "data_size",
		Help:        "data size",
		ConstLabels: labels,
	})

	shardCntProCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   namespace,
		Subsystem:   "task",
		Name:        "shard_cnt",
		Help:        "shard cnt",
		ConstLabels: labels,
	})

	taskCntGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "",
			Name:        "task_cnt",
			Help:        "task cnt",
			ConstLabels: labels,
		}, []string{"task_status"})

	reclaimCounter := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "task",
			Name:        "reclaim",
			Help:        "task reclaim",
			ConstLabels: labels,
		})

	cancelCounter := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "task",
			Name:        "cancel",
			Help:        "task cancel",
			ConstLabels: labels,
		})

	if err := prometheus.Register(dataSizeProCounter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			dataSizeProCounter = are.ExistingCollector.(prometheus.Counter)
		} else {
			panic(err)
		}
	}
	if err := prometheus.Register(shardCntProCounter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			shardCntProCounter = are.ExistingCollector.(prometheus.Counter)
		} else {
			panic(err)
		}
	}
	if err := prometheus.Register(taskCntGauge); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			taskCntGauge = are.ExistingCollector.(*prometheus.GaugeVec)
		} else {
			panic(err)
		}
	}
	if err := prometheus.Register(reclaimCounter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			reclaimCounter = are.ExistingCollector.(prometheus.Counter)
		} else {
			panic(err)
		}
	}
	if err := prometheus.Register(cancelCounter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			cancelCounter = are.ExistingCollector.(prometheus.Counter)
		} else {
			panic(err)
		}
	}

	mgr := &TaskStatsMgr{
		TaskRunInfos:       make(map[string]TaskRunDetailInfo),
		dataSizeProCounter: dataSizeProCounter,
		shardCntProCounter: shardCntProCounter,
		taskCntGauge:       taskCntGauge,
		reclaimCounter:     reclaimCounter,
		cancelCounter:      cancelCounter,
	}

	return mgr
}

// ReportTaskCntLoop report task count
func (statsMgr *TaskStatsMgr) ReportTaskCntLoop() {
	t := time.NewTicker(time.Duration(defaultTaskCntReportIntervalS) * time.Second)
	for range t.C {
		preparing, workerDoing, finishing := statsMgr.taskCntStats.StatQueueTaskCnt()

		statsMgr.mu.Lock()
		statsMgr.taskCntGauge.WithLabelValues("preparing").Set(float64(preparing))
		statsMgr.taskCntGauge.WithLabelValues("worker_doing").Set(float64(workerDoing))
		statsMgr.taskCntGauge.WithLabelValues("finishing").Set(float64(finishing))
		statsMgr.mu.Unlock()
	}
}

// ReportWorkerTaskStats report worker task stats
func (statsMgr *TaskStatsMgr) ReportWorkerTaskStats(
	taskID string,
	s proto.TaskStatistics,
	increaseDataSize,
	increaseShardCnt int) {
	statsMgr.mu.Lock()
	defer statsMgr.mu.Unlock()

	var taskRunInfo TaskRunDetailInfo
	if _, ok := statsMgr.TaskRunInfos[taskID]; ok {
		taskRunInfo = statsMgr.TaskRunInfos[taskID]
	} else {
		taskRunInfo.StartTime = time.Now()
	}

	taskRunInfo.Statistics = s
	if taskRunInfo.Statistics.Completed() {
		taskRunInfo.CompleteTime = time.Now()
		taskRunInfo.Completed = true
	}

	statsMgr.TaskRunInfos[taskID] = taskRunInfo
	statsMgr.dataSizeByteCounter.AddN(increaseDataSize)
	statsMgr.shardCntCounter.AddN(increaseShardCnt)

	statsMgr.dataSizeProCounter.Add(float64(increaseDataSize))
	statsMgr.shardCntProCounter.Add(float64(increaseShardCnt))
}

// ReclaimTask reclaim task
func (statsMgr *TaskStatsMgr) ReclaimTask() {
	statsMgr.reclaimCounter.Inc()
}

// CancelTask cancel task
func (statsMgr *TaskStatsMgr) CancelTask() {
	statsMgr.cancelCounter.Inc()
}

// QueryTaskDetail find task detail info
func (statsMgr *TaskStatsMgr) QueryTaskDetail(taskID string) (detail TaskRunDetailInfo, err error) {
	statsMgr.mu.Lock()
	defer statsMgr.mu.Unlock()

	if info, ok := statsMgr.TaskRunInfos[taskID]; ok {
		return info, nil
	}

	return TaskRunDetailInfo{}, errors.New("not found")
}

// Counters returns task stats counters
func (statsMgr *TaskStatsMgr) Counters() (increaseDataSize, increaseShardCnt [counter.SLOT]int) {
	increaseDataSize = statsMgr.dataSizeByteCounter.Show()
	increaseShardCnt = statsMgr.shardCntCounter.Show()
	return
}
