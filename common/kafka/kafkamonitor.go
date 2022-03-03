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

package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/blobstore/util/log"
)

var mockTestKafkaClient sarama.Client

func newKafkaOffsetGauge() *prometheus.GaugeVec {
	gaugeOpts := prometheus.GaugeOpts{
		Namespace: "kafka",
		Subsystem: "topic_partition",
		Name:      "offset",
		Help:      "monitor kafka newest oldest and consume offset",
	}
	labelNames := []string{"module_name", "topic", "partition", "type"}
	kafkaOffsetGaugeVec := prometheus.NewGaugeVec(gaugeOpts, labelNames)

	err := prometheus.Register(kafkaOffsetGaugeVec)
	if err == nil {
		return kafkaOffsetGaugeVec
	}
	if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
		return are.ExistingCollector.(*prometheus.GaugeVec)
	}
	panic(err)
}

func newKafkaLatencyGauge() *prometheus.GaugeVec {
	gaugeOpts := prometheus.GaugeOpts{
		Namespace: "kafka",
		Subsystem: "topic_partition",
		Name:      "consume_lag",
		Help:      "monitor kafka latency",
	}
	labelNames := []string{"module_name", "topic", "partition"}
	kafkaLatencyGaugeVec := prometheus.NewGaugeVec(gaugeOpts, labelNames)

	err := prometheus.Register(kafkaLatencyGaugeVec)
	if err == nil {
		return kafkaLatencyGaugeVec
	}
	if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
		return are.ExistingCollector.(*prometheus.GaugeVec)
	}
	panic(err)
}

type offsetMap struct {
	offsetMap  map[int32]int64
	OffsetLock sync.RWMutex
}

func newOffsetMap() *offsetMap {
	m := make(map[int32]int64)
	retOffsetMap := offsetMap{offsetMap: m}
	return &retOffsetMap
}

func (self *offsetMap) getOffset(pid int32) int64 {
	self.OffsetLock.RLock()
	defer self.OffsetLock.RUnlock()
	if _, ok := self.offsetMap[pid]; ok {
		return self.offsetMap[pid]
	}
	return 0
}

func (self *offsetMap) setOffset(offset int64, pid int32) {
	self.OffsetLock.Lock()
	defer self.OffsetLock.Unlock()
	self.offsetMap[pid] = offset
}

type KafkaMonitor struct {
	kafkaClient                 sarama.Client
	topic                       string
	pids                        []int32
	newestOffsetMap             *offsetMap
	oldestOffsetMap             *offsetMap
	consumeOffsetMap            *offsetMap
	kafkaOffAcquireIntervalSecs int64
	offsetGauge                 *prometheus.GaugeVec
	latencyGauge                *prometheus.GaugeVec
	moduleName                  string
}

const DefauleintervalSecs = 60

func NewKafkaMonitor(
	moduleName string,
	brokerHosts []string,
	topic string,
	pids []int32,
	intervalSecs int64) (*KafkaMonitor, error,
) {
	monitor := KafkaMonitor{
		topic:                       topic,
		pids:                        pids,
		newestOffsetMap:             newOffsetMap(),
		oldestOffsetMap:             newOffsetMap(),
		consumeOffsetMap:            newOffsetMap(),
		kafkaOffAcquireIntervalSecs: DefauleintervalSecs,
		moduleName:                  moduleName,
	}

	if intervalSecs == 0 {
		intervalSecs = DefauleintervalSecs
	}
	monitor.kafkaOffAcquireIntervalSecs = intervalSecs

	if mockTestKafkaClient != nil {
		monitor.kafkaClient = mockTestKafkaClient
	} else {
		client, err := sarama.NewClient(brokerHosts, nil)
		if err != nil {
			return nil, err
		}
		monitor.kafkaClient = client
	}

	monitor.offsetGauge = newKafkaOffsetGauge()
	monitor.latencyGauge = newKafkaLatencyGauge()

	go monitor.loopAcquireKafkaOffset()

	return &monitor, nil
}

func (monitor *KafkaMonitor) loopAcquireKafkaOffset() {
	log.Info("KafkaMonitor start loopAcquireKafkaOffset")
	for {
		for _, pid := range monitor.pids {
			newestOffset, err := monitor.kafkaClient.GetOffset(monitor.topic, pid, sarama.OffsetNewest)
			if err != nil {
				log.Error(fmt.Sprintf("get newest offset fail topic %v pid %v ", monitor.topic, pid))
				continue
			}
			log.Debug("loopAcquireKafkaOffset newestOffset:", newestOffset)
			monitor.newestOffsetMap.setOffset(newestOffset, pid)

			oldestOffset, err := monitor.kafkaClient.GetOffset(monitor.topic, pid, sarama.OffsetOldest)
			if err != nil {
				log.Error(fmt.Sprintf("get oldest offset fail topic %v pid %v ", monitor.topic, pid))
				continue
			}
			log.Debug("loopAcquireKafkaOffset oldestOffset:", oldestOffset)
			monitor.oldestOffsetMap.setOffset(oldestOffset, pid)
		}

		monitor.report()
		time.Sleep(time.Duration(monitor.kafkaOffAcquireIntervalSecs) * time.Second)
	}
}

func (monitor *KafkaMonitor) report() {
	for _, pid := range monitor.pids {
		oldestOffset := monitor.oldestOffsetMap.getOffset(pid)
		newestOffset := monitor.newestOffsetMap.getOffset(pid)
		consumeOffset := monitor.consumeOffsetMap.getOffset(pid)
		latency := newestOffset - consumeOffset - 1 //-1，because the newestOffset is the next message offset
		if latency < 0 {
			latency = 0
		}

		monitor.reportOffsetMetric(pid, string("oldest"), float64(oldestOffset))
		monitor.reportOffsetMetric(pid, string("newest"), float64(newestOffset))
		monitor.reportOffsetMetric(pid, string("consume"), float64(consumeOffset))
		monitor.reportLatencyMetric(pid, float64(latency))
		log.Debug("Report...")
	}
}

func (monitor *KafkaMonitor) reportOffsetMetric(pid int32, metricType string, val float64) {
	labels := prometheus.Labels{
		"module_name": monitor.moduleName,
		"topic":       monitor.topic,
		"partition":   fmt.Sprintf("%d", pid),
		"type":        metricType,
	}
	monitor.offsetGauge.With(labels).Set(val)
}

func (monitor *KafkaMonitor) reportLatencyMetric(pid int32, val float64) {
	labels := prometheus.Labels{
		"module_name": monitor.moduleName,
		"topic":       monitor.topic,
		"partition":   fmt.Sprintf("%d", pid),
	}
	monitor.latencyGauge.With(labels).Set(val)
}

func (monitor *KafkaMonitor) SetConsumeOffset(consumerOff int64, pid int32) {
	monitor.consumeOffsetMap.setOffset(consumerOff, pid)
}
