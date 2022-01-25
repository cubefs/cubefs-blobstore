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

package counter

import (
	"sync"

	"github.com/facebookgo/clock"
)

const (
	Interval = 60
	SLOT     = 20
)

var timeClock = clock.New()

type minCounter struct {
	count       int
	interverIdx int64
}

type CounterByMin struct {
	mu     sync.Mutex
	counts [SLOT]minCounter
}

func (self *CounterByMin) Add() {
	self.AddEx(1)
}

func (self *CounterByMin) AddEx(n int) {
	nowInterval := timeClock.Now().Unix() / Interval
	idx := nowInterval % SLOT
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.counts[idx].interverIdx == nowInterval {
		self.counts[idx].count += n
	} else {
		self.counts[idx].count = n
		self.counts[idx].interverIdx = nowInterval
	}
}

func (self *CounterByMin) Show() (count [SLOT]int) {
	nowInterval := timeClock.Now().Unix() / Interval
	idx := nowInterval % SLOT
	self.mu.Lock()
	defer self.mu.Unlock()
	for i := int64(0); i < SLOT; i++ {
		index := idx + 1 + i
		if index >= SLOT {
			index -= SLOT
		}
		ts := nowInterval - SLOT + 1 + i
		count[i] = 0
		if ts == self.counts[index].interverIdx {
			count[i] = self.counts[index].count
		}
	}
	return
}
