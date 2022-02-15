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

package tinker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/tinker/client"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/log"
	"github.com/cubefs/blobstore/util/retry"
)

const (
	shardCount = 32

	defaultMarker = proto.Vid(0)
	defaultCount  = 1000
)

// ErrFrequentlyUpdate frequently update
var ErrFrequentlyUpdate = errors.New("frequently update")

type (
	volumeTime struct {
		time   time.Time
		volume client.VolInfo
	}
	shardCacher struct {
		sync.RWMutex
		m map[proto.Vid]*volumeTime
	}
	volumeCacher struct {
		interval time.Duration
		cache    [shardCount]*shardCacher
	}
)

func (vc *volumeCacher) getShard(vid proto.Vid) *shardCacher {
	return vc.cache[uint(vid)%shardCount]
}

func (vc *volumeCacher) Get(vid proto.Vid) (*client.VolInfo, bool) {
	shard := vc.getShard(vid)
	shard.RLock()
	val, exist := shard.m[vid]
	shard.RUnlock()
	if exist {
		return &val.volume, true
	}
	return nil, false
}

func (vc *volumeCacher) Set(vid proto.Vid, volume client.VolInfo) {
	shard := vc.getShard(vid)
	shard.Lock()
	shard.m[vid] = &volumeTime{
		time:   time.Now(),
		volume: volume,
	}
	shard.Unlock()
}

func (vc *volumeCacher) Settable(vid proto.Vid) bool {
	shard := vc.getShard(vid)
	shard.RLock()
	val, exist := shard.m[vid]
	shard.RUnlock()
	if !exist {
		return true
	}
	return time.Now().After(val.time.Add(vc.interval))
}

func newVolumeCacher(interval time.Duration) *volumeCacher {
	c := new(volumeCacher)
	c.interval = interval
	for idx := range c.cache {
		m := make(map[proto.Vid]*volumeTime, 32)
		c.cache[idx] = &shardCacher{m: m}
	}
	return c
}

// VolumeCache volume cache
type VolumeCache struct {
	cmClient client.ClusterMgrAPI
	group    singleflight.Group
	cache    *volumeCacher
}

// NewVolumeCache returns volume cache manager.
func NewVolumeCache(client client.ClusterMgrAPI, updateIntervalS int) *VolumeCache {
	return &VolumeCache{
		cmClient: client,
		cache:    newVolumeCacher(time.Duration(updateIntervalS) * time.Second),
	}
}

// Load list all volumes info memory cache.
func (c *VolumeCache) Load() error {
	marker := defaultMarker
	for {
		log.Infof("to load volume marker[%d], count[%d]", marker, defaultCount)

		var (
			volInfos   []client.VolInfo
			nextMarker proto.Vid
			err        error
		)
		if err = retry.Timed(3, 200).On(func() error {
			volInfos, nextMarker, err = c.cmClient.ListVolume(context.Background(), marker, defaultCount)
			return err
		}); err != nil {
			log.Errorf("list volume: marker[%d], count[%+v], code[%d], error[%v]",
				marker, defaultCount, rpc.DetectStatusCode(err), err)
			return err
		}

		for _, v := range volInfos {
			c.cache.Set(v.Vid, v)
		}
		if len(volInfos) == 0 || nextMarker == defaultMarker {
			break
		}

		marker = nextMarker
	}
	return nil
}

// Get returns this volume info.
func (c *VolumeCache) Get(vid proto.Vid) (*client.VolInfo, error) {
	if vol, ok := c.cache.Get(vid); ok {
		return vol, nil
	}

	vol, err := c.Update(vid)
	if err != nil {
		return nil, err
	}
	return vol, nil
}

// Update this volume info cache.
func (c *VolumeCache) Update(vid proto.Vid) (*client.VolInfo, error) {
	if !c.cache.Settable(vid) {
		return nil, ErrFrequentlyUpdate
	}

	val, err, _ := c.group.Do(fmt.Sprintf("volume-update-%d", vid), func() (interface{}, error) {
		vol, err := c.cmClient.GetVolInfo(context.Background(), vid)
		if err != nil {
			return nil, err
		}

		c.cache.Set(vid, vol)
		return &vol, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*client.VolInfo), nil
}
