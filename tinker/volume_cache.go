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

	cmap "github.com/orcaman/concurrent-map"
	"golang.org/x/sync/singleflight"

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/tinker/client"
	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/log"
	"github.com/cubefs/blobstore/util/retry"
)

const (
	defaultMarker = proto.Vid(0)
	defaultCount  = 1000
)

// ErrUpdateNotLongAgo update not long ago
var ErrUpdateNotLongAgo = errors.New("update not long ago")

type updateStatus struct {
	updated         map[proto.Vid]time.Time
	l               sync.Mutex
	updateDurationS time.Duration
}

func newUpdateStatus(updateDurationS int) *updateStatus {
	return &updateStatus{
		updated:         make(map[proto.Vid]time.Time),
		updateDurationS: time.Duration(updateDurationS) * time.Second,
	}
}

func (s *updateStatus) update(vid proto.Vid) {
	s.l.Lock()
	defer s.l.Unlock()
	s.updated[vid] = time.Now()
}

func (s *updateStatus) canUpdate(vid proto.Vid) bool {
	s.l.Lock()
	defer s.l.Unlock()

	lastUpdate, ok := s.updated[vid]
	if !ok {
		return true
	}

	return time.Now().After(lastUpdate.Add(s.updateDurationS))
}

// VolCache volume cache
type VolCache struct {
	cmClient     client.ClusterMgrAPI
	m            cmap.ConcurrentMap
	group        singleflight.Group
	updateStatus *updateStatus
}

// NewVolCache returns volume cache manager
func NewVolCache(client client.ClusterMgrAPI, updateDurationS int) *VolCache {
	return &VolCache{
		m:            cmap.New(),
		cmClient:     client,
		updateStatus: newUpdateStatus(updateDurationS),
	}
}

// Load list all volume info
func (c *VolCache) Load() error {
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
			c.m.Set(v.Vid.ToString(), v)
		}
		if len(volInfos) == 0 || nextMarker == defaultMarker {
			break
		}

		marker = nextMarker
	}
	return nil
}

// Get returns volume info with volumeID
func (c *VolCache) Get(vid proto.Vid) (*client.VolInfo, error) {
	if v, ok := c.m.Get(vid.ToString()); ok {
		volInfo := v.(client.VolInfo)
		return &volInfo, nil
	}

	volInfo, err := c.Update(vid)
	if err != nil {
		return nil, err
	}
	return volInfo, nil
}

// Update update volume cache with volumeID
func (c *VolCache) Update(vid proto.Vid) (*client.VolInfo, error) {
	if !c.updateStatus.canUpdate(vid) {
		return nil, ErrUpdateNotLongAgo
	}

	ret, err, _ := c.group.Do(fmt.Sprintf("volume-update-%d", vid), func() (interface{}, error) {
		volInfo, err := c.cmClient.GetVolInfo(context.Background(), vid)
		if err != nil {
			return nil, err
		}
		c.m.Set(vid.ToString(), volInfo)
		return &volInfo, nil
	})

	if err != nil {
		return nil, err
	}
	c.updateStatus.update(vid)
	return ret.(*client.VolInfo), nil
}
