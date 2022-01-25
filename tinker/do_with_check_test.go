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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/tinker/client"
)

func TestDoWithCheckVolConsistencyErr(t *testing.T) {
	ctr := gomock.NewController(t)
	volCache := NewMockVolumeCache(ctr)

	{
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{Vid: 1, VunitLocations: []proto.VunitLocation{
					{Vuid: 1},
				}}, nil
			},
		)

		err := DoWithCheckVolConsistency(context.Background(), volCache, 1, func(volInfo *client.VolInfo) error {
			return errMock
		})
		require.Equal(t, err, errMock)
	}
	{
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return nil, errors.ErrVolumeNotExist
			},
		)
		err := DoWithCheckVolConsistency(context.Background(), volCache, 12, func(volInfo *client.VolInfo) error {
			return errMock
		})
		require.Equal(t, err, errors.ErrVolumeNotExist)
	}
	{
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{Vid: 1, VunitLocations: []proto.VunitLocation{
					{Vuid: 1},
				}}, nil
			},
		)
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return nil, errors.ErrVolumeNotExist
			},
		)
		err := DoWithCheckVolConsistency(context.Background(), volCache, 1, func(volInfo *client.VolInfo) error {
			return nil
		})
		require.Equal(t, err, errors.ErrVolumeNotExist)
	}
	{
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{Vid: 1, VunitLocations: []proto.VunitLocation{
					{Vuid: 1},
				}}, nil
			},
		)
		volCache.EXPECT().Get(gomock.Any()).Times(2).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{Vid: 1, VunitLocations: []proto.VunitLocation{
					{Vuid: 2},
				}}, nil
			},
		)
		err := DoWithCheckVolConsistency(context.Background(), volCache, 1, func(volInfo *client.VolInfo) error {
			return nil
		})
		require.NoError(t, err)
	}
	{
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{Vid: proto.Vid(1), VunitLocations: []proto.VunitLocation{
					{Vuid: proto.Vuid(0)},
				}}, nil
			},
		)
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{Vid: proto.Vid(2), VunitLocations: []proto.VunitLocation{
					{Vuid: proto.Vuid(0)},
				}}, nil
			},
		)
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{Vid: proto.Vid(3), VunitLocations: []proto.VunitLocation{
					{Vuid: proto.Vuid(0)},
				}}, nil
			},
		)
		volCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(vid proto.Vid) (*client.VolInfo, error) {
				return &client.VolInfo{Vid: proto.Vid(4), VunitLocations: []proto.VunitLocation{
					{Vuid: proto.Vuid(0)},
				}}, nil
			},
		)
		err := DoWithCheckVolConsistency(context.Background(), volCache, 1, func(volInfo *client.VolInfo) error {
			return nil
		})
		require.Equal(t, err, errVolInfoNotEqual)
	}
}
