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

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/tinker/client"
	"github.com/cubefs/blobstore/util/errors"
)

func TestVolCache(t *testing.T) {
	ctr := gomock.NewController(t)
	cmClient := NewMockClusterMgrAPI(ctr)
	cmClient.EXPECT().ListVolume(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, afterVid proto.Vid, count int) ([]*client.VolInfo, proto.Vid, error) {
			return []*client.VolInfo{{Vid: 3}}, 1, nil
		},
	)
	cmClient.EXPECT().ListVolume(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, afterVid proto.Vid, count int) ([]*client.VolInfo, proto.Vid, error) {
			return []*client.VolInfo{{Vid: 4}}, 0, nil
		},
	)
	cmClient.EXPECT().GetVolInfo(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, vid proto.Vid) (*client.VolInfo, error) {
			return &client.VolInfo{
				Vid: proto.Vid(1),
			}, nil
		},
	)

	volCache := NewVolCache(cmClient, -1)
	err := volCache.Load(0, 0)
	require.NoError(t, err)

	// no cache will update
	_, err = volCache.Get(1)
	require.NoError(t, err)

	// return catch
	_, err = volCache.Get(1)
	require.NoError(t, err)

	////ErrUpdateNotLongAgo
	//_, err = volCache.Get(1)
	//require.Equal(t, true, errors.Is(err, ErrUpdateNotLongAgo))

	// update failed
	cmClient.EXPECT().GetVolInfo(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, vid proto.Vid) (*client.VolInfo, error) {
			return nil, errMock
		},
	)
	volCache = NewVolCache(cmClient, -1)
	_, err = volCache.Get(1)
	require.Equal(t, true, errors.Is(err, errMock))

	// list volume failed
	cmClient.EXPECT().ListVolume(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, afterVid proto.Vid, count int) ([]*client.VolInfo, proto.Vid, error) {
			return nil, 0, errMock
		},
	)
	volCache = NewVolCache(cmClient, 60)
	err = volCache.Load(0, 0)
	require.Equal(t, true, errors.Is(err, errMock))
}
