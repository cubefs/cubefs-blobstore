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
)

func TestVolCache(t *testing.T) {
	ctr := gomock.NewController(t)
	cmClient := NewMockClusterMgrAPI(ctr)
	cmClient.EXPECT().ListVolume(gomock.Any(), gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
		func(_ context.Context, marker proto.Vid, _ int) ([]client.VolInfo, proto.Vid, error) {
			if marker == defaultMarker {
				return []client.VolInfo{{Vid: 4}}, proto.Vid(10), nil
			}
			return []client.VolInfo{{Vid: 9}}, defaultMarker, nil
		})
	cmClient.EXPECT().GetVolInfo(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, vid proto.Vid) (client.VolInfo, error) {
			return client.VolInfo{Vid: vid}, nil
		})

	volCache := NewVolCache(cmClient, -1)
	err := volCache.Load()
	require.NoError(t, err)

	// no cache will update
	_, err = volCache.Get(1)
	require.NoError(t, err)

	// return catch
	_, err = volCache.Get(1)
	require.NoError(t, err)

	// // ErrUpdateNotLongAgo
	// _, err = volCache.Get(1)
	// require.ErrorIs(t, ErrUpdateNotLongAgo, err)

	// update failed
	cmClient.EXPECT().GetVolInfo(gomock.Any(), gomock.Any()).Return(
		client.VolInfo{}, errMock)
	volCache = NewVolCache(cmClient, -1)
	_, err = volCache.Get(1)
	require.ErrorIs(t, errMock, err)

	// list volume failed
	cmClient.EXPECT().ListVolume(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(
		nil, proto.Vid(0), errMock)
	volCache = NewVolCache(cmClient, 60)
	err = volCache.Load()
	require.ErrorIs(t, errMock, err)
}
