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

func TestTinkerVolumeCache(t *testing.T) {
	cmClient := NewMockClusterMgrAPI(gomock.NewController(t))
	cmClient.EXPECT().ListVolume(any, any, any).Times(2).DoAndReturn(
		func(_ context.Context, marker proto.Vid, _ int) ([]client.VolInfo, proto.Vid, error) {
			if marker == defaultMarker {
				return []client.VolInfo{{Vid: 4}}, proto.Vid(10), nil
			}
			return []client.VolInfo{{Vid: 9}}, defaultMarker, nil
		},
	)
	cmClient.EXPECT().GetVolInfo(any, any).DoAndReturn(
		func(_ context.Context, vid proto.Vid) (client.VolInfo, error) {
			return client.VolInfo{Vid: vid}, nil
		},
	)

	volCache := NewVolumeCache(cmClient, 10)
	err := volCache.Load()
	require.NoError(t, err)

	// no cache will update
	_, err = volCache.Get(1)
	require.NoError(t, err)
	// return cache
	_, err = volCache.Get(1)
	require.NoError(t, err)

	// update ErrFrequentlyUpdate
	_, err = volCache.Update(1)
	require.ErrorIs(t, err, ErrFrequentlyUpdate)

	// list and get failed
	cmClient.EXPECT().ListVolume(any, any, any).AnyTimes().Return(nil, proto.Vid(0), errMock)
	cmClient.EXPECT().GetVolInfo(any, any).Return(client.VolInfo{}, errMock)
	volCache = NewVolumeCache(cmClient, -1)
	_, err = volCache.Get(1)
	require.ErrorIs(t, err, errMock)
	err = volCache.Load()
	require.ErrorIs(t, err, errMock)
}

func BenchmarkTinkerVolumeCache(b *testing.B) {
	for _, cs := range []struct {
		Name  string
		Items int
	}{
		{"tiny", 1 << 5},
		{"norm", 1 << 16},
		{"huge", 1 << 20},
	} {
		items := cs.Items
		b.Run(cs.Name, func(b *testing.B) {
			vols := make([]client.VolInfo, items)
			for idx := range vols {
				vols[idx] = client.VolInfo{Vid: proto.Vid(idx)}
			}
			cmCli := NewMockClusterMgrAPI(gomock.NewController(b))
			cmCli.EXPECT().ListVolume(any, any, any).Return(vols, defaultMarker, nil)

			cacher := NewVolumeCache(cmCli, -1)
			require.NoError(b, cacher.Load())

			b.ResetTimer()
			for ii := 0; ii < b.N; ii++ {
				cacher.Get(proto.Vid(ii % items))
			}
		})
	}
}
