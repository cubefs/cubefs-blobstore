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

package access

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/common/codemode"
)

func newReader(size int) io.Reader {
	buff := make([]byte, size)
	rand.Read(buff)
	return bytes.NewReader(buff)
}

func TestAccessStreamConfig(t *testing.T) {
	cfg := StreamConfig{
		IDC:                idcOther,
		MemPoolSizeClasses: map[int]int{1024: 1},
		CodeModesPutQuorums: map[codemode.CodeMode]int{
			codemode.EC15P12:  16,
			codemode.EC6P10L2: 18,
		},
	}
	confCheck(&cfg)

	require.Equal(t, idcOther, cfg.IDC)
	require.Equal(t, map[int]int{1024: 1}, cfg.MemPoolSizeClasses)
	require.Equal(t, defaultDiskPunishIntervalS, cfg.DiskPunishIntervalS)
}

func TestAccessStreamNew(t *testing.T) {
	require.Equal(t, idc, streamer.IDC)
}

func TestAccessStreamDelete(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamDelete")
	size := 1 << 18
	loc, err := streamer.Put(ctx(), newReader(size), int64(size), nil)
	require.NoError(t, err)

	err = streamer.Delete(ctx(), loc)
	require.NoError(t, err)

	dataShards.clean()
}
