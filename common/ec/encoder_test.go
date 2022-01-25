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

package ec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/blobstore/common/codemode"
)

var srcData = "Hello world"

func TestEncoder(t *testing.T) {
	// initial
	cfg := &Config{
		CodeMode:     codemode.EC15P12.Tactic(),
		EnableVerify: true,
		Concurrency:  10,
	}
	encoder, err := NewEncoder(cfg)
	assert.NoError(t, err)

	// source data split
	shards, err := encoder.Split([]byte(srcData))
	assert.NoError(t, err)

	// encode data
	err = encoder.Encode(shards)
	assert.NoError(t, err)
	wbuff := bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	assert.NoError(t, err)
	assert.Equal(t, []byte(srcData), wbuff.Bytes())

	dataShards := encoder.GetDataShards(shards)

	// set one data shards broken
	for i := range dataShards[0] {
		dataShards[0][i] = 222
	}
	// reconstruct data and check
	err = encoder.ReconstructData(shards, []int{0})
	assert.NoError(t, err)
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	assert.NoError(t, err)
	assert.Equal(t, []byte(srcData), wbuff.Bytes())

	// reconstruct shard and check
	parityShards := encoder.GetParityShards(shards)
	// set one data shard broken
	for i := range parityShards[1] {
		parityShards[1][i] = 11
	}
	err = encoder.Reconstruct(shards, []int{cfg.CodeMode.N + 1})
	assert.NoError(t, err)
	ok, err := encoder.Verify(shards)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)
	// check data
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	assert.NoError(t, err)
	assert.Equal(t, []byte(srcData), wbuff.Bytes())

	ls := encoder.GetLocalShards(shards)
	assert.Equal(t, 0, len(ls))
	si := encoder.GetShardsInIdc(shards, 0)
	assert.Equal(t, (cfg.CodeMode.N+cfg.CodeMode.M)/3, len(si))
}

func TestLrcEncoder(t *testing.T) {
	// initial
	cfg := &Config{
		CodeMode:     codemode.EC6P10L2.Tactic(),
		EnableVerify: true,
		Concurrency:  10,
	}
	encoder, err := NewEncoder(cfg)
	assert.NoError(t, err)

	// source data split
	shards, err := encoder.Split([]byte(srcData))
	assert.NoError(t, err)

	// encode data
	err = encoder.Encode(shards)
	assert.NoError(t, err)
	wbuff := bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	assert.NoError(t, err)
	assert.Equal(t, []byte(srcData), wbuff.Bytes())

	dataShards := encoder.GetDataShards(shards)

	// set one data shard broken
	for i := range dataShards[0] {
		dataShards[0][i] = 222
	}
	// reconstruct data and check
	err = encoder.ReconstructData(shards, []int{0})
	assert.NoError(t, err)
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	assert.NoError(t, err)
	assert.Equal(t, []byte(srcData), wbuff.Bytes())

	// Local reconstruct shard and check
	localShardsInIdc := encoder.GetShardsInIdc(shards, 0)
	// set wrong data
	for i := range localShardsInIdc[0] {
		localShardsInIdc[0][i] = 11
	}
	// check must be false when a shard broken
	ok, err := encoder.Verify(shards)
	assert.NoError(t, err)
	assert.Equal(t, false, ok)

	err = encoder.Reconstruct(localShardsInIdc, []int{0})
	assert.NoError(t, err)
	ok, err = encoder.Verify(shards)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)

	// global reconstruct shard and check
	dataShards = encoder.GetDataShards(shards)
	parityShards := encoder.GetParityShards(shards)
	badIdxs := make([]int, 0)
	for i := 0; i < cfg.CodeMode.M; i++ {
		if i%2 == 0 {
			badIdxs = append(badIdxs, i)
			// set wrong data
			if i < len(dataShards) {
				for j := range dataShards[i] {
					dataShards[i][j] = 222
				}
			}
		} else {
			badIdxs = append(badIdxs, cfg.CodeMode.N+i)
			// set wrong data
			for j := range parityShards[i] {
				parityShards[i][j] = 222
			}
		}
	}
	// add a local broken
	for j := range shards[cfg.CodeMode.N+cfg.CodeMode.M+1] {
		shards[cfg.CodeMode.N+cfg.CodeMode.M+1][j] = 222
	}
	badIdxs = append(badIdxs, cfg.CodeMode.N+cfg.CodeMode.M+1)
	err = encoder.Reconstruct(shards, badIdxs)
	assert.NoError(t, err)
	ok, err = encoder.Verify(shards)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)
	// check data
	wbuff = bytes.NewBuffer(make([]byte, 0))
	err = encoder.Join(wbuff, shards, len(srcData))
	assert.NoError(t, err)
	assert.Equal(t, []byte(srcData), wbuff.Bytes())

	ls := encoder.GetLocalShards(shards)
	assert.Equal(t, cfg.CodeMode.L, len(ls))
	si := encoder.GetShardsInIdc(shards, 0)
	assert.Equal(t, (cfg.CodeMode.N+cfg.CodeMode.M+cfg.CodeMode.L)/cfg.CodeMode.AZCount, len(si))
}
