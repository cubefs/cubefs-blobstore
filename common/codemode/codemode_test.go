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

package codemode

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	ec6P10L2Stripes = [][]int{
		{0, 1, 2, 6, 7, 8, 9, 10, 16},
		{3, 4, 5, 11, 12, 13, 14, 15, 17},
	}
	ec16P20L2Stripes = [][]int{
		{0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 36},
		{8, 9, 10, 11, 12, 13, 14, 15, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 37},
	}
)

func TestCodeModeBase(t *testing.T) {
	codeMode1 := EC15P12.Tactic()
	assert.Equal(t, codeMode1.MinShardSize, 2048)
	indexes, n, m := (&codeMode1).GlobalStripe()
	assert.Equal(t, codeMode1.N, n)
	assert.Equal(t, codeMode1.M, m)
	assert.True(t, codeMode1.IsValid())
	expectedIndex := make([]int, 0)
	for i := 0; i < codeMode1.N+codeMode1.M; i++ {
		expectedIndex = append(expectedIndex, i)
	}
	assert.Equal(t, expectedIndex, indexes)

	codeMode2 := EC6P10L2.Tactic()
	assert.Equal(t, codeMode2.MinShardSize, 2048)
	indexes, n, m = (&codeMode2).GlobalStripe()
	assert.Equal(t, codeMode2.N, n)
	assert.Equal(t, codeMode2.M, m)
	assert.True(t, codeMode2.IsValid())
	expectedIndex = make([]int, 0)
	for i := 0; i < codeMode2.N+codeMode2.M; i++ {
		expectedIndex = append(expectedIndex, i)
	}
	assert.Equal(t, expectedIndex, indexes)
}

func TestCodeModeGetTactic(t *testing.T) {
	cases := []struct {
		mode    CodeMode
		isPanic bool
	}{
		{0, true},
		{1, false},
		{4, false},
		{(1 << 8) - 1, true},
		{math.MaxInt8, true},
	}

	for _, cs := range cases {
		if cs.isPanic {
			assert.Panics(t, func() { cs.mode.Tactic() })
		} else {
			assert.NotPanics(t, func() { cs.mode.Tactic() })
		}
	}
}

func TestGetLayoutByAZ(t *testing.T) {
	codeMode1 := EC15P12.Tactic()
	indexes := (&codeMode1).GetECLayoutByAZ()
	assert.Equal(t, 3, len(indexes))

	for i := range indexes {
		assert.Equal(t, 9, len(indexes[i]))
	}

	codeMode2 := EC6P10L2.Tactic()
	indexes = (&codeMode2).GetECLayoutByAZ()
	assert.Equal(t, 2, len(indexes))

	assert.Equal(t, ec6P10L2Stripes[0], indexes[0])
	assert.Equal(t, ec6P10L2Stripes[1], indexes[1])

	{
		codeMode := EC12P4.Tactic()
		indexes := codeMode.GetECLayoutByAZ()
		assert.Equal(t, 1, len(indexes))

		for i := range indexes {
			assert.Equal(t, codeMode.N+codeMode.M+codeMode.L, len(indexes[i]))
		}
	}
}

func TestGlobalStripe(t *testing.T) {
	cases := []struct {
		mode CodeMode
		n    int
	}{
		{EC15P12, 27},
		{EC6P6, 12},
		{EC16P20L2, 36},
		{EC6P10L2, 16},
		{EC12P4, 16},
		{EC16P4, 20},
	}
	for _, cs := range cases {
		tactic := cs.mode.Tactic()
		stripe, n, m := tactic.GlobalStripe()
		assert.Equal(t, cs.n, len(stripe))
		assert.Equal(t, tactic.N, n)
		assert.Equal(t, tactic.M, m)
	}
}

func TestAllLocalStripe(t *testing.T) {
	cases := []struct {
		mode    CodeMode
		stripes [][]int
		n       int
		m       int
	}{
		{EC6P6, nil, 0, 0},
		{EC6P10L2, ec6P10L2Stripes, 8, 1},
		{EC16P20L2, ec16P20L2Stripes, 18, 1},
	}
	for _, cs := range cases {
		tactic := cs.mode.Tactic()
		stripes, n, m := tactic.AllLocalStripe()
		assert.Equal(t, cs.stripes, stripes)
		assert.Equal(t, cs.n, n)
		assert.Equal(t, cs.m, m)
	}
}

func TestLocalStripe(t *testing.T) {
	cases := []struct {
		mode   CodeMode
		index  int
		stripe []int
		n      int
		m      int
	}{
		{EC6P6, 0, nil, 0, 0},
		{EC6P6, 1, nil, 0, 0},
		{EC6P6, 4, nil, 0, 0},
		{EC6P6, 100, nil, 0, 0},

		{EC6P10L2, 0, ec6P10L2Stripes[0], 8, 1},
		{EC6P10L2, 1, ec6P10L2Stripes[0], 8, 1},
		{EC6P10L2, 16, ec6P10L2Stripes[0], 8, 1},
		{EC6P10L2, 3, ec6P10L2Stripes[1], 8, 1},
		{EC6P10L2, 11, ec6P10L2Stripes[1], 8, 1},
		{EC6P10L2, 17, ec6P10L2Stripes[1], 8, 1},
		{EC6P10L2, 18, nil, 0, 0},

		{EC16P20L2, 0, ec16P20L2Stripes[0], 18, 1},
		{EC16P20L2, 18, ec16P20L2Stripes[0], 18, 1},
		{EC16P20L2, 36, ec16P20L2Stripes[0], 18, 1},
		{EC16P20L2, 8, ec16P20L2Stripes[1], 18, 1},
		{EC16P20L2, 35, ec16P20L2Stripes[1], 18, 1},
		{EC16P20L2, 37, ec16P20L2Stripes[1], 18, 1},
		{EC16P20L2, 38, nil, 0, 0},
	}
	for _, cs := range cases {
		tactic := cs.mode.Tactic()
		stripe, n, m := tactic.LocalStripe(cs.index)
		assert.Equal(t, cs.stripe, stripe)
		assert.Equal(t, cs.n, n)
		assert.Equal(t, cs.m, m)
	}
}

func TestLocalStripeInAZ(t *testing.T) {
	cases := []struct {
		mode    CodeMode
		azIndex int
		stripe  []int
		n       int
		m       int
	}{
		{EC6P6, 0, nil, 0, 0},
		{EC6P6, 1, nil, 0, 0},
		{EC6P6, 4, nil, 0, 0},
		{EC6P6, 100, nil, 0, 0},

		{EC6P10L2, 0, ec6P10L2Stripes[0], 8, 1},
		{EC6P10L2, 1, ec6P10L2Stripes[1], 8, 1},
		{EC6P10L2, 2, nil, 0, 0},

		{EC16P20L2, 0, ec16P20L2Stripes[0], 18, 1},
		{EC16P20L2, 1, ec16P20L2Stripes[1], 18, 1},
		{EC16P20L2, 2, nil, 0, 0},
	}
	for _, cs := range cases {
		tactic := cs.mode.Tactic()
		stripe, n, m := tactic.LocalStripeInAZ(cs.azIndex)
		assert.Equal(t, cs.stripe, stripe)
		assert.Equal(t, cs.n, n)
		assert.Equal(t, cs.m, m)
	}
}

func BenchmarkGlobalStripe(b *testing.B) {
	t := EC16P20L2.Tactic()
	tactic := &t
	for ii := 0; ii < b.N; ii++ {
		tactic.GlobalStripe()
	}
}

func BenchmarkGetECLayoutByAZ(b *testing.B) {
	t := EC16P20L2.Tactic()
	tactic := &t
	for ii := 0; ii < b.N; ii++ {
		tactic.GetECLayoutByAZ()
	}
}

func BenchmarkLocalStripe(b *testing.B) {
	t := EC16P20L2.Tactic()
	tactic := &t
	for ii := 0; ii < b.N; ii++ {
		tactic.LocalStripe(37)
	}
}
