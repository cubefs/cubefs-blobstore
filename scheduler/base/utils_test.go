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

package base

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/common/counter"
	"github.com/cubefs/blobstore/common/errors"
	comproto "github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
)

func TestSubtraction(t *testing.T) {
	fromCm := []comproto.Vuid{1, 2, 3, 4, 5}
	fromDb := []comproto.Vuid{1, 2, 3}
	remain := Subtraction(fromCm, fromDb)
	require.Equal(t, []comproto.Vuid{4, 5}, remain)
}

func TestHumanRead(t *testing.T) {
	fmt.Println(bytesCntFormat(0))
	fmt.Println(bytesCntFormat(1024*1024*1024*1024*1024*1024 + 1))
}

func TestDataMountBytePrintEx(t *testing.T) {
	dataMountBytes := [counter.SLOT]int{
		1024*1024*1024*1024*1024*1024 + 1,
		2445979449, 2363491431, 2122318836,
		4341106710, 3521119887, 3681901617,
		3697979790, 4244637672, 3424650849,
		3279947292, 2675967228, 3624579435,
		4855608246, 5161093533, 5064624495,
		4855608246, 4984233630, 512, 0,
	}
	r := DataMountFormat(dataMountBytes)
	fmt.Print(r)
}

func TestLoopExecUntilSuccess(t *testing.T) {
	LoopExecUntilSuccess(context.Background(), "test", func() error {
		return nil
	})
}

func TestShouldAllocAndRedo(t *testing.T) {
	err := errors.ErrNewVuidNotMatch
	code := rpc.DetectStatusCode(err)
	redo := ShouldAllocAndRedo(code)
	require.Equal(t, true, redo)
}
