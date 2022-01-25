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

package mergetask

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMergeTask(t *testing.T) {
	var n int32
	testFn1 := func(i interface{}) error {
		fmt.Printf("i: %v\n", i)
		atomic.AddInt32(&n, 1)
		time.Sleep(30 * time.Millisecond)
		return nil
	}
	runner := NewMergeTask(1000000, testFn1)
	assert.NotNil(t, runner)

	wg := sync.WaitGroup{}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func(i int) {
			defer wg.Done()
			e := runner.Do(i)
			assert.NoError(t, e)
		}(i)
	}
	wg.Wait()

	fmt.Printf("n:%v\n", n)
	assert.True(t, n < 1000)
}
