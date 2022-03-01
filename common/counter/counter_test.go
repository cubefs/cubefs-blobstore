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

package counter

import (
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
)

func TestCounterByMin(t *testing.T) {
	mtime := clock.NewMock()
	mtime.Add(1483950782 * 1e9)
	timeClock = mtime
	c := new(CounterByMin)

	expected := [SLOT]int{}
	count := c.Show()
	assert.Equal(t, expected, count)
	c.Add()
	expected[SLOT-1] = 1
	count = c.Show()
	assert.Equal(t, expected, count)

	for i := 0; i < 3; i++ {
		mtime.Add(60e9)
		expected = [SLOT]int{}
		expected[SLOT-2] = 1
		expected[SLOT-1] = 0
		count = c.Show()
		assert.Equal(t, expected, count)
		c.Add()
		c.Add()
		expected[SLOT-2] = 1
		expected[SLOT-1] = 2
		count = c.Show()
		assert.Equal(t, expected, count)

		mtime.Add((SLOT - 1) * 60e9)
		expected = [SLOT]int{}
		expected[0] = 2
		count = c.Show()
		assert.Equal(t, expected, count)
		c.Add()
		expected[0] = 2
		expected[SLOT-1] = 1
		count = c.Show()
		assert.Equal(t, expected, count)
	}
}
