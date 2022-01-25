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
	"sync"
)

// ErrGroup waits for a collection of goroutines to finish and get response error
type ErrGroup struct {
	wg   sync.WaitGroup
	errs []error
}

// NewNErrsGroup returns ErrGroup
func NewNErrsGroup(size int) *ErrGroup {
	return &ErrGroup{errs: make([]error, size)}
}

// Go adds handle function
func (g *ErrGroup) Go(f func() error, index int) {
	g.wg.Add(1)

	go func() {
		defer g.wg.Done()

		if err := f(); err != nil {
			g.errs[index] = err
		}
	}()
}

// Wait wait for all goroutines done
func (g *ErrGroup) Wait() []error {
	g.wg.Wait()
	return g.errs
}
