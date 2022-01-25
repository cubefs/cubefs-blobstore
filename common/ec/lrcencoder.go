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
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"

	"github.com/cubefs/blobstore/util/errors"
	"github.com/cubefs/blobstore/util/taskpool"
)

type lrcEncoder struct {
	engine      reedsolomon.Encoder
	localEngine reedsolomon.Encoder
	taskPool    taskpool.TaskPool
	*Config
}

func (e *lrcEncoder) Encode(shards [][]byte) error {
	if len(shards) != (e.CodeMode.N + e.CodeMode.M + e.CodeMode.L) {
		return ErrInvalidShards
	}

	var (
		err  error
		errs = make([]error, e.CodeMode.AZCount)
	)

	// firstly, do global ec encode
	wg := sync.WaitGroup{}
	wg.Add(1)
	e.taskPool.Run(func() {
		defer wg.Done()
		err = e.engine.Encode(shards[:e.CodeMode.N+e.CodeMode.M])
		if err != nil {
			return
		}
		if e.EnableVerify {
			ok, verifyErr := e.engine.Verify(shards[:e.CodeMode.N+e.CodeMode.M])
			if verifyErr != nil {
				err = verifyErr
				return
			}
			if !ok {
				err = ErrVerify
			}
		}
	})
	wg.Wait()
	if err != nil {
		return errors.Info(err, "lrcEncoder.Encode failed").Detail(err)
	}

	// secondly, do local ec encode
	for i := 0; i < e.CodeMode.AZCount; i++ {
		idx := i
		localShards := e.GetShardsInIdc(shards, idx)
		wg.Add(1)
		e.taskPool.Run(func() {
			defer wg.Done()
			errs[idx] = e.localEngine.Encode(localShards)
			if errs[idx] != nil {
				return
			}

			if e.EnableVerify {
				ok, err := e.localEngine.Verify(localShards)
				if err != nil {
					errs[idx] = err
					return
				}
				if !ok {
					errs[idx] = ErrVerify
				}
			}
		})
	}
	wg.Wait()

	for i := range errs {
		if errs[i] != nil {
			return errors.Info(errs[i], "lrcEncoder.Encode local engine encode failed").Detail(err)
		}
	}

	return nil
}

func (e *lrcEncoder) Verify(shards [][]byte) (bool, error) {
	type verifyErr struct {
		ok  bool
		err error
	}

	var (
		wg   = sync.WaitGroup{}
		ok   bool
		err  error
		errs = make([]*verifyErr, e.CodeMode.AZCount)
	)

	wg.Add(1)
	e.taskPool.Run(func() {
		defer wg.Done()
		ok, err = e.engine.Verify(shards[:e.CodeMode.N+e.CodeMode.M])
	})
	wg.Wait()
	if !ok || err != nil {
		if err != nil {
			err = errors.Info(err, "lrcEncoder.Verify global shards failed").Detail(err)
		}
		return ok, err
	}

	for i := 0; i < e.CodeMode.AZCount; i++ {
		idx := i
		localShards := e.GetShardsInIdc(shards, idx)
		wg.Add(1)
		e.taskPool.Run(func() {
			defer wg.Done()
			ok, err := e.localEngine.Verify(localShards)
			errs[idx] = &verifyErr{ok: ok, err: err}
		})
	}
	wg.Wait()

	for i := range errs {
		if !errs[i].ok || errs[i].err != nil {
			if errs[i].err != nil {
				errs[i].err = errors.Info(errs[i].err, "lrcEncoder.Verify local shards failed").Detail(errs[i].err)
			}
			return errs[i].ok, errs[i].err
		}
	}

	return true, nil
}

func (e *lrcEncoder) Reconstruct(shards [][]byte, badIdx []int) error {
	globalBadIdx := make([]int, 0)
	for _, i := range badIdx {
		if i < e.CodeMode.N+e.CodeMode.M {
			globalBadIdx = append(globalBadIdx, i)
		}
	}
	initBadShards(shards, globalBadIdx)
	var (
		retErr error
		wg     = sync.WaitGroup{}
	)

	// use local ec reconstruct, saving network bandwidth
	if len(shards) == (e.CodeMode.N+e.CodeMode.M+e.CodeMode.L)/e.CodeMode.AZCount {
		wg.Add(1)
		e.taskPool.Run(func() {
			defer wg.Done()
			retErr = e.localEngine.Reconstruct(shards)
		})
		wg.Wait()
		if retErr != nil {
			return errors.Info(retErr, "lrcEncoder.Reconstruct local ec reconstruct failed")
		}
		return nil
	}

	// can't reconstruct from local ec
	// firstly, use global ec reconstruct
	wg.Add(1)
	e.taskPool.Run(func() {
		defer wg.Done()
		retErr = e.engine.Reconstruct(shards[:e.CodeMode.N+e.CodeMode.M])
	})
	wg.Wait()
	if retErr != nil {
		return errors.Info(retErr, "lrcEncoder.Reconstruct global ec reconstruct failed")
	}

	// secondly, check if need to reconstruct the local shards
	errs := make([]error, e.CodeMode.AZCount)
	localRestructs := make(map[int][]int)
	for _, i := range badIdx {
		if i >= (e.CodeMode.N + e.CodeMode.M) {
			idcIdx := (i - e.CodeMode.N - e.CodeMode.M) * e.CodeMode.AZCount / e.CodeMode.L
			badIdx := idcIdx + (e.CodeMode.N+e.CodeMode.M)/e.CodeMode.AZCount - 1
			if _, ok := localRestructs[idcIdx]; !ok {
				localRestructs[idcIdx] = make([]int, 0)
			}
			localRestructs[idcIdx] = append(localRestructs[idcIdx], badIdx)
		}
	}
	for idx, badIdx := range localRestructs {
		wg.Add(1)
		localShards := e.GetShardsInIdc(shards, idx)
		initBadShards(localShards, badIdx)
		e.taskPool.Run(func() {
			defer wg.Done()
			errs[idx] = e.localEngine.Reconstruct(localShards)
		})
	}

	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			return errors.Info(errs[i], "lrcEncoder.Reconstruct local ec reconstruct after global ec failed")
		}
	}
	return nil
}

func (e *lrcEncoder) ReconstructData(shards [][]byte, badIdx []int) error {
	initBadShards(shards, badIdx)
	shards = shards[:e.CodeMode.N+e.CodeMode.M]

	var (
		retErr error
		wg     = sync.WaitGroup{}
	)
	wg.Add(1)
	e.taskPool.Run(func() {
		defer wg.Done()
		retErr = e.engine.ReconstructData(shards)
	})
	wg.Wait()
	return retErr
}

func (e *lrcEncoder) Split(data []byte) ([][]byte, error) {
	shards, err := e.engine.Split(data)
	if err != nil {
		return nil, err
	}
	shardN, shardLen := len(shards), len(shards[0])
	if cap(data) >= (e.CodeMode.L+shardN)*shardLen {
		if cap(data) > len(data) {
			data = data[:cap(data)]
		}
		for i := 0; i < e.CodeMode.L; i++ {
			shards = append(shards, data[(shardN+i)*shardLen:(shardN+i+1)*shardLen])
		}
	} else {
		for i := 0; i < e.CodeMode.L; i++ {
			shards = append(shards, make([]byte, shardLen))
		}
	}
	return shards, nil
}

func (e *lrcEncoder) GetDataShards(shards [][]byte) [][]byte {
	return shards[:e.CodeMode.N]
}

func (e *lrcEncoder) GetParityShards(shards [][]byte) [][]byte {
	return shards[e.CodeMode.N : e.CodeMode.N+e.CodeMode.M]
}

func (e *lrcEncoder) GetLocalShards(shards [][]byte) [][]byte {
	return shards[e.CodeMode.N+e.CodeMode.M:]
}

func (e *lrcEncoder) GetShardsInIdc(shards [][]byte, idx int) [][]byte {
	n, m, l := e.CodeMode.N, e.CodeMode.M, e.CodeMode.L
	idcCnt := e.CodeMode.AZCount
	localN, localM, localL := n/idcCnt, m/idcCnt, l/idcCnt

	localShardsInIdc := make([][]byte, localN+localM+localL)

	shardsN := shards[idx*localN : (idx+1)*localN]
	shardsM := shards[(n + localM*idx):(n + localM*(idx+1))]
	shardsL := shards[(n + m + localL*idx):(n + m + localL*(idx+1))]

	copy(localShardsInIdc, shardsN)
	copy(localShardsInIdc[localN:], shardsM)
	copy(localShardsInIdc[localN+localM:], shardsL)
	return localShardsInIdc
}

func (e *lrcEncoder) Join(dst io.Writer, shards [][]byte, outSize int) error {
	return e.engine.Join(dst, shards[:(e.CodeMode.N+e.CodeMode.M)], outSize)
}
