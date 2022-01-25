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
	"errors"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"

	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/util/taskpool"
)

const (
	defaultConcurrency = 100
)

var (
	ErrShortData       = errors.New("short data")
	ErrInvalidConfig   = errors.New("invalid config")
	ErrInvalidCodeMode = errors.New("invalid code mode")
	ErrVerify          = errors.New("shards verify failed")
	ErrInvalidShards   = errors.New("invalid shards")
)

// normal ec encoder, implements all these function
type Encoder interface {
	// encode source data into shards, whatever normal ec or LRC
	Encode(shards [][]byte) error
	// reconstruct all missing shards, you should assign the missing or bad idx in shards
	Reconstruct(shards [][]byte, badIdx []int) error
	// only reconstruct data shards, you should assign the missing or bad idx in shards
	ReconstructData(shards [][]byte, badIdx []int) error
	// split source data into adapted shards size
	Split(data []byte) ([][]byte, error)
	// get data shards(No-Copy)
	GetDataShards(shards [][]byte) [][]byte
	// get parity shards(No-Copy)
	GetParityShards(shards [][]byte) [][]byte
	// get local shards(LRC model, No-Copy)
	GetLocalShards(shards [][]byte) [][]byte
	// get shards in an idc
	GetShardsInIdc(shards [][]byte, idx int) [][]byte
	// output source data into dst(io.Writer)
	Join(dst io.Writer, shards [][]byte, outSize int) error
	// verify parity shards with data shards
	Verify(shards [][]byte) (bool, error)
}

type Config struct {
	CodeMode     codemode.Tactic
	EnableVerify bool
	Concurrency  int
}

type encoder struct {
	engine   reedsolomon.Encoder
	taskPool taskpool.TaskPool
	*Config
}

// NewEncoder return an encoder which support normal EC or LRC
func NewEncoder(cfg *Config) (Encoder, error) {
	if cfg == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.CodeMode.AZCount == 0 || cfg.CodeMode.N == 0 || cfg.CodeMode.M == 0 {
		return nil, ErrInvalidCodeMode
	}

	var tp taskpool.TaskPool
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
	}
	tp = taskpool.New(cfg.Concurrency, cfg.Concurrency)

	engine, err := reedsolomon.New(cfg.CodeMode.N, cfg.CodeMode.M)
	if err != nil {
		return nil, err
	}

	if cfg.CodeMode.L != 0 {
		localN := (cfg.CodeMode.N + cfg.CodeMode.M) / cfg.CodeMode.AZCount
		localM := cfg.CodeMode.L / cfg.CodeMode.AZCount
		localEngine, err := reedsolomon.New(localN, localM)
		if err != nil {
			return nil, err
		}
		return &lrcEncoder{
			engine:      engine,
			localEngine: localEngine,
			taskPool:    tp,
			Config:      cfg,
		}, nil
	}

	return &encoder{
		engine:   engine,
		taskPool: tp,
		Config:   cfg,
	}, nil
}

func (e *encoder) Encode(shards [][]byte) error {
	var (
		retErr error
		wg     = sync.WaitGroup{}
	)

	wg.Add(1)
	e.taskPool.Run(func() {
		defer wg.Done()
		err := e.engine.Encode(shards)
		if err != nil {
			retErr = err
			return
		}
		if e.EnableVerify {
			ok, err := e.engine.Verify(shards)
			if err != nil {
				retErr = err
				return
			}
			if !ok {
				retErr = err
				return
			}
		}
	})
	wg.Wait()

	return retErr
}

func (e *encoder) Verify(shards [][]byte) (bool, error) {
	var (
		ok  bool
		err error
		wg  = sync.WaitGroup{}
	)
	wg.Add(1)
	e.taskPool.Run(func() {
		defer wg.Done()
		ok, err = e.engine.Verify(shards)
	})
	wg.Wait()
	return ok, err
}

func (e *encoder) Reconstruct(shards [][]byte, badIdx []int) error {
	initBadShards(shards, badIdx)
	var (
		retErr error
		wg     = sync.WaitGroup{}
	)
	wg.Add(1)
	e.taskPool.Run(func() {
		defer wg.Done()
		retErr = e.engine.Reconstruct(shards)
	})
	wg.Wait()
	return retErr
}

func (e *encoder) ReconstructData(shards [][]byte, badIdx []int) error {
	initBadShards(shards, badIdx)

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

func (e *encoder) Split(data []byte) ([][]byte, error) {
	return e.engine.Split(data)
}

func (e *encoder) GetDataShards(shards [][]byte) [][]byte {
	return shards[:e.CodeMode.N]
}

func (e *encoder) GetParityShards(shards [][]byte) [][]byte {
	return shards[e.CodeMode.N:]
}

func (e *encoder) GetLocalShards(shards [][]byte) [][]byte {
	return nil
}

func (e *encoder) GetShardsInIdc(shards [][]byte, idx int) [][]byte {
	n, m := e.CodeMode.N, e.CodeMode.M
	idcCnt := e.CodeMode.AZCount

	localN, localM := n/idcCnt, m/idcCnt

	return append(shards[idx*localN:(idx+1)*localN], shards[n+localM*idx:n+localM*(idx+1)]...)
}

func (e *encoder) Join(dst io.Writer, shards [][]byte, outSize int) error {
	return e.engine.Join(dst, shards, outSize)
}

func initBadShards(shards [][]byte, badIdx []int) {
	for _, i := range badIdx {
		if shards[i] != nil && len(shards[i]) != 0 && cap(shards[i]) > 0 {
			shards[i] = shards[i][:0]
		}
	}
}
