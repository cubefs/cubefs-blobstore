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

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/tecbot/gorocksdb"

	"github.com/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/blobstore/util/log"
)

var (
	appliedKey = []byte("$applied")
	memberKey  = []byte(raftserver.RaftMemberKey)
)

type snapshot struct {
	s     *Store
	name  string
	index uint64
	db    *gorocksdb.DB
	ro    *gorocksdb.ReadOptions
	st    *gorocksdb.Snapshot
	iter  *gorocksdb.Iterator
}

func (s *snapshot) Name() string {
	return s.name
}

func (s *snapshot) Index() uint64 {
	return s.index
}

func (s *snapshot) Read() (data []byte, err error) {
	for i := 0; i < 1024; i++ {
		if !s.iter.Valid() {
			break
		}
		key := s.iter.Key()
		value := s.iter.Value()
		b := make([]byte, 8+key.Size()+value.Size())
		binary.BigEndian.PutUint32(b, uint32(key.Size()))
		binary.BigEndian.PutUint32(b[4:], uint32(value.Size()))
		copy(b[8:], key.Data())
		copy(b[8+key.Size():], value.Data())
		data = append(data, b...)
		key.Free()
		value.Free()
		s.iter.Next()
		err = s.iter.Err()
		if err != nil {
			log.Errorf("snapshot iter Next error: %v", err)
			key.Free()
			value.Free()
			break
		}
	}
	if len(data) == 0 && err == nil {
		err = io.EOF
	}
	return data, err
}

func (s *snapshot) Close() {
	s.db.ReleaseSnapshot(s.st)
	s.ro.Destroy()
	atomic.AddInt32(&s.s.snapshotNum, -1)
}

type Store struct {
	rs                 raftserver.RaftServer
	dir                string
	db                 *gorocksdb.DB
	applied            uint64
	stableApplied      uint64
	snapshotNum        int32
	handleLeaderChange func(leader uint64, host string)
}

func NewStore(dir string, handleLeaderChange func(leader uint64, host string)) (*Store, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}

	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(256 * 1024 * 1024))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetMaxWriteBufferNumber(2)
	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		err = fmt.Errorf("action[openRocksDB],err:%v", err)
		return nil, err
	}
	store := &Store{
		dir:                dir,
		db:                 db,
		handleLeaderChange: handleLeaderChange,
	}
	applied, err := store.Applied()
	if err != nil {
		err = fmt.Errorf("get applied error: %v", err)
		return nil, err
	}
	store.applied = applied
	return store, nil
}

func (s *Store) newSnapshot(index uint64) raftserver.Snapshot {
	st := s.db.NewSnapshot()
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetSnapshot(st)
	ro.SetVerifyChecksums(true)
	iter := s.db.NewIterator(ro)
	iter.SeekToFirst()
	atomic.AddInt32(&s.snapshotNum, 1)
	return &snapshot{
		s:     s,
		name:  uuid.New().String(), // name must unique
		index: index,
		db:    s.db,
		ro:    ro,
		st:    st,
		iter:  iter,
	}
}

func (s *Store) Applied() (uint64, error) {
	val, err := s.Get(appliedKey)
	if err != nil {
		return 0, err
	}
	if len(val) > 0 {
		return binary.BigEndian.Uint64(val), nil
	}
	return 0, nil
}

func (s *Store) Put(key, val []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	// wo.SetSync(true)
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	wb.Put(key, val)
	if err := s.db.Write(wo, wb); err != nil {
		return err
	}
	return nil
}

func (s *Store) SyncPut(key, val []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(true)
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	wb.Put(key, val)
	if err := s.db.Write(wo, wb); err != nil {
		return err
	}
	return nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	return s.db.GetBytes(ro, key)
}

func (s *Store) decodeAndPut(data []byte) error {
	// decode key and value from data
	// ignore type
	keyLen := binary.BigEndian.Uint32(data[1:])
	valLen := binary.BigEndian.Uint32(data[5:])
	key := data[9 : 9+keyLen]
	val := data[9+keyLen : 9+keyLen+valLen]

	// log.Infof("put key(%s) val(%s)", string(key), string(val))
	return s.Put(key, val)
}

func (s *Store) Apply(datas [][]byte, index uint64) error {
	for _, data := range datas {
		s.decodeAndPut(data)
	}
	if index-s.stableApplied >= 40000 && atomic.LoadInt32(&s.snapshotNum) <= 0 {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, index)
		if err := s.SyncPut(appliedKey, b); err != nil {
			return err
		}
		s.rs.Truncate(s.stableApplied)
		s.stableApplied = index
	}
	s.applied = index
	return nil
}

func (s *Store) ApplyMemberChange(cc raftserver.ConfChange, index uint64) error {
	// store applied
	log.Infof("member change type=%s nodeid=%d index=%d", cc.Type.String(), cc.NodeID, index)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, index)
	if err := s.SyncPut(appliedKey, b); err != nil {
		return err
	}
	if index-s.stableApplied >= 40000 && atomic.LoadInt32(&s.snapshotNum) <= 0 {
		s.rs.Truncate(s.stableApplied)
	}
	s.stableApplied = index
	s.applied = index
	return nil
}

func (s *Store) Snapshot() (raftserver.Snapshot, error) {
	return s.newSnapshot(s.applied), nil
}

func (s *Store) ApplySnapshot(meta raftserver.SnapshotMeta, st raftserver.Snapshot) error {
	// TODO clean rocksdb
	var n int
	for {
		data, err := st.Read()
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			break
		}
		// decode snapshot
		for {
			if len(data) == 0 {
				break
			}
			if len(data) < 8 {
				return fmt.Errorf("Invalid snapshot: data len(%d)", len(data))
			}
			keyLen := binary.BigEndian.Uint32(data)
			valLen := binary.BigEndian.Uint32(data[4:])
			key := data[8 : 8+keyLen]
			val := data[8+keyLen : 8+keyLen+valLen]
			if !bytes.Equal(key, appliedKey) && !bytes.Equal(key, memberKey) {
				if err = s.Put(key, val); err != nil {
					log.Errorf("ApplySnapshot key(%s) val(%s) error: %v", string(key), string(val), err)
					return err
				}
			}
			n++
			data = data[8+keyLen+valLen:]
		}
	}

	log.Infof("apply snapshot success: %d records", n)
	// store applied
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, meta.Index)
	if err := s.SyncPut(appliedKey, b); err != nil {
		return nil
	}
	s.stableApplied = meta.Index
	s.applied = meta.Index
	return nil
}

func (s *Store) LeaderChange(leader uint64, host string) {
	log.Infof("leader change id: %d host: %s", leader, host)
	if leader != 0 && s.handleLeaderChange != nil {
		s.handleLeaderChange(leader, host)
	}
}
