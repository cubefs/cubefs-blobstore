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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"

	"github.com/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/blobstore/util/log"
)

type Service struct {
	cfg       config
	store     *Store
	raft      raftserver.RaftServer
	leader    uint64
	raftReady chan struct{}
	httpSrv   *http.Server
}

func NewService(cfg config) *Service {
	s := &Service{
		cfg:       cfg,
		raftReady: make(chan struct{}, 1),
	}

	store, err := NewStore(cfg.DataDir, s.handleLeaderChange)
	if err != nil {
		log.Panicf("new store error: %v", err)
	}

	applied, err := store.Applied()
	if err != nil {
		log.Panicf("restore applied error: %v", err)
	}

	raftCfg := &raftserver.Config{
		NodeId:         cfg.NodeID,
		ListenPort:     int(cfg.RaftListenPort),
		WalDir:         cfg.WalDir,
		TickInterval:   cfg.TickInterval,
		HeartbeatTick:  cfg.HeartbeatTick,
		ElectionTick:   cfg.ElectionTick,
		ProposeTimeout: cfg.ProposeTimeout,
		Applied:        applied,
		Peers:          cfg.RaftPeers,
		KV:             store,
		SM:             store,
	}
	rs, err := raftserver.NewRaftServer(raftCfg)
	if err != nil {
		log.Fatal(err)
	}
	store.rs = rs
	s.store = store
	s.raft = rs

	r := mux.NewRouter()
	r.Methods(http.MethodPut).Path("/data").HandlerFunc(s.handlePut)
	r.Methods(http.MethodGet).Path("/data").HandlerFunc(s.handleGet)
	r.Methods(http.MethodPut).Path("/member").HandlerFunc(s.handleAddMember)
	r.Methods(http.MethodDelete).Path("/member").HandlerFunc(s.handleDeleteMember)
	r.Methods(http.MethodGet).Path("/members").HandlerFunc(s.handleGetMembers)
	r.Methods(http.MethodPost).Path("/leader").HandlerFunc(s.handleChangeLeader)
	r.Methods(http.MethodGet).Path("/status").HandlerFunc(s.handleStatus)

	s.httpSrv = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.cfg.ListenPort),
		Handler: r,
	}
	return s
}

func (s *Service) Start() {
	<-s.raftReady
	for {
		// wait wal log applied
		if err := s.raft.ReadIndex(context.TODO()); err == nil {
			break
		}
	}
	log.Infof("node[%d] raft has ready, start http server", s.cfg.NodeID)
	log.Fatal(s.httpSrv.ListenAndServe())
}

func (s *Service) handleLeaderChange(leader uint64, host string) {
	atomic.StoreUint64(&s.leader, leader)
	select {
	case s.raftReady <- struct{}{}:
	default:
	}
	log.Infof("node[%d] id: %d host: %s is leader", s.cfg.NodeID, leader, host)
}

func (s *Service) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	val := r.FormValue("value")
	if key == "" || val == "" {
		log.Errorf("invalid parament key=%s value=%s", key, val)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	cmd := make([]byte, 1+8+len(key)+len(val))
	cmd[0] = '1'
	binary.BigEndian.PutUint32(cmd[1:], uint32(len(key)))
	binary.BigEndian.PutUint32(cmd[5:], uint32(len(val)))
	copy(cmd[9:], []byte(key))
	copy(cmd[9+len(key):], []byte(val))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.raft.Propose(ctx, cmd); err != nil {
		log.Errorf("put key: %s value: %s error: %v", key, val, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	if key == "" {
		log.Error("invalid parament key=")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infof("get key=%s", key)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.raft.ReadIndex(ctx); err != nil {
		log.Errorf("readindex key: %s error: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	val, err := s.store.Get([]byte(key))
	if err != nil {
		log.Errorf("get key: %s error: %v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if len(val) == 0 {
		log.Errorf("not found value for key=%s", key)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Write(val)
}

func (s *Service) handleAddMember(w http.ResponseWriter, r *http.Request) {
	nodeid := r.FormValue("nodeid")
	host := r.FormValue("host")
	learner := r.FormValue("learner")

	if nodeid == "" || host == "" {
		log.Errorf("invalid parament nodeid=%s host=%s learner=%s", nodeid, host, learner)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseUint(nodeid, 10, 64)
	if err != nil {
		log.Errorf("invalid parament nodeid=%s", nodeid)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if learner == "true" {
		if err := s.raft.AddLearner(ctx, id, host); err != nil {
			log.Errorf("add learner nodeid=%d host=%s error: %s", id, host, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		if err := s.raft.AddMember(ctx, id, host); err != nil {
			log.Errorf("add member nodeid=%d host=%s error: %s", id, host, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

func (s *Service) handleDeleteMember(w http.ResponseWriter, r *http.Request) {
	nodeid := r.FormValue("nodeid")
	if nodeid == "" {
		log.Errorf("invalid parament nodeid=%s", nodeid)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseUint(nodeid, 10, 64)
	if err != nil {
		log.Errorf("invalid parament nodeid=%s", nodeid)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.raft.RemoveMember(ctx, id); err != nil {
		log.Errorf("remove member %d error: %s", id, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleGetMembers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.raft.ReadIndex(ctx); err != nil {
		log.Errorf("readindex error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	val, err := s.store.Get(memberKey)
	if err != nil {
		log.Errorf("get members error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(val) == 0 {
		log.Error("not found members")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Write(val)
}

func (s *Service) handleChangeLeader(w http.ResponseWriter, r *http.Request) {
	nodeid := r.FormValue("nodeid")
	if nodeid == "" {
		log.Error("invalid parament nodeid=")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseUint(nodeid, 10, 64)
	if err != nil {
		log.Errorf("invalid parament nodeid=%s", nodeid)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.raft.TransferLeadership(ctx, s.leader, id)
}

func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.raft.Status()
	body, _ := json.Marshal(status)
	w.Write(body)
}
