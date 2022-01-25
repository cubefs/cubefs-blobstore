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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cubefs/blobstore/util/log"
)

type config struct {
	NodeID         uint64            `json:"node_id"`
	ListenPort     uint16            `json:"listen_port"`
	RaftListenPort uint16            `json:"raft_listen_port"`
	DataDir        string            `json:"data_dir"`
	WalDir         string            `json:"wal_dir"`
	RaftPeers      map[uint64]string `json:"raft_peers"`
	ConcurrentNum  int               `json:"concurrent_num"`
	TickInterval   int               `json:"raft_tick_interval"`
	HeartbeatTick  int               `json:"raft_heartbeat_tick"`
	ElectionTick   int               `json:"raft_election_tick"`
	ProposeTimeout int               `json:"raft_propose_timeout"`
}

func main() {
	log.SetOutputLevel(log.Ldebug)
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s [configFile]\n", os.Args[0])
		return
	}
	b, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		log.Errorf("read %s error: %v", os.Args[1], err)
		return
	}
	var cfg config
	if err = json.Unmarshal(b, &cfg); err != nil {
		log.Errorf("unmarshal %s error: %v", os.Args[1], err)
		return
	}

	s := NewService(cfg)
	s.Start()
}
