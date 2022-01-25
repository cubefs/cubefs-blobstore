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

package clustermgr

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/blobstore/common/codemode"
	"github.com/cubefs/blobstore/common/raftserver"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestStateMachine(t *testing.T) {
	srcService := initTestService(t)
	defer clear(srcService)
	defer srcService.Close()

	srcClusterClient := initTestClusterClient(srcService)

	// change another listen port
	oldPort := testServiceCfg.RaftConfig.ServerConfig.ListenPort
	oldPeers := testServiceCfg.RaftConfig.ServerConfig.Peers
	testServiceCfg.RaftConfig.ServerConfig.ListenPort = GetFreePort()
	testServiceCfg.RaftConfig.ServerConfig.Peers = map[uint64]string{1: "127.0.0.1:65342"}
	testServiceCfg.CodeModePolicies = []codemode.Policy{
		{
			ModeName:  codemode.EC15P12.Name(),
			MinSize:   1048577,
			MaxSize:   1073741824,
			SizeRatio: 0.8,
			Enable:    false,
		},
		{
			ModeName:  codemode.EC6P6.Name(),
			MinSize:   0,
			MaxSize:   1048576,
			SizeRatio: 0.2,
			Enable:    false,
		},
	}
	destService := initTestService(t)
	testServiceCfg.RaftConfig.ServerConfig.ListenPort = oldPort
	testServiceCfg.RaftConfig.ServerConfig.Peers = oldPeers
	defer clear(destService)
	defer destService.Close()

	insertDiskInfos(t, srcClusterClient, 1, 10, "z0")

	// test snapshot
	{
		snapshot, err := srcService.Snapshot()
		assert.NoError(t, err)

		meta := raftpb.SnapshotMetadata{Index: snapshot.Index()}
		err = destService.ApplySnapshot(raftserver.SnapshotMeta(meta), snapshot)
		assert.NoError(t, err)
	}
}
