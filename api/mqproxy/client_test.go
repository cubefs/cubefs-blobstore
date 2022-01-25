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

package mqproxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/api/clustermgr"
	c "github.com/cubefs/blobstore/common/rpc"
)

func TestLbClient_SendShardRepairMsg(t *testing.T) {
	mqproxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
	}))

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(fmt.Sprintf(`{"nodes":[{"cluster_id":1,"name":"MQPROXY","host":"%s","idc":"z0"}]}`, mqproxyServer.URL)))
	}))

	lbCfg := c.LbConfig{
		Hosts: []string{s.URL},
	}

	cmCfg := clustermgr.Config{LbConfig: lbCfg}
	cm := clustermgr.New(&cmCfg)
	cli, err := NewLbClient(&LbConfig{
		Config:             Config{},
		RetryHostsCnt:      0,
		HostSyncIntervalMs: 0,
	}, cm, 1)

	require.NoError(t, err)
	err = cli.SendShardRepairMsg(context.Background(), &ShardRepairArgs{
		ClusterID: 0,
		Bid:       0,
		Vid:       0,
		BadIdxes:  nil,
		Reason:    "test",
	})
	require.NoError(t, err)
}

func TestLbClient_SendShardRepairMsg_failed(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(fmt.Sprintf(`{"nodes":[{"cluster_id":1,"name":"MQPROXY","host":"%s","idc":"z0"}]}`, "abc.com")))
	}))

	lbCfg := c.LbConfig{
		Hosts: []string{s.URL},
	}

	cmCfg := clustermgr.Config{LbConfig: lbCfg}
	cm := clustermgr.New(&cmCfg)
	cli, err := NewLbClient(&LbConfig{
		Config:             Config{},
		RetryHostsCnt:      0,
		HostSyncIntervalMs: 0,
	}, cm, 1)

	require.NoError(t, err)
	err = cli.SendShardRepairMsg(context.Background(), &ShardRepairArgs{
		ClusterID: 0,
		Bid:       0,
		Vid:       0,
		BadIdxes:  nil,
		Reason:    "test",
	})
	require.Error(t, err)
}

func TestLbClient_BlobDelete_failed(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(fmt.Sprintf(`{"nodes":[{"cluster_id":1,"name":"MQPROXY","host":"%s","idc":"z0"}]}`, "abc.com")))
	}))

	lbCfg := c.LbConfig{
		Hosts: []string{s.URL},
	}

	cmCfg := clustermgr.Config{LbConfig: lbCfg}
	cm := clustermgr.New(&cmCfg)
	cli, err := NewLbClient(&LbConfig{
		Config:             Config{},
		RetryHostsCnt:      0,
		HostSyncIntervalMs: 0,
	}, cm, 1)

	require.NoError(t, err)
	err = cli.SendDeleteMsg(context.Background(), &DeleteArgs{
		ClusterID: 0,
		Blobs:     nil,
	})
	require.Error(t, err)
}

func TestLbClient_BlobDelete(t *testing.T) {
	mqproxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
	}))

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(fmt.Sprintf(`{"nodes":[{"cluster_id":1,"name":"MQPROXY","host":"%s","idc":"z0"}]}`, mqproxyServer.URL)))
	}))

	lbCfg := c.LbConfig{
		Hosts: []string{s.URL},
	}

	cmCfg := clustermgr.Config{LbConfig: lbCfg}
	cm := clustermgr.New(&cmCfg)
	cli, err := NewLbClient(&LbConfig{
		Config:             Config{},
		RetryHostsCnt:      0,
		HostSyncIntervalMs: 0,
	}, cm, 1)

	require.NoError(t, err)
	err = cli.SendDeleteMsg(context.Background(), &DeleteArgs{
		ClusterID: 0,
		Blobs:     nil,
	})
	require.NoError(t, err)
}
