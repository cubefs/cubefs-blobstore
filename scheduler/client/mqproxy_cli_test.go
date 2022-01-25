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

package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	cmapi "github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/api/mqproxy"
)

type mockMqproxy struct{}

func (proxy *mockMqproxy) SendDeleteMsg(ctx context.Context, info *mqproxy.DeleteArgs) error {
	return nil
}

func (proxy *mockMqproxy) SendShardRepairMsg(ctx context.Context, info *mqproxy.ShardRepairArgs) error {
	return nil
}

func TestMqCli(t *testing.T) {
	cli := &mqProxyClient{
		cli:       &mockMqproxy{},
		clusterID: 1,
	}
	err := cli.SendShardRepairMsg(context.Background(), 0, 0, []uint8{0})
	require.NoError(t, err)

	defer func() {
		recover()
	}()
	_, err = NewMqProxyClient(&mqproxy.LbConfig{}, &cmapi.Config{}, 1)
	require.Error(t, err)
}
