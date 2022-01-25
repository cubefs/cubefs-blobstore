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

	api "github.com/cubefs/blobstore/api/tinker"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
)

// ITinker define the interface of tinker used by scheduler
type ITinker interface {
	UpdateVol(ctx context.Context, host string, vid proto.Vid, clusterID proto.ClusterID) (err error)
}

// TinkerClient tinker client
type TinkerClient struct {
	tinkerCli api.VolInfoUpdater
}

// NewTinkerClient returns tinker client
func NewTinkerClient(conf *api.Config) ITinker {
	return &TinkerClient{
		tinkerCli: api.New(conf),
	}
}

// UpdateVol update volume cache
func (c TinkerClient) UpdateVol(ctx context.Context, host string, vid proto.Vid, clusterID proto.ClusterID) (err error) {
	pSpan := trace.SpanFromContextSafe(ctx)
	_, ctx = trace.StartSpanFromContextWithTraceID(context.Background(), "UpdateVol", pSpan.TraceID())
	err = c.tinkerCli.UpdateVolInfo(ctx, host, vid)
	return
}
