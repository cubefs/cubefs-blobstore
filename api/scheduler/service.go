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

package scheduler

import (
	"context"
	"fmt"

	"github.com/cubefs/blobstore/common/proto"
)

type RegisterServiceArgs struct {
	ClusterID proto.ClusterID `json:"cluster_id"`
	Module    string          `json:"module"`
	Host      string          `json:"host"`
	Idc       string          `json:"idc"`
}

func (args *RegisterServiceArgs) Valid(shouldEqualCID proto.ClusterID) bool {
	if args.Host == "" || args.ClusterID != shouldEqualCID || args.Module == "" {
		return false
	}
	return true
}

func (c *client) RegisterService(ctx context.Context, args *RegisterServiceArgs) (err error) {
	return c.PostWith(ctx, c.Host+"/service/register", nil, args)
}

type DeleteServiceArgs struct {
	Host string `json:"host"`
}

func (c *client) DeleteService(ctx context.Context, args *DeleteServiceArgs) (err error) {
	return c.PostWith(ctx, c.Host+"/service/delete", nil, args)
}

type FindServiceArgs struct {
	Host string `json:"host"`
}

func (c *client) GetService(ctx context.Context, host string) (*proto.SvrInfo, error) {
	var ret proto.SvrInfo
	urlStr := fmt.Sprintf("%v/service/get?host=%v", c.Host, host)
	err := c.GetWith(ctx, urlStr, &ret)
	return &ret, err
}

type ListServicesArgs struct {
	Module string `json:"module,omitempty"`
	IDC    string `json:"idc,omitempty"`
}

func (c *client) ListServices(ctx context.Context, args *ListServicesArgs) ([]*proto.SvrInfo, error) {
	var ret []*proto.SvrInfo
	urlStr := fmt.Sprintf("%v/service/list?module=%s&idc=%s", c.Host, args.Module, args.IDC)
	err := c.GetWith(ctx, urlStr, &ret)
	return ret, err
}
