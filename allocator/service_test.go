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

package allocator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	api "github.com/cubefs/blobstore/api/allocator"
	"github.com/cubefs/blobstore/common/codemode"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const AllocUrl = "/volume/alloc"

func TestNewSvr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmcli := MockClusterCli(ctrl)
	s := New(cmcli, Config{})
	ctx := context.Background()
	time.Sleep(time.Duration(1) * time.Second)
	vids, vols, err := s.volumeMgr.List(ctx, codemode.CodeMode(2))
	require.NoError(t, err)
	t.Log(vids, vols)

	allocServer := httptest.NewServer(NewHandler(s))
	allocUrl := allocServer.URL + AllocUrl
	{
		args := api.AllocVolsArgs{
			Fsize:    100,
			BidCount: 1,
			CodeMode: codemode.CodeMode(2),
		}
		b, err := json.Marshal(args)
		if err != nil {
			t.Error(err)
		}
		_, err = httpReq(http.MethodPost, allocUrl, bytes.NewBuffer(b))
		require.NoError(t, err)
	}
	{
		args := api.AllocVolsArgs{
			Fsize:    2147483648,
			BidCount: 5,
			CodeMode: codemode.CodeMode(2),
		}
		b, err := json.Marshal(args)
		if err != nil {
			t.Error(err)
		}
		_, err = httpReq(http.MethodPost, allocUrl, bytes.NewBuffer(b))
		require.NoError(t, err)
	}
	{
		args := api.AllocVolsArgs{
			Fsize:    16106127360,
			BidCount: 5,
			CodeMode: codemode.CodeMode(2),
		}
		b, err := json.Marshal(args)
		if err != nil {
			t.Error(err)
		}
		_, err = httpReq(http.MethodPost, allocUrl, bytes.NewBuffer(b))
		require.NoError(t, err)
	}
	{
		args := api.AllocVolsArgs{
			Fsize:    16642998272,
			BidCount: 12,
			CodeMode: codemode.CodeMode(2),
		}
		b, err := json.Marshal(args)
		if err != nil {
			t.Error(err)
		}
		_, err = httpReq(http.MethodPost, allocUrl, bytes.NewBuffer(b))
		require.NoError(t, err)
	}

	{
		listUrl := allocServer.URL + "/volume/list?code_mode=3"
		_, err := httpReq(http.MethodGet, listUrl, nil)
		require.NoError(t, err)
	}
	{
		listUrl := allocServer.URL + "/volume/list?code_mode=2"
		_, err := httpReq(http.MethodGet, listUrl, nil)
		require.NoError(t, err)
	}
	{
		listUrl := allocServer.URL + "/volume/list?code_mode=11"
		_, err := httpReq(http.MethodGet, listUrl, nil)
		require.NoError(t, err)
	}
}

func httpReq(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return req, err
}
