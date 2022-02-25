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

package controller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/access/controller"
	"github.com/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/blobstore/common/redis"
	"github.com/cubefs/blobstore/common/rpc"
)

var (
	hostAddr string
	consulKV api.KVPairs = nil

	cc, cc0, cc1, cc2, cc3 controller.ClusterController
)

func initCluster() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", consul)
	mux.HandleFunc("/service/get", serviceGet)

	testServer := httptest.NewServer(mux)
	hostAddr = testServer.URL

	cluster1 := clustermgr.ClusterInfo{
		Region:    region,
		ClusterID: 1,
		Capacity:  1 << 50,
		Available: 1 << 45,
		Readonly:  false,
		Nodes:     []string{hostAddr, hostAddr},
	}
	data1, _ := json.Marshal(cluster1)

	cluster2 := clustermgr.ClusterInfo{
		Region:    region,
		ClusterID: 2,
		Capacity:  1 << 50,
		Available: 1 << 45,
		Readonly:  true,
		Nodes:     []string{hostAddr, hostAddr},
	}
	data2, _ := json.Marshal(cluster2)

	cluster3 := clustermgr.ClusterInfo{
		Region:    region,
		ClusterID: 3,
		Capacity:  1 << 50,
		Available: -1024,
		Readonly:  false,
		Nodes:     []string{hostAddr, hostAddr},
	}
	data3, _ := json.Marshal(cluster3)

	consulKV = api.KVPairs{
		&api.KVPair{Key: "1", Value: data1},
		&api.KVPair{Key: "2", Value: data2},
		&api.KVPair{Key: "3", Value: data3},
		&api.KVPair{Key: "4", Value: data1},
		&api.KVPair{Key: "5", Value: []byte("{invalid json")},
		&api.KVPair{Key: "cannot-parse-key", Value: []byte("{}")},
	}

	time.Sleep(time.Millisecond * 200)
	initCC()
}

func consul(w http.ResponseWriter, req *http.Request) {
	data, _ := json.Marshal(consulKV)
	w.Write(data)
}

func serviceGet(w http.ResponseWriter, req *http.Request) {
	if rand.Int31()%2 == 0 {
		w.Write([]byte("{cannot unmarshal"))
	} else {
		w.Write([]byte("{}"))
	}
}

func initCC() {
	count := 0
	for cc == nil || cc0 == nil || cc1 == nil || cc2 == nil || cc3 == nil {
		c := newCC()
		if cc == nil {
			cc = c
		}

		clusters := c.All()
		if len(clusters) == 0 {
			if cc0 == nil {
				cc0 = c
			}
			continue
		}

		if len(clusters) == 1 {
			switch clusters[0].ClusterID {
			case 1:
				if cc1 == nil {
					cc1 = c
				}
			case 2:
				if cc2 == nil {
					cc2 = c
				}
			case 3:
				if cc3 == nil {
					cc3 = c
				}
			}
		}

		count++
	}
	fmt.Println("init clusters run:", count)
}

func newCC() controller.ClusterController {
	cfg := controller.ClusterConfig{
		Region:            region,
		ClusterReloadSecs: 0,
		CMClientConfig: clustermgr.Config{
			LbConfig: rpc.LbConfig{
				Hosts: []string{"http://localhost"},
			},
		},
		RedisClientConfig: redis.ClusterConfig{
			Addrs: []string{redismr.Addr()},
		},
	}
	if rand.Int31()%2 == 0 {
		cfg.RedisClientConfig.Addrs = nil
	}
	conf := api.DefaultConfig()
	conf.Address = hostAddr
	client, _ := api.NewClient(conf)
	cc, err := controller.NewClusterController(&cfg, client)
	if err != nil {
		panic(err)
	}
	return cc
}

func TestAccessClusterNew(t *testing.T) {
	require.Equal(t, region, cc.Region())
}

func TestAccessClusterAll(t *testing.T) {
	for ii := 0; ii < 10; ii++ {
		require.LessOrEqual(t, len(newCC().All()), 3)
	}
}

func TestAccessClusterChooseOne(t *testing.T) {
	{
		cluster, err := cc0.ChooseOne()
		require.Error(t, err)
		require.Equal(t, (*clustermgr.ClusterInfo)(nil), cluster)
	}
	{
		_, err := cc3.ChooseOne()
		require.Error(t, err)

		_, err = cc2.ChooseOne()
		require.Error(t, err)

		cc2.ChangeChooseAlg(controller.AlgRandom)
		_, err = cc2.ChooseOne()
		require.Error(t, err)
	}
	{
		cluster, err := cc1.ChooseOne()
		require.NoError(t, err)
		require.NotNil(t, cluster)
		for ii := 0; ii < 100; ii++ {
			clusterx, err := cc1.ChooseOne()
			require.NoError(t, err)
			require.Equal(t, cluster, clusterx)
		}

		cc1.ChangeChooseAlg(controller.AlgRandom)
		for ii := 0; ii < 100; ii++ {
			clusterx, err := cc1.ChooseOne()
			require.NoError(t, err)
			require.Equal(t, cluster, clusterx)
		}
	}
}

func TestAccessClusterGetHandler(t *testing.T) {
	{
		service, err := cc2.GetServiceController(1)
		require.Error(t, err)
		require.Equal(t, nil, service)

		getter, err := cc2.GetVolumeGetter(1)
		require.Error(t, err)
		require.Equal(t, nil, getter)

		_, err = cc2.GetConfig(context.TODO(), "key")
		require.Error(t, err)
	}
	{
		service, err := cc1.GetServiceController(1)
		require.NoError(t, err)
		require.NotNil(t, service)

		getter, err := cc1.GetVolumeGetter(1)
		require.NoError(t, err)
		require.NotNil(t, getter)

		_, err = cc1.GetConfig(context.TODO(), "key")
		require.Error(t, err)
	}
}

func TestAccessClusterChangeChooseAlg(t *testing.T) {
	cases := []struct {
		alg controller.AlgChoose
		err error
	}{
		{0, controller.ErrInvalidChooseAlg},
		{controller.AlgAvailable, nil},
		{controller.AlgRandom, nil},
		{1024, controller.ErrInvalidChooseAlg},
	}

	for _, cs := range cases {
		err := cc.ChangeChooseAlg(cs.alg)
		require.Equal(t, err, cs.err)
	}
}
