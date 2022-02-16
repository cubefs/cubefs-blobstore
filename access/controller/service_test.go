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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/access/controller"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
)

var serviceName = proto.ServiceNameAllocator

type hostSet map[string]struct{}

func (set hostSet) Keys() []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}

var _, serviceCtx = trace.StartSpanFromContext(context.Background(), "TestAccessService")

func TestAccessServiceNew(t *testing.T) {
	{
		sc, err := controller.NewServiceController(
			controller.ServiceConfig{
				ClusterID: 0,
				IDC:       idc,
			}, cmcli)
		require.NoError(t, err)

		_, err = sc.GetServiceHost(serviceCtx, serviceName)
		require.NoError(t, err)
	}
	{
		sc, err := controller.NewServiceController(
			controller.ServiceConfig{
				ClusterID: 0,
				IDC:       idc + "x",
				ReloadSec: 1,
			}, cmcli)
		require.NoError(t, err)

		_, err = sc.GetServiceHost(serviceCtx, serviceName)
		require.Error(t, err)

		time.Sleep(time.Second * 2)
	}
}

func TestAccessServiceGetServiceHost(t *testing.T) {
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{
			ClusterID: 0,
			IDC:       idc,
			ReloadSec: 1,
		}, cmcli)
	require.NoError(t, err)

	keys := make(hostSet)
	for ii := 0; ii < 100; ii++ {
		host, err := sc.GetServiceHost(serviceCtx, serviceName)
		require.NoError(t, err)
		keys[host] = struct{}{}
	}
	require.ElementsMatch(t, keys.Keys(), []string{"allocator-1", "allocator-2"})

	hosts, err := sc.GetServiceHosts(serviceCtx, serviceName)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"allocator-1", "allocator-2"}, hosts)
}

func TestAccessServicePunishService(t *testing.T) {
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{
			ClusterID: 0,
			IDC:       idc,
			ReloadSec: 1,
		}, cmcli)
	require.NoError(t, err)

	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"allocator-1", "allocator-2"})
	}

	sc.PunishService(serviceCtx, serviceName, "allocator-2", 2)
	for ii := 0; ii < 100; ii++ {
		host, err := sc.GetServiceHost(serviceCtx, serviceName)
		require.NoError(t, err)
		require.True(t, host == "allocator-1")
	}

	time.Sleep(time.Second * 2)
	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"allocator-1", "allocator-2"})
	}
}

func TestAccessServicePunishServiceWithThreshold(t *testing.T) {
	threshold := uint32(5)
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{
			ClusterID:                   0,
			IDC:                         idc,
			ReloadSec:                   1,
			ServicePunishThreshold:      threshold,
			ServicePunishValidIntervalS: 2,
		}, cmcli)
	require.NoError(t, err)

	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"allocator-1", "allocator-2"})
	}

	// not punish
	for ii := threshold; ii > 1; ii-- {
		sc.PunishServiceWithThreshold(serviceCtx, serviceName, "allocator-1", 2)
	}
	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"allocator-1", "allocator-2"})
	}

	// punished
	sc.PunishServiceWithThreshold(serviceCtx, serviceName, "allocator-1", 2)
	for ii := 0; ii < 100; ii++ {
		host, err := sc.GetServiceHost(serviceCtx, serviceName)
		require.NoError(t, err)
		require.True(t, host == "allocator-2")
	}

	time.Sleep(time.Second * 2)
	{
		keys := make(hostSet)
		for ii := 0; ii < 100; ii++ {
			host, err := sc.GetServiceHost(serviceCtx, serviceName)
			require.NoError(t, err)
			keys[host] = struct{}{}
		}
		require.ElementsMatch(t, keys.Keys(), []string{"allocator-1", "allocator-2"})
	}
}

func TestAccessServiceGetDiskHost(t *testing.T) {
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{
			ClusterID: 0,
			IDC:       idc,
			ReloadSec: 1,
		}, cmcli)
	require.NoError(t, err)

	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
	}
	{
		_, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10000))
		require.Error(t, err)
	}
}

func TestAccessServicePunishDisk(t *testing.T) {
	sc, err := controller.NewServiceController(
		controller.ServiceConfig{
			ClusterID: 0,
			IDC:       idc,
			ReloadSec: 1,
		}, cmcli)
	require.NoError(t, err)

	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
		require.False(t, host.Punished)
	}
	sc.PunishDisk(serviceCtx, 10001, 1)
	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
		require.True(t, host.Punished)
	}
	time.Sleep(time.Second)
	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
		require.False(t, host.Punished)
	}
	sc.PunishDiskWithThreshold(serviceCtx, 10001, 1)
	{
		host, err := sc.GetDiskHost(serviceCtx, proto.DiskID(10001))
		require.NoError(t, err)
		require.True(t, host.Host == "blobnode-1")
		require.False(t, host.Punished)
	}
}
