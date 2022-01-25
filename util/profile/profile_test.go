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

package profile

import (
	"expvar"
	"io/ioutil"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileBase(t *testing.T) {
	Handle("/test/handle", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(255)
	}))
	HandleFunc("/test/handlefunc", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(277)
	})

	i := expvar.NewInt("int_key")
	i.Set(100)

	var defaultHandleAddr string
	initDone := make(chan bool)
	go func() {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defaultHandleAddr = "http://" + ln.Addr().String()
		close(initDone)
		err = http.Serve(ln, nil)
		require.NoError(t, err)
	}()
	<-initDone

	// default handle 404
	resp, err := http.Get(defaultHandleAddr + "/debug/var/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 404, resp.StatusCode)
	resp, err = http.Get(defaultHandleAddr + "/metrics")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 404, resp.StatusCode)

	if without {
		require.Equal(t, "", profileAddr)
		require.Panics(t, func() {
			ListenOn()
		})
		return
	}

	// profile handle 200
	resp, err = http.Get(profileAddr + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	buf := make([]byte, 1<<20)
	n, _ := resp.Body.Read(buf)
	t.Log(string(buf[:n]))

	resp, err = http.Get(profileAddr + "/debug/vars")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	resp, err = http.Get(profileAddr + "/debug/pprof/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	// handle of defined
	resp, err = http.Get(profileAddr + "/test/handle")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 255, resp.StatusCode)
	resp, err = http.Get(profileAddr + "/test/handlefunc")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 277, resp.StatusCode)

	// get one expvar
	resp, err = http.Get(profileAddr + "/debug/var/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 400, resp.StatusCode)
	resp, err = http.Get(profileAddr + "/debug/var/not_exist_key")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 404, resp.StatusCode)
	resp, err = http.Get(profileAddr + "/debug/var/int_key")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	data, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "100", string(data))

	require.Equal(t, listenAddr, ListenOn())
	{
		genListenAddr()
		genDumpScript()
		genMetricExporter()
	}
}
