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
	"bufio"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	localhost = "127.0.0.1"

	minPort = 30000
	maxPort = 31000

	envBindAddr             = "profile_bind_addr"
	envMetricExporterLabels = "profile_metric_exporter_labels"

	suffixListenAddr     = "_profile_listen_addr"
	suffixMetricExporter = "_profile_metric_exporter.yaml"
	suffixDumpScript     = "_profile_dump.sh"
)

var (
	serveMux    *http.ServeMux
	profileAddr string
	listenAddr  string
	startTime   = time.Now()

	uptime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "profile",
		Subsystem: "process",
		Name:      "uptime_seconds",
		Help:      "Process uptime seconds.",
	}, func() (v float64) {
		return time.Since(startTime).Seconds()
	})

	gomaxprocs = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "profile",
		Subsystem: "process",
		Name:      "maxprocs",
		Help:      "Go runtime.GOMAXPROCS.",
	}, func() (v float64) {
		return float64(runtime.GOMAXPROCS(-1))
	})

	router = new(route)
)

type route struct {
	mu sync.Mutex

	vars    []string
	pprof   []string
	metrics []string
	users   []string
}

func (r *route) AddUserPath(path string) {
	r.mu.Lock()
	r.users = append(r.users, path)
	sort.Strings(r.users)
	r.mu.Unlock()
}

func (r *route) String() string {
	r.mu.Lock()
	sb := strings.Builder{}
	sb.WriteString("usage:\n\t/\n\n")
	sb.WriteString("vars:\n")
	sb.WriteString(fmt.Sprintf("\t%s\n\n", strings.Join(r.vars, "\n\t")))
	sb.WriteString("pprof:\n")
	sb.WriteString(fmt.Sprintf("\t%s\n\n", strings.Join(r.pprof, "\n\t")))
	sb.WriteString("metrics:\n")
	sb.WriteString(fmt.Sprintf("\t%s\n\n", strings.Join(r.metrics, "\n\t")))
	if len(r.users) > 0 {
		sb.WriteString("users:\n")
		sb.WriteString(fmt.Sprintf("\t%s\n\n", strings.Join(r.users, "\n\t")))
	}
	r.mu.Unlock()
	return sb.String()
}

func init() {
	prometheus.MustRegister(uptime)
	prometheus.MustRegister(gomaxprocs)

	expvar.NewString("GoVersion").Set(runtime.Version())
	expvar.NewInt("NumCPU").Set(int64(runtime.NumCPU()))
	expvar.NewInt("Pid").Set(int64(os.Getpid()))
	expvar.NewInt("StartTime").Set(startTime.Unix())

	expvar.Publish("Uptime", expvar.Func(func() interface{} { return time.Since(startTime) / time.Second }))
	expvar.Publish("NumGoroutine", expvar.Func(func() interface{} { return runtime.NumGoroutine() }))

	serveMux = http.NewServeMux()
	serveMux.HandleFunc("/", index)
	serveMux.HandleFunc("/debug/pprof/", pprof.Index)
	serveMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serveMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serveMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serveMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	serveMux.HandleFunc("/debug/var/", oneExpvar)
	serveMux.Handle("/debug/vars", expvar.Handler())
	serveMux.Handle("/metrics", promhttp.Handler())

	router.mu.Lock()
	router.vars = []string{
		"/debug/vars",
		"/debug/var/",
	}
	router.pprof = []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
		"/debug/pprof/trace",
	}
	router.metrics = []string{
		"/metrics",
	}
	router.mu.Unlock()

	// without profile setting
	if without {
		return
	}

	ln, err := tryListen()
	if err != nil {
		log.Panic("profile listen failed", err)
		return
	}

	listenAddr = ln.Addr().String()
	profileAddr = "http://" + ln.Addr().String()

	// do not gen files in `go test`
	if !strings.HasSuffix(os.Args[0], ".test") {
		genListenAddr()
		genDumpScript()
		genMetricExporter()
	}

	go func() {
		if err := http.Serve(ln, serveMux); err != nil {
			log.Panic(err)
		}
	}()
}

// HandleFunc register handler func to profile serve multiplexer.
func HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	router.AddUserPath(pattern)
	serveMux.HandleFunc(pattern, handler)
}

// Handle register handler to profile serve multiplexer.
func Handle(pattern string, handler http.Handler) {
	router.AddUserPath(pattern)
	serveMux.Handle(pattern, handler)
}

// ListenOn returns address of profile listen on, like host:port
func ListenOn() string {
	if listenAddr == "" {
		panic("profile did not listened")
	}
	return listenAddr
}

// handle path /, show usage
func index(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, router.String())
}

// handler path /debug/var/<var>, get one expvar
func oneExpvar(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/debug/var/"):]
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	val := expvar.Get(key)
	if val == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Write([]byte(val.String()))
}

func try2GetAddr() string {
	fd, err := os.Open(os.Args[0] + suffixListenAddr)
	if err != nil {
		return ""
	}
	defer fd.Close()

	if line, _, err := bufio.NewReader(fd).ReadLine(); err == nil {
		return string(line)
	}
	return ""
}

func tryListen() (ln net.Listener, err error) {
	if addr := os.Getenv(envBindAddr); addr != "" {
		if ln, err = net.Listen("tcp4", addr); err == nil {
			return
		}
	}
	if addr := try2GetAddr(); addr != "" {
		if ln, err = net.Listen("tcp4", addr); err == nil {
			return
		}
	}

	for port := minPort; port < maxPort; port++ {
		if ln, err = net.Listen("tcp4", localhost+":"+strconv.Itoa(port)); err == nil {
			return
		}
	}
	return nil, errors.New("cannot bind tcp4 address")
}
