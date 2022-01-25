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

package access_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/blobstore/api/access"
	"github.com/cubefs/blobstore/common/crc32block"
	errcode "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/rpc"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/common/uptoken"
	"github.com/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/blobstore/util/log"
)

const (
	blobSize = 1 << 20
)

type dataCacheT struct {
	mu   sync.Mutex
	data map[proto.BlobID][]byte
}

func (c *dataCacheT) clean() {
	c.mu.Lock()
	c.data = make(map[proto.BlobID][]byte, len(c.data))
	c.mu.Unlock()
}

func (c *dataCacheT) put(bid proto.BlobID, b []byte) {
	c.mu.Lock()
	c.data[bid] = b
	c.mu.Unlock()
}

func (c *dataCacheT) get(bid proto.BlobID) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[bid]
}

var (
	mockServer *httptest.Server
	client     access.API
	dataCache  *dataCacheT
	services   []*api.ServiceEntry
	hostsApply []*api.ServiceEntry
	tokenAlloc = []byte("token")
	tokenPutat = []byte("token")

	partRandBroken = false
)

func init() {
	log.SetOutputLevel(log.Lfatal)

	t := time.Now()
	services = make([]*api.ServiceEntry, 0, 2)
	services = append(services, &api.ServiceEntry{
		Node: &api.Node{
			ID: "unreachable",
		},
		Service: &api.AgentService{
			Service: "access",
			Address: "127.0.0.1",
			Port:    9997,
		},
	})

	dataCache = &dataCacheT{}
	dataCache.clean()
	mockServer = httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/v1/health/service/access" {
				w.WriteHeader(http.StatusOK)
				b, _ := json.Marshal(hostsApply)
				w.Write(b)

			} else if req.URL.Path == "/put" {
				w.Header().Set(rpc.HeaderAckCrcEncoded, "1")

				// /put?size=1023&hashes=0
				putSize := req.URL.Query().Get("size")
				// just for testing timeout
				if strings.HasPrefix(putSize, "-") {
					time.Sleep(30 * time.Second)
					w.WriteHeader(http.StatusForbidden)
					return
				}

				size := req.Header.Get("Content-Length")
				l, _ := strconv.Atoi(size)

				w.WriteHeader(http.StatusOK)

				decoder := crc32block.NewBodyDecoder(req.Body)
				defer decoder.Close()
				buf := make([]byte, decoder.CodeSize(int64(l)))
				io.ReadFull(decoder, buf)
				dataCache.put(0, buf)

				hashesStr := req.URL.Query().Get("hashes")
				algsInt, _ := strconv.Atoi(hashesStr)
				algs := access.HashAlgorithm(algsInt)

				hashSumMap := algs.ToHashSumMap()
				for alg := range hashSumMap {
					hasher := alg.ToHasher()
					hasher.Write(buf)
					hashSumMap[alg] = hasher.Sum(nil)
				}

				loc := access.Location{Size: uint64(l)}
				fillCrc(&loc)
				resp := access.PutResp{
					Location:   loc,
					HashSumMap: hashSumMap,
				}
				b, _ := json.Marshal(resp)
				w.Write(b)

			} else if req.URL.Path == "/get" {
				var args access.GetArgs
				requestBody(req, &args)
				if !verifyCrc(&args.Location) {
					w.WriteHeader(http.StatusForbidden)
					return
				}

				w.Header().Set("Content-Length", strconv.Itoa(int(args.Location.Size)))
				w.WriteHeader(http.StatusOK)
				if buf := dataCache.get(0); len(buf) > 0 {
					w.Write(buf)
				} else {
					for _, blob := range args.Location.Spread() {
						w.Write(dataCache.get(blob.Bid))
					}
				}

			} else if req.URL.Path == "/alloc" {
				args := access.AllocArgs{}
				requestBody(req, &args)

				loc := access.Location{
					ClusterID: 1,
					Size:      args.Size,
					BlobSize:  blobSize,
					Blobs: []access.SliceInfo{
						{
							MinBid: proto.BlobID(mrand.Int()),
							Vid:    proto.Vid(mrand.Int()),
							Count:  uint32((args.Size + blobSize - 1) / blobSize),
						},
					},
				}
				// split to two blobs if large enough
				if loc.Blobs[0].Count > 2 {
					loc.Blobs[0].Count = 2
					loc.Blobs = append(loc.Blobs, []access.SliceInfo{
						{
							MinBid: proto.BlobID(mrand.Int()),
							Vid:    proto.Vid(mrand.Int()),
							Count:  uint32((args.Size - 2*blobSize + blobSize - 1) / blobSize),
						},
					}...)
				}

				// alloc the rest parts
				if args.AssignClusterID > 0 {
					loc.Blobs = []access.SliceInfo{
						{
							MinBid: proto.BlobID(mrand.Int()),
							Vid:    proto.Vid(mrand.Int()),
							Count:  uint32((args.Size + blobSize - 1) / blobSize),
						},
					}
				}

				tokens := make([]string, 0, len(loc.Blobs)+1)

				hasMultiBlobs := loc.Size >= uint64(loc.BlobSize)
				lastSize := uint32(loc.Size % uint64(loc.BlobSize))
				for idx, blob := range loc.Blobs {
					// returns one token if size < blobsize
					if hasMultiBlobs {
						count := blob.Count
						if idx == len(loc.Blobs)-1 && lastSize > 0 {
							count--
						}
						tokens = append(tokens, uptoken.EncodeToken(uptoken.NewUploadToken(loc.ClusterID,
							blob.Vid, blob.MinBid, count,
							loc.BlobSize, 0, tokenAlloc[:])))
					}

					// token of the last blob
					if idx == len(loc.Blobs)-1 && lastSize > 0 {
						tokens = append(tokens, uptoken.EncodeToken(uptoken.NewUploadToken(loc.ClusterID,
							blob.Vid, blob.MinBid+proto.BlobID(blob.Count)-1, 1,
							lastSize, 0, tokenAlloc[:])))
					}
				}

				fillCrc(&loc)
				resp := access.AllocResp{
					Location: loc,
					Tokens:   tokens,
				}

				w.WriteHeader(http.StatusOK)
				b, _ := json.Marshal(resp)
				w.Write(b)

			} else if req.URL.Path == "/putat" {
				w.Header().Set(rpc.HeaderAckCrcEncoded, "1")

				// /putat?clusterid=1&volumeid=1&blobid=1299313571767079875&size=1023&hashes=0&token=1111111
				query := req.URL.Query()
				bid, _ := strconv.Atoi(query.Get("blobid"))
				if partRandBroken && bid%3 == 0 { // random broken
					w.WriteHeader(http.StatusForbidden)
					return
				}
				cid, _ := strconv.Atoi(query.Get("clusterid"))
				vid, _ := strconv.Atoi(query.Get("volumeid"))
				sizeArg, _ := strconv.Atoi(query.Get("size"))
				token := uptoken.DecodeToken(query.Get("token"))
				if !token.IsValid(proto.ClusterID(cid), proto.Vid(vid), proto.BlobID(bid), uint32(sizeArg), tokenPutat[:]) {
					w.WriteHeader(http.StatusForbidden)
					return
				}

				size := req.Header.Get("Content-Length")
				l, _ := strconv.Atoi(size)

				w.WriteHeader(http.StatusOK)

				decoder := crc32block.NewBodyDecoder(req.Body)
				defer decoder.Close()
				blobBuf := make([]byte, decoder.CodeSize(int64(l)))
				io.ReadFull(decoder, blobBuf)

				hashesStr := query.Get("hashes")
				algsInt, _ := strconv.Atoi(hashesStr)
				algs := access.HashAlgorithm(algsInt)

				hashSumMap := algs.ToHashSumMap()
				for alg := range hashSumMap {
					hasher := alg.ToHasher()
					hasher.Write(blobBuf)
					hashSumMap[alg] = hasher.Sum(nil)
				}

				dataCache.put(proto.BlobID(bid), blobBuf)
				resp := access.PutAtResp{HashSumMap: hashSumMap}
				b, _ := json.Marshal(resp)
				w.Write(b)

			} else if req.URL.Path == "/delete" {
				args := access.DeleteArgs{}
				requestBody(req, &args)
				if !args.IsValid() {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				for _, loc := range args.Locations {
					if !verifyCrc(&loc) {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
				}

				if len(args.Locations) > 0 && len(args.Locations)%2 == 0 {
					locs := args.Locations[:]
					b, _ := json.Marshal(access.DeleteResp{FailedLocations: locs})
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Length", strconv.Itoa(len(b)))
					w.WriteHeader(http.StatusIMUsed)
					w.Write(b)
					return
				}

				b, _ := json.Marshal(access.DeleteResp{})
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", strconv.Itoa(len(b)))
				w.WriteHeader(http.StatusOK)
				w.Write(b)

			} else if req.URL.Path == "/sign" {
				args := access.SignArgs{}
				requestBody(req, &args)
				if err := signCrc(&args.Location, args.Locations); err != nil {
					w.WriteHeader(http.StatusForbidden)
					return
				}

				b, _ := json.Marshal(access.SignResp{Location: args.Location})
				w.WriteHeader(http.StatusOK)
				w.Write(b)

			} else {
				w.WriteHeader(http.StatusOK)
			}
		}))

	u := strings.Split(mockServer.URL[7:], ":")
	port, _ := strconv.Atoi(u[1])
	services = append(services, &api.ServiceEntry{
		Node: &api.Node{
			ID: "mockServer",
		},
		Service: &api.AgentService{
			Service: "access",
			Address: u[0],
			Port:    port,
		},
	})
	hostsApply = services

	cfg := access.Config{}
	cfg.Consul.Address = mockServer.URL[7:] // strip http://
	cfg.PriorityAddrs = []string{mockServer.URL}
	cfg.ConnMode = access.QuickConnMode
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	cli, err := access.New(cfg)
	if err != nil {
		panic(err)
	}
	client = cli

	mrand.Seed(int64(time.Since(t)))
}

func requestBody(req *http.Request, val interface{}) {
	l := req.Header.Get(rpc.HeaderContentLength)
	size, _ := strconv.Atoi(l)
	data := make([]byte, size)
	io.ReadFull(req.Body, data)
	json.Unmarshal(data, val)
}

func calcCrc(loc *access.Location) (uint32, error) {
	crcWriter := crc32.New(crc32.IEEETable)

	buf := bytespool.Alloc(1024)
	defer bytespool.Free(buf)

	n := loc.Encode2(buf)
	if n < 4 {
		return 0, fmt.Errorf("no enough bytes(%d) fill into buf", n)
	}

	if _, err := crcWriter.Write(buf[4:n]); err != nil {
		return 0, fmt.Errorf("fill crc %s", err.Error())
	}

	return crcWriter.Sum32(), nil
}

func fillCrc(loc *access.Location) error {
	crc, err := calcCrc(loc)
	if err != nil {
		return err
	}
	loc.Crc = crc
	return nil
}

func verifyCrc(loc *access.Location) bool {
	crc, err := calcCrc(loc)
	if err != nil {
		return false
	}
	return loc.Crc == crc
}

func signCrc(loc *access.Location, locs []access.Location) error {
	first := locs[0]
	bids := make(map[proto.BlobID]struct{}, 64)

	if loc.ClusterID != first.ClusterID ||
		loc.CodeMode != first.CodeMode ||
		loc.BlobSize != first.BlobSize {
		return fmt.Errorf("not equal in constant field")
	}

	for _, l := range locs {
		if !verifyCrc(&l) {
			return fmt.Errorf("not equal in crc %d", l.Crc)
		}

		// assert
		if l.ClusterID != first.ClusterID ||
			l.CodeMode != first.CodeMode ||
			l.BlobSize != first.BlobSize {
			return fmt.Errorf("not equal in constant field")
		}

		for _, blob := range l.Blobs {
			for c := 0; c < int(blob.Count); c++ {
				bids[blob.MinBid+proto.BlobID(c)] = struct{}{}
			}
		}
	}

	for _, blob := range loc.Blobs {
		for c := 0; c < int(blob.Count); c++ {
			bid := blob.MinBid + proto.BlobID(c)
			if _, ok := bids[bid]; !ok {
				return fmt.Errorf("not equal in blob_id(%d)", bid)
			}
		}
	}

	return fillCrc(loc)
}

type stringid struct{ id string }

func (s stringid) String() string { return s.id }

type traceid struct{ id string }

func (t traceid) TraceID() string { return t.id }

type requestid struct{ id string }

func (r requestid) RequestID() string { return r.id }

func randCtx() context.Context {
	ctx := context.Background()

	switch mrand.Int31() % 7 {
	case 0:
		return ctx
	case 1:
		return access.WithRequestID(ctx, nil)
	case 2:
		return access.WithRequestID(ctx, "TestAccessClient-string")
	case 3:
		return access.WithRequestID(ctx, stringid{"TestAccessClient-String"})
	case 4:
		return access.WithRequestID(ctx, traceid{"TestAccessClient-TraceID"})
	case 5:
		return access.WithRequestID(ctx, requestid{"TestAccessClient-RequestID"})
	case 6:
		return access.WithRequestID(ctx, struct{}{})
	default:
	}

	_, ctx = trace.StartSpanFromContext(ctx, "TestAccessClient")
	return ctx
}

func TestAccessClientConnectionMode(t *testing.T) {
	cases := []struct {
		mode access.RPCConnectMode
		size int64
	}{
		{0, 0},
		{0, -1},
		{access.QuickConnMode, 1 << 10},
		{access.GeneralConnMode, 1 << 10},
		{access.SlowConnMode, 1 << 10},
		{access.NoLimitConnMode, 1 << 10},
		{access.DefaultConnMode, 1 << 10},
		{100, 1 << 10},
		{0xff, 100},
	}

	for _, cs := range cases {
		cfg := access.Config{}
		cfg.Consul.Address = mockServer.URL[7:]
		cfg.MaxSizePutOnce = cs.size
		cfg.ConnMode = cs.mode
		cli, err := access.New(cfg)
		require.NoError(t, err)

		if cs.size <= 0 {
			continue
		}
		loc := access.Location{Size: uint64(mrand.Int63n(cs.size))}
		fillCrc(&loc)
		_, err = cli.Delete(randCtx(), &access.DeleteArgs{
			Locations: []access.Location{loc},
		})
		require.NoError(t, err)
	}
}

func TestAccessClientPutGet(t *testing.T) {
	cases := []struct {
		size int
	}{
		{0},
		{1},
		{1 << 6},
		{1 << 10},
		{1 << 19},
		{(1 << 19) + 1},
		{1 << 20},
	}
	for _, cs := range cases {
		buff := make([]byte, cs.size)
		rand.Read(buff)
		crcExpected := crc32.ChecksumIEEE(buff)
		args := access.PutArgs{
			Size:   int64(cs.size),
			Hashes: access.HashAlgCRC32,
			Body:   bytes.NewBuffer(buff),
		}

		loc, hashSumMap, err := client.Put(randCtx(), &args)
		crc, _ := hashSumMap.GetSum(access.HashAlgCRC32)
		require.NoError(t, err)
		require.Equal(t, crcExpected, crc)

		body, err := client.Get(randCtx(), &access.GetArgs{Location: loc, ReadSize: uint64(cs.size)})
		require.NoError(t, err)
		defer body.Close()

		io.ReadFull(body, buff)
		require.Equal(t, crcExpected, crc32.ChecksumIEEE(buff))
	}
}

func TestAccessClientPutAtBase(t *testing.T) {
	cfg := access.Config{}
	cfg.Consul.Address = mockServer.URL[7:]
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	client, err := access.New(cfg)
	require.NoError(t, err)

	cases := []struct {
		size int
	}{
		{0},
		{1},
		{1 << 10},
		{(1 << 19) + 1},
		{1 << 20},
		{1<<20 + 1023},
		{1<<23 + 1023},
		{1 << 24},
	}
	for _, cs := range cases {
		dataCache.clean()

		buff := make([]byte, cs.size)
		rand.Read(buff)
		crcExpected := crc32.ChecksumIEEE(buff)
		args := access.PutArgs{
			Size:   int64(cs.size),
			Hashes: access.HashAlgCRC32,
			Body:   bytes.NewBuffer(buff),
		}

		loc, hashSumMap, err := client.Put(randCtx(), &args)
		crc, _ := hashSumMap.GetSum(access.HashAlgCRC32)
		require.NoError(t, err)
		require.Equal(t, crcExpected, crc)

		body, err := client.Get(randCtx(), &access.GetArgs{Location: loc, ReadSize: uint64(cs.size)})
		require.NoError(t, err)
		defer body.Close()

		io.ReadFull(body, buff)
		require.Equal(t, crcExpected, crc32.ChecksumIEEE(buff))
	}
}

func TestAccessClientPutAtMerge(t *testing.T) {
	cfg := access.Config{}
	cfg.Consul.Address = mockServer.URL[7:]
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	client, err := access.New(cfg)
	require.NoError(t, err)

	partRandBroken = true
	defer func() {
		partRandBroken = false
	}()
	dataCache.clean()

	cases := []struct {
		size int
	}{
		{1<<21 + 77777},
		{1<<21 + 77777},
		{1<<21 + 77777},
		{1 << 22},
		{1 << 22},
		{1 << 22},
	}
	for _, cs := range cases {
		dataCache.clean()

		size := cs.size
		buff := make([]byte, size)
		rand.Read(buff)
		crcExpected := crc32.ChecksumIEEE(buff)
		args := access.PutArgs{
			Size:   int64(size),
			Hashes: access.HashAlgCRC32,
			Body:   bytes.NewBuffer(buff),
		}

		loc, hashSumMap, err := client.Put(randCtx(), &args)
		crc, _ := hashSumMap.GetSum(access.HashAlgCRC32)
		require.NoError(t, err)
		require.Equal(t, crcExpected, crc)

		body, err := client.Get(randCtx(), &access.GetArgs{Location: loc, ReadSize: uint64(size)})
		require.NoError(t, err)
		defer body.Close()

		io.ReadFull(body, buff)
		require.Equal(t, crcExpected, crc32.ChecksumIEEE(buff))
	}
}

func TestAccessClientPutMaxBlobsLength(t *testing.T) {
	cfg := access.Config{}
	cfg.Consul.Address = mockServer.URL[7:]
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	client, err := access.New(cfg)
	require.NoError(t, err)

	partRandBroken = true
	defer func() {
		partRandBroken = false
	}()

	cases := []struct {
		size int
		err  error
	}{
		{1 << 20, nil},
		{(1 << 21) + 1023, nil},
		{1 << 25, errcode.ErrUnexpected},
	}
	for _, cs := range cases {
		dataCache.clean()

		buff := make([]byte, cs.size)
		rand.Read(buff)
		args := access.PutArgs{
			Size:   int64(cs.size),
			Hashes: access.HashAlgDummy,
			Body:   bytes.NewBuffer(buff),
		}

		_, _, err := client.Put(randCtx(), &args)
		require.ErrorIs(t, cs.err, err)
	}
}

func linearTimeoutMs(baseSec, size, speedMBps float64) int64 {
	const alignMs = 500
	spentSec := size / 1024.0 / 1024.0 / speedMBps
	timeoutMs := int64((baseSec + spentSec) * 1000)
	if timeoutMs < 0 {
		timeoutMs = 0
	}
	if ms := timeoutMs % alignMs; ms > 0 {
		timeoutMs += (alignMs - ms)
	}
	return timeoutMs
}

func TestAccessClientPutTimeout(t *testing.T) {
	cfg := access.Config{}
	cfg.Consul.Address = mockServer.URL[7:]

	mb := int(1 << 20)
	ms := time.Millisecond

	cases := []struct {
		mode  access.RPCConnectMode
		size  int
		minMs time.Duration
		maxMs time.Duration
	}{
		// QuickConnMode 3s + size / 40, dial and response 2s
		{access.QuickConnMode, mb * -119, ms * 0, ms * 600},
		{access.QuickConnMode, mb * -100, ms * 500, ms * 1000},
		{access.QuickConnMode, mb * -80, ms * 1000, ms * 1500},
		{access.QuickConnMode, mb * -1, ms * 2000, ms * 2500},

		// DefaultConnMode 30s + size / 10, dial and response 5s
		{access.DefaultConnMode, mb * -299, ms * 0, ms * 600},
		{access.DefaultConnMode, mb * -280, ms * 2000, ms * 2500},
		{access.DefaultConnMode, mb * -270, ms * 3000, ms * 3500},
		{access.DefaultConnMode, mb * -1, ms * 5000, ms * 5500},
	}
	for _, cs := range cases {
		cfg.ConnMode = cs.mode
		switch cs.mode {
		case access.QuickConnMode:
			cfg.ClientTimeoutMs = linearTimeoutMs(3, float64(cs.size), 40)
		case access.DefaultConnMode:
			cfg.ClientTimeoutMs = linearTimeoutMs(30, float64(cs.size), 10)
		}
		client, err := access.New(cfg)
		require.NoError(t, err)

		buff := make([]byte, 0)
		args := access.PutArgs{
			Size: int64(cs.size),
			Body: bytes.NewBuffer(buff),
		}

		startTime := time.Now()
		_, _, err = client.Put(randCtx(), &args)
		require.Error(t, err)
		duration := time.Since(startTime)
		require.GreaterOrEqual(t, cs.maxMs, duration, "greater duration: ", duration)
		require.LessOrEqual(t, cs.minMs, duration, "less duration: ", duration)
	}
}

func TestAccessClientDelete(t *testing.T) {
	{
		locs, err := client.Delete(randCtx(), nil)
		require.Nil(t, locs)
		require.ErrorIs(t, errcode.ErrIllegalArguments, err)
	}
	{
		locs, err := client.Delete(randCtx(), &access.DeleteArgs{})
		require.Nil(t, locs)
		require.ErrorIs(t, errcode.ErrIllegalArguments, err)
	}
	{
		locs, err := client.Delete(randCtx(), &access.DeleteArgs{
			Locations: make([]access.Location, 1),
		})
		require.Nil(t, locs)
		require.NoError(t, err)
	}
	{
		args := &access.DeleteArgs{
			Locations: make([]access.Location, 1000),
		}
		_, err := client.Delete(randCtx(), args)
		require.NoError(t, err)
	}
	{
		args := &access.DeleteArgs{
			Locations: make([]access.Location, access.MaxDeleteLocations+1),
		}
		locs, err := client.Delete(randCtx(), args)
		require.Equal(t, args.Locations, locs)
		require.ErrorIs(t, errcode.ErrIllegalArguments, err)
	}
	{
		loc := access.Location{Size: 100, Blobs: make([]access.SliceInfo, 0)}
		fillCrc(&loc)
		args := &access.DeleteArgs{
			Locations: make([]access.Location, 0, access.MaxDeleteLocations),
		}
		for i := 1; i < access.MaxDeleteLocations/10; i++ {
			args.Locations = append(args.Locations, loc)
			locs, err := client.Delete(randCtx(), args)
			if i%2 == 0 {
				require.Equal(t, args.Locations, locs)
				require.Equal(t, errcode.ErrUnexpected, err)
			} else {
				require.NoError(t, err)
			}
		}
	}
}

func TestAccessClientRequestBody(t *testing.T) {
	cfg := access.Config{}
	cfg.Consul.Address = mockServer.URL[7:]
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	cfg.PriorityAddrs = []string{mockServer.URL}
	client, err := access.New(cfg)
	require.NoError(t, err)

	cases := []struct {
		size int
	}{
		{0},
		{1},
		{1 << 10},
		{(1 << 19) + 1},
		{1 << 20},
		{1<<20 + 1023},
		{1<<23 + 1023},
		{1 << 24},
	}
	for _, cs := range cases {
		dataCache.clean()

		buff := make([]byte, cs.size)
		rand.Read(buff)
		args := access.PutArgs{
			Size: int64(cs.size) + 1,
			Body: bytes.NewBuffer(buff),
		}

		_, _, err := client.Put(randCtx(), &args)
		if cs.size < int(cfg.MaxSizePutOnce) {
			if cs.size <= 1<<22 {
				require.Equal(t, errcode.ErrAccessReadRequestBody, err)
			} else {
				require.Equal(t, errcode.ErrAccessReadConflictBody, err)
			}
		} else {
			require.Equal(t, errcode.ErrAccessReadRequestBody, err)
		}
	}
}

func TestAccessClientPutAtToken(t *testing.T) {
	tokenKey := tokenPutat
	defer func() {
		tokenPutat = tokenKey
	}()

	cfg := access.Config{}
	cfg.Consul.Address = mockServer.URL[7:]
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 1
	cfg.MaxPartRetry = -1
	client, err := access.New(cfg)
	require.NoError(t, err)

	cases := []struct {
		keyLen int
	}{
		{0},
		{1},
		{20},
		{100},
	}
	buff := make([]byte, 1<<21)
	rand.Read(buff)
	for _, cs := range cases {
		tokenPutat = make([]byte, cs.keyLen)
		rand.Read(tokenPutat)
		args := access.PutArgs{
			Size: int64(1 << 21),
			Body: bytes.NewBuffer(buff),
		}
		_, _, err := client.Put(randCtx(), &args)
		require.ErrorIs(t, errcode.ErrUnexpected, err)
	}
}

func TestAccessClientRPCConfig(t *testing.T) {
	cfg := access.Config{
		RPCConfig: &rpc.Config{},
	}
	cfg.Consul.Address = mockServer.URL[7:]
	cfg.PriorityAddrs = []string{mockServer.URL}
	client, err := access.New(cfg)
	require.NoError(t, err)
	cases := []struct {
		size int
	}{
		{0},
		{1},
		{1 << 6},
		{1 << 10},
	}
	for _, cs := range cases {
		buff := make([]byte, cs.size)
		rand.Read(buff)
		args := access.PutArgs{
			Size: int64(cs.size),
			Body: bytes.NewBuffer(buff),
		}
		loc, _, err := client.Put(randCtx(), &args)
		require.NoError(t, err)
		_, err = client.Get(randCtx(), &access.GetArgs{Location: loc, ReadSize: uint64(cs.size)})
		require.NoError(t, err)
	}
}

func TestAccessClientLogger(t *testing.T) {
	file, err := ioutil.TempFile(os.TempDir(), "TestAccessClientLogger")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	defer func() {
		os.Remove(file.Name())
	}()

	cfg := access.Config{
		Logger: &access.Logger{
			Filename: file.Name(),
		},
	}
	cfg.Consul.Address = mockServer.URL[7:]
	client, err := access.New(cfg)
	require.NoError(t, err)
	defer func() {
		cfg.Logger.Close()
	}()

	size := 1024
	args := access.PutArgs{
		Size: int64(size),
		Body: bytes.NewBuffer(make([]byte, size-1)),
	}
	_, _, err = client.Put(randCtx(), &args)
	require.Error(t, err)
}
