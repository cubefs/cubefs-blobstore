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

package access

import (
	"context"
	"io"
	"time"

	"github.com/cubefs/blobstore/api/access"
	"github.com/cubefs/blobstore/common/ec"
	errcode "github.com/cubefs/blobstore/common/errors"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
)

// PutAt access interface /putat, put one blob
//     required: rc file reader
//     required: clusterID VolumeID BlobID
//     required: size, one blob size
//     optional: hasherMap, computing hash
func (h *Handler) PutAt(ctx context.Context, rc io.Reader,
	clusterID proto.ClusterID, vid proto.Vid, bid proto.BlobID, size int64,
	hasherMap access.HasherMap) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("putat request cluster:%d vid:%d bid:%d size:%d hashes:b(%b)",
		clusterID, vid, bid, size, hasherMap.ToHashAlgorithm())

	if len(hasherMap) > 0 {
		rc = io.TeeReader(rc, hasherMap.ToWriter())
	}

	volume, err := h.getVolume(ctx, clusterID, vid, true)
	if err != nil {
		return err
	}

	readSize := int(size)
	tactic := volume.CodeMode.Tactic()
	st := time.Now()
	buffer, err := ec.NewBuffer(readSize, tactic, h.memPool)
	if dur := time.Since(st); dur > time.Millisecond {
		span.Debug("new ec buffer", dur)
	}
	if err != nil {
		return err
	}
	defer buffer.Release()

	shards, err := h.encoder[volume.CodeMode].Split(buffer.ECDataBuf)
	if err != nil {
		return err
	}

	putTime := new(timeReadWrite)
	defer func() {
		span.AppendRPCTrackLog([]string{putTime.String()})
	}()

	startRead := time.Now()
	n, err := io.ReadFull(rc, buffer.DataBuf)
	putTime.IncR(time.Since(startRead))
	if err != nil && err != io.EOF {
		span.Info("read blob data from request body", err)
		return errcode.ErrAccessReadRequestBody
	}
	if n != readSize {
		span.Infof("want to read %d, but %d", readSize, n)
		return errcode.ErrAccessReadRequestBody
	}

	if err = h.encoder[volume.CodeMode].Encode(shards); err != nil {
		return err
	}

	blobident := blobIdent{clusterID, vid, bid}
	span.Debug("to write", blobident)

	startWrite := time.Now()
	badIdx, err := h.writeToBlobnodesWithHystrix(ctx, blobident, shards)
	putTime.IncW(time.Since(startWrite))
	if err != nil {
		return err
	}
	if len(badIdx) > 0 {
		h.sendRepairMsgBg(ctx, clusterID, vid, bid, badIdx)
	}

	span.Debugf("putat done cluster:%d vid:%d bid:%d size:%d", clusterID, vid, bid, size)
	return nil
}
