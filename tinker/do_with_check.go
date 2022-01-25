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

package tinker

import (
	"context"

	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
	"github.com/cubefs/blobstore/tinker/base"
	"github.com/cubefs/blobstore/tinker/client"
	"github.com/cubefs/blobstore/util/errors"
)

var errVolInfoNotEqual = errors.New("volInfo not equal")

// DoWithCheckVolConsistency the moment of scheduler volume update mapping relation and notice tinker
// some task(delet & shard repair)may be has start running use old volume mapping relation info
// if delete use old relation will can not delete shard in new chunk ==> rubbish shard
// if shard repair use old relation will can not repair shard in new chunk ==> miss shard repair
// to resolve this problem,tinker will equal volume info before task execute and after
// if not equal retry execute task again
func DoWithCheckVolConsistency(ctx context.Context, volCache base.IVolumeCache, vid proto.Vid, execTask func(volInfo *client.VolInfo) error) error {
	span := trace.SpanFromContextSafe(ctx)

	volInfo, err := volCache.Get(vid)
	if err != nil {
		span.Errorf("get volume cache failed: volInfo[%+v], err[%+v]", volInfo, err)
		return err
	}
	for try := 0; try < 3; try++ {
		err := execTask(volInfo)
		if err != nil {
			return err
		}

		// query volume info and check equal after executing task
		newVolInfo, err := volCache.Get(volInfo.Vid)
		if err != nil {
			return err
		}
		if newVolInfo.EqualWith(*volInfo) {
			return nil
		}

		span.Warnf("volInfo check not consist: old volInfo[%+v], new volInfo[%+v]", volInfo, newVolInfo)
		volInfo = newVolInfo
	}
	return errVolInfoNotEqual
}
