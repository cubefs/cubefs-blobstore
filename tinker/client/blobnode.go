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

	"github.com/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/blobstore/common/proto"
)

// BlobNodeAPI define the interface of blobnode use by delete
type BlobNodeAPI interface {
	MarkDelete(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) error
	Delete(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) error
}

// BlobNodeClient blobnode client
type BlobNodeClient struct {
	client blobnode.StorageAPI
}

// NewBlobNodeClient returns blobnode client
func NewBlobNodeClient(cfg *blobnode.Config) BlobNodeAPI {
	return &BlobNodeClient{blobnode.New(cfg)}
}

// MarkDelete mark delete blob
func (c *BlobNodeClient) MarkDelete(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) error {
	return c.client.MarkDeleteShard(ctx, location.Host, &blobnode.DeleteShardArgs{
		DiskID: location.DiskID,
		Vuid:   location.Vuid,
		Bid:    bid,
	})
}

// Delete delete blob
func (c *BlobNodeClient) Delete(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) error {
	return c.client.DeleteShard(ctx, location.Host, &blobnode.DeleteShardArgs{
		DiskID: location.DiskID,
		Vuid:   location.Vuid,
		Bid:    bid,
	})
}
