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

package mqproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cubefs/blobstore/api/mqproxy"
	"github.com/cubefs/blobstore/common/kafka"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/common/trace"
)

// BlobDeleteHandler stream http handler
type BlobDeleteHandler interface {
	SendDeleteMsg(ctx context.Context, info *mqproxy.DeleteArgs) error
}

// Producer is used to send messages to kafka
type Producer interface {
	kafka.MsgProducer
}

// BlobDeleteConfig is blob delete config
type BlobDeleteConfig struct {
	Topic        string            `json:"topic"`
	MsgSenderCfg kafka.ProducerCfg `json:"msg_sender_cfg"`
}

// BlobDeleteMgr is blob delete manager
type BlobDeleteMgr struct {
	topic        string
	delMsgSender Producer
}

// NewBlobDeleteMgr returns blob delete manager to handle delete message
func NewBlobDeleteMgr(cfg BlobDeleteConfig) (*BlobDeleteMgr, error) {
	delMsgSender, err := kafka.NewProducer(&cfg.MsgSenderCfg)
	if err != nil {
		return nil, err
	}

	blobDeleteMgr := BlobDeleteMgr{
		topic:        cfg.Topic,
		delMsgSender: delMsgSender,
	}

	return &blobDeleteMgr, nil
}

// SendDeleteMsg sends delete message to kafka
func (d *BlobDeleteMgr) SendDeleteMsg(ctx context.Context, info *mqproxy.DeleteArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	msgs := make([][]byte, 0, len(info.Blobs))
	for _, blobInfo := range info.Blobs {
		msg := proto.DeleteMsg{
			ClusterID: info.ClusterID,
			Vid:       blobInfo.Vid,
			Bid:       blobInfo.Bid,
			Time:      time.Now().Unix(),
			ReqId:     span.TraceID(),
		}

		msgByte, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal message: mgs [%+v], err:[%w]", msg, err)
		}
		msgs = append(msgs, msgByte)
	}

	now := time.Now()
	err := d.delMsgSender.SendMessages(d.topic, msgs)
	if err != nil {
		return fmt.Errorf("send delete messages: topic[%s], info[%+v], err[%w]", d.topic, info, err)
	}

	span.Debugf("send delete messages success: topic[%s], info[%+v], spend[%+v(100ns)]", d.topic, info, int64(time.Since(now)/100))
	return nil
}
