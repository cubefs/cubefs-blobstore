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

package base

import "github.com/cubefs/blobstore/common/kafka"

// IProducer define the interface of producer
type IProducer interface {
	SendMessage(msg []byte) (err error)
	SendMessages(msgs [][]byte) (err error)
}

type msgSenderEx struct {
	topic    string
	producer kafka.MsgProducer
}

// NewMsgSenderEx returns message sender
func NewMsgSenderEx(topic string, cfg *kafka.ProducerCfg) (IProducer, error) {
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}
	return &msgSenderEx{topic: topic, producer: producer}, nil
}

// SendMessage send message to mq
func (sender *msgSenderEx) SendMessage(msg []byte) (err error) {
	return sender.producer.SendMessage(sender.topic, msg)
}

// SendMessages send message batch
func (sender *msgSenderEx) SendMessages(msgs [][]byte) error {
	return sender.producer.SendMessages(sender.topic, msgs)
}
