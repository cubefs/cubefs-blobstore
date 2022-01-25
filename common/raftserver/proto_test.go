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

package raftserver

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

func TestProto(t *testing.T) {
	var mbs Members

	mbs.Mbs = append(mbs.Mbs, Member{1, "127.0.0.1:8081", false})
	mbs.Mbs = append(mbs.Mbs, Member{2, "127.0.0.1:8082", false})
	mbs.Mbs = append(mbs.Mbs, Member{3, "127.0.0.1:8083", true})

	cs := mbs.ConfState()
	assert.Equal(t, cs.Learners, []uint64{3})
	assert.Equal(t, cs.Voters, []uint64{1, 2})

	var ms raftMsgs
	datas := [][]byte{
		[]byte("daweuiriooidaohnr"),
		[]byte("bnufdaiurekjhitu"),
		[]byte("yiuewqojitruiewouio"),
		[]byte("eqopiwerhnewhioruthor"),
		[]byte("yuweqj;eoritrowiojopi"),
	}
	for i := 0; i < len(datas); i++ {
		ms = append(ms, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: datas[i]}}})
	}

	buffer := &bytes.Buffer{}
	_ = ms.Encode(buffer)

	var (
		nms raftMsgs
		err error
	)
	nms, err = nms.Decode(buffer)
	assert.Nil(t, err)
	assert.Equal(t, nms, ms)

	buffer.Reset()
	buffer.Write([]byte("123"))
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)
	buffer.Reset()

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 4)
	buffer.Write(b)
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)

	buffer.Reset()
	binary.BigEndian.PutUint32(b, 1) // write cnt
	buffer.Write(b)
	binary.BigEndian.PutUint32(b, 10) // write msg len
	buffer.Write(b)
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)

	buffer.Reset()
	binary.BigEndian.PutUint32(b, 1) // write cnt
	buffer.Write(b)
	binary.BigEndian.PutUint32(b, 10) // write msg len
	buffer.Write(b)
	buffer.Write([]byte("0123456789"))
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)

	msg := &pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("njdaiuerjkteia")}}}
	buffer.Reset()
	binary.BigEndian.PutUint32(b, 1) // write cnt
	buffer.Write(b)
	binary.BigEndian.PutUint32(b, uint32(msg.Size())) // write msg len
	buffer.Write(b)
	mdata, _ := msg.Marshal()
	buffer.Write(mdata)
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)

	buffer.Reset()
	binary.BigEndian.PutUint32(b, 1) // write cnt
	buffer.Write(b)
	binary.BigEndian.PutUint32(b, uint32(msg.Size())) // write msg len
	buffer.Write(b)
	buffer.Write(mdata)
	binary.BigEndian.PutUint32(b, 783287498) // write crc
	buffer.Write(b)
	_, err = nms.Decode(buffer)
	assert.NotNil(t, err)
}
