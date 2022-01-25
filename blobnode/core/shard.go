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

package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	bnapi "github.com/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/blobstore/common/proto"
)

// blob shard in chunk data
//
// chunk datafile format:
//  --------------
// | chunk  data  |
//  --------------
// |    shard    |		 ----------------------
// |    shard    |        |  header (32 Bytes) |
// |    shard    | ---->  |  data   (...)      |
// |    shard    |        |  footer (8 Bytes)  |
// |    ....    |        ----------------------
//
// header format:
// ---------------------
// |crc(header)(uint32)|
// |  magic  (uint32)  |
// |  bid    (int64)   |
// |  vuid   (uint64)  |
// |  size   (uint32)  |
// | padding (4 bytes) |
// ---------------------
//
// data format.
//  --------------
// | block        |   ---- 64 KiB
// | block        |   ---- 64 KiB
// | block        |   ---- 64 KiB
//  ---------------
//
// block format.
//  --------------
// | crc          |   ---- 4 Byte
// | data         |   ---- (64 KiB - 4)
//  ---------------
//
// footer format:
// ----------------------
// |  magic   (int32)   |
// | crc(shard) (int32) |
// | padding  (0 bytes) |
// ----------------------

const (
	// shard header size
	_shardHeaderSize = 4 + 4 + 8 + 8 + 4 + 4 // 32 (crc + magic + bid + vuid + size + reserved)
	// shard footer size
	_shardFooterSize = 4 + 4 // 8 (magic + checkSum)

	_shardCrcSize   = 4
	_shardMagicSize = 4
	_shardBidSize   = 8
	_shardVuidSize  = 8
	_shardSizeSize  = 4
	//_shardHeaderPaddingSize = _shardHeaderSize - _shardCrcSize - _shardMagicSize - _shardBidSize - _shardVuidSize - _shardSizeSize

	// shard header offset
	_shardCrcOffset      = 0
	_shardHdrMagicOffset = _shardCrcOffset + _shardCrcSize
	_shardBidOffset      = _shardHdrMagicOffset + _shardMagicSize
	_shardVuidOffset     = _shardBidOffset + _shardBidSize
	_shardSizeOffset     = _shardVuidOffset + _shardVuidSize
	_shardPaddingOffset  = _shardSizeOffset + _shardSizeSize
	//_shardDataOffset     = _shardPaddingOffset + _shardHeaderPaddingSize

	// footer offset
	_checksumSize = 4

	_shardFooterMagicOffset = 0
	_checksumOffset         = _shardFooterMagicOffset + _shardMagicSize
	_footerPaddingOffset    = _checksumOffset + _checksumSize
)

var (
	// magic number
	_shardHeaderMagic = [_shardMagicSize]byte{0xab, 0xcd, 0xef, 0xcc}
	_shardFooterMagic = [_shardMagicSize]byte{0xcc, 0xef, 0xcd, 0xab}
)

var (
	ErrShardHeaderMagic = errors.New("shard header magic")
	ErrShardHeaderSize  = errors.New("shard header size")
	ErrShardHeaderCrc   = errors.New("shard header crc not match")
	ErrShardFooterMagic = errors.New("shard footer magic")
	ErrShardFooterSize  = errors.New("shard footer size")
	ErrShardBufferSize  = errors.New("shard buffer size not match")
)

const (
	_ShardMetaSize = 32
)

// meta db key
type ShardKey struct {
	Chunk bnapi.ChunkId `json:"chunk"`
	Bid   proto.BlobID  `json:"bid"`
}

// meta db value
type ShardMeta struct {
	Version uint8             `json:"version"`
	Flag    bnapi.ShardStatus `json:"flag"`
	Offset  int64             `json:"offset"`
	Size    uint32            `json:"size"`
	Crc     uint32            `json:"crc"`
	Padding [8]byte           `json:"-"`
}

// Blob Shard in memory
type Shard struct {
	Bid  proto.BlobID // shard id
	Vuid proto.Vuid   // volume unit id

	Size   uint32            // size for shard
	Offset int64             // offset in data file. align when write
	Crc    uint32            // crc for shard data
	Flag   bnapi.ShardStatus // shard status

	Body     io.Reader // for put: shard body
	From, To int64     // for get: range (note: may fix in cs)
	Writer   io.Writer // for get: transmission to network

	PrepareHook func(shard *Shard)
	AfterHook   func(shard *Shard)
}

func (sm *ShardMeta) Marshal() ([]byte, error) {
	buf := make([]byte, _ShardMetaSize)

	buf[0] = sm.Version
	buf[1] = uint8(sm.Flag)

	binary.LittleEndian.PutUint64(buf[8:16], uint64(sm.Offset))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(sm.Size))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(sm.Crc))

	copy(buf[24:32], sm.Padding[:])

	return buf, nil
}

func (sm *ShardMeta) Unmarshal(data []byte) error {
	if len(data) != _ShardMetaSize {
		panic(ErrShardBufferSize)
	}

	sm.Version = data[0]
	sm.Flag = bnapi.ShardStatus(data[1])

	sm.Offset = int64(binary.LittleEndian.Uint64(data[8:16]))
	sm.Size = binary.LittleEndian.Uint32(data[16:20])
	sm.Crc = binary.LittleEndian.Uint32(data[20:24])

	copy(sm.Padding[:], data[24:32])

	return nil
}

func (b *Shard) WriterHeader(buf []byte) (err error) {
	if len(buf) != _shardHeaderSize {
		return ErrShardHeaderSize
	}

	// magic
	copy(buf[_shardHdrMagicOffset:_shardBidOffset], _shardHeaderMagic[:])
	// bid
	binary.BigEndian.PutUint64(buf[_shardBidOffset:_shardVuidOffset], uint64(b.Bid))
	// vuid
	binary.BigEndian.PutUint64(buf[_shardVuidOffset:_shardSizeOffset], uint64(b.Vuid))
	// size
	binary.BigEndian.PutUint32(buf[_shardSizeOffset:_shardPaddingOffset], uint32(b.Size))
	// write shard header crc
	headerCrc := crc32.ChecksumIEEE(buf[_shardHdrMagicOffset:])
	binary.BigEndian.PutUint32(buf[_shardCrcOffset:_shardHdrMagicOffset], headerCrc)

	return
}

func (b *Shard) WriterFooter(buf []byte) (err error) {
	if len(buf) != _shardFooterSize {
		return ErrShardFooterSize
	}

	// magic
	copy(buf[_shardFooterMagicOffset:_checksumOffset], _shardFooterMagic[:])
	// write shard data crc
	binary.BigEndian.PutUint32(buf[_checksumOffset:_footerPaddingOffset], b.Crc)

	return
}

func (b *Shard) ParseHeader(buf []byte) (err error) {
	if len(buf) != _shardHeaderSize {
		return ErrShardHeaderSize
	}

	headerMagic := buf[_shardHdrMagicOffset:_shardBidOffset]
	if !bytes.Equal(headerMagic, _shardHeaderMagic[:]) {
		return ErrShardHeaderMagic
	}
	b.Bid = proto.BlobID(binary.BigEndian.Uint64(buf[_shardBidOffset:_shardVuidOffset]))
	b.Vuid = proto.Vuid(binary.BigEndian.Uint64(buf[_shardVuidOffset:_shardSizeOffset]))
	b.Size = binary.BigEndian.Uint32(buf[_shardSizeOffset:_shardPaddingOffset])

	// shard header crc
	actualCrc := crc32.ChecksumIEEE(buf[_shardHdrMagicOffset:])
	expectCrc := binary.BigEndian.Uint32(buf[_shardCrcOffset:_shardHdrMagicOffset])
	if actualCrc != expectCrc {
		return ErrShardHeaderCrc
	}

	return
}

func (b *Shard) ParseFooter(buf []byte) (err error) {
	if len(buf) != _shardFooterSize {
		return ErrShardFooterSize
	}

	footerMagic := buf[_shardFooterMagicOffset:_checksumOffset]
	if !bytes.Equal(footerMagic, _shardFooterMagic[:]) {
		return ErrShardFooterMagic
	}
	b.Crc = binary.BigEndian.Uint32(buf[_checksumOffset:_footerPaddingOffset])

	return
}

func (b *Shard) String() string {
	return fmt.Sprintf(`
-----------------------------
---- head
Bid:            %d
Vuid:           %d
Flag:           %d
Size:           %d

---- foot
Crc:       %d
-----------------------------
`, b.Bid, b.Vuid, b.Flag, b.Size, b.Crc)
}

func (b *Shard) init() {
	b.Flag = bnapi.ShardStatusNormal
}

func (b *Shard) FillMeta(meta ShardMeta) {
	b.Offset = meta.Offset
	b.Size = meta.Size
	b.Crc = meta.Crc
	b.Flag = meta.Flag
}

// for write
func NewShardWriter(id proto.BlobID, vuid proto.Vuid, size uint32, body io.Reader) *Shard {
	s := new(Shard)

	s.Bid = id
	s.Vuid = vuid

	s.Size = size
	s.Body = body

	s.init()

	return s
}

// for read
func NewShardReader(id proto.BlobID, vuid proto.Vuid, from int64, to int64, writer io.Writer) *Shard {
	s := new(Shard)

	s.Bid = id
	s.Vuid = vuid

	s.From = from
	s.To = to
	s.Writer = writer

	s.init()

	return s
}

func ShardCopy(src *Shard) (dest *Shard) {
	dest = new(Shard)

	dest.Bid = src.Bid
	dest.Vuid = src.Vuid

	dest.Size = src.Size
	dest.Offset = src.Offset
	dest.Crc = src.Crc
	dest.Flag = src.Flag

	dest.Body = src.Body
	dest.From, dest.To = src.From, src.To
	dest.Writer = src.Writer

	dest.PrepareHook = src.PrepareHook
	dest.AfterHook = src.AfterHook

	return
}

func Alignphysize(shardSize int64) int64 {
	bodysize := crc32block.EncodeSize(shardSize, CrcBlockUnitSize)
	return _shardHeaderSize + int64(bodysize) + _shardFooterSize
}

func GetShardHeaderSize() int64 {
	return _shardHeaderSize
}

func GetShardFooterSize() int64 {
	return _shardFooterSize
}
