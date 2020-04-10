/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"gitee.com/gbat/thrift/lib/go/thrift/metadata2"
	"io"
	"log"
	"math"
	"strings"
	"time"
)

type TBinaryProtocol struct {
	trans         TRichTransport
	origTransport TTransport
	reader        io.Reader
	writer        io.Writer
	strictRead    bool
	strictWrite   bool
	buffer        [64]byte
}

type TBinaryProtocolFactory struct {
	strictRead  bool
	strictWrite bool
}

func NewTBinaryProtocolTransport(t TTransport) *TBinaryProtocol {
	return NewTBinaryProtocol(t, false, true)
}

func NewTBinaryProtocol(t TTransport, strictRead, strictWrite bool) *TBinaryProtocol {
	p := &TBinaryProtocol{origTransport: t, strictRead: strictRead, strictWrite: strictWrite}
	if et, ok := t.(TRichTransport); ok {
		p.trans = et
	} else {
		p.trans = NewTRichTransport(t)
	}
	p.reader = p.trans
	p.writer = p.trans
	return p
}

func NewTBinaryProtocolFactoryDefault() *TBinaryProtocolFactory {
	return NewTBinaryProtocolFactory(false, true)
}

func NewTBinaryProtocolFactory(strictRead, strictWrite bool) *TBinaryProtocolFactory {
	return &TBinaryProtocolFactory{strictRead: strictRead, strictWrite: strictWrite}
}

func (p *TBinaryProtocolFactory) GetProtocol(t TTransport) TProtocol {
	return NewTBinaryProtocol(t, p.strictRead, p.strictWrite)
}

/**
 * Writing Methods
 */
func (p *TBinaryProtocol) WriteRichMessageBegin(name string, typeId TMessageType, seqId int32, c context.Context) error {
	if p.strictWrite {
		version := uint32(VERSION_1) | uint32(typeId)
		e := p.WriteI32(int32(version))
		if e != nil {
			return e
		}
		md, ok := metadata2.FromOutgoingContext(c)
		if !ok {
			md = metadata2.New(nil)
		} else {
			md = md.Copy()
		}
		traceStr := ""
		if traces := md["uber-trace-id"]; traces != nil {
			traceStr = strings.Join(traces, "|")
		}
		if traceStr != "" {
			name = name + "@" + traceStr
		}
		e = p.WriteString(name)
		if e != nil {
			return e
		}
		e = p.WriteI32(seqId)
		//add context
		//if e != nil {
		//	return e
		//}
		//var buf bytes.Buffer
		//enc := gob.NewEncoder(&buf)
		//md, ok := metadata2.FromOutgoingContext(c)
		//if !ok {
		//	md = metadata2.New(nil)
		//} else {
		//	md = md.Copy()
		//}
		//if err := enc.Encode(md); err != nil {
		//	log.Fatal("encode error:", err)
		//}
		//
		//e=p.WriteBinary(buf.Bytes())
		return e
	} else {
		e := p.WriteString(name)
		if e != nil {
			return e
		}
		e = p.WriteByte(int8(typeId))
		if e != nil {
			return e
		}
		e = p.WriteI32(seqId)
		return e
	}
	return nil
}

func (p *TBinaryProtocol) WriteMessageBegin(name string, typeId TMessageType, seqId int32) error {
	if p.strictWrite {
		version := uint32(VERSION_1) | uint32(typeId)
		e := p.WriteI32(int32(version))
		if e != nil {
			return e
		}
		e = p.WriteString(name)
		if e != nil {
			return e
		}
		e = p.WriteI32(seqId)
		return e
	} else {
		e := p.WriteString(name)
		if e != nil {
			return e
		}
		e = p.WriteByte(int8(typeId))
		if e != nil {
			return e
		}
		e = p.WriteI32(seqId)
		return e
	}
	return nil
}

func (p *TBinaryProtocol) WriteMessageEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteStructBegin(name string) error {
	return nil
}

func (p *TBinaryProtocol) WriteStructEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteFieldBegin(name string, typeId TType, id int16) error {
	e := p.WriteByte(int8(typeId))
	if e != nil {
		return e
	}
	e = p.WriteI16(id)
	return e
}

func (p *TBinaryProtocol) WriteFieldEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteFieldStop() error {
	e := p.WriteByte(STOP)
	return e
}

func (p *TBinaryProtocol) WriteMapBegin(keyType TType, valueType TType, size int) error {
	e := p.WriteByte(int8(keyType))
	if e != nil {
		return e
	}
	e = p.WriteByte(int8(valueType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TBinaryProtocol) WriteMapEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteListBegin(elemType TType, size int) error {
	e := p.WriteByte(int8(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TBinaryProtocol) WriteListEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteSetBegin(elemType TType, size int) error {
	e := p.WriteByte(int8(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TBinaryProtocol) WriteSetEnd() error {
	return nil
}

func (p *TBinaryProtocol) WriteBool(value bool) error {
	if value {
		return p.WriteByte(1)
	}
	return p.WriteByte(0)
}

func (p *TBinaryProtocol) WriteByte(value int8) error {
	e := p.trans.WriteByte(byte(value))
	return NewTProtocolException(e)
}

func (p *TBinaryProtocol) WriteI16(value int16) error {
	v := p.buffer[0:2]
	binary.BigEndian.PutUint16(v, uint16(value))
	_, e := p.writer.Write(v)
	return NewTProtocolException(e)
}

func (p *TBinaryProtocol) WriteI32(value int32) error {
	v := p.buffer[0:4]
	binary.BigEndian.PutUint32(v, uint32(value))
	_, e := p.writer.Write(v)
	return NewTProtocolException(e)
}

func (p *TBinaryProtocol) WriteI64(value int64) error {
	v := p.buffer[0:8]
	binary.BigEndian.PutUint64(v, uint64(value))
	_, err := p.writer.Write(v)
	return NewTProtocolException(err)
}

func (p *TBinaryProtocol) WriteDouble(value float64) error {
	return p.WriteI64(int64(math.Float64bits(value)))
}

func (p *TBinaryProtocol) WriteString(value string) error {
	e := p.WriteI32(int32(len(value)))
	if e != nil {
		return e
	}
	_, err := p.trans.WriteString(value)
	return NewTProtocolException(err)
}

func (p *TBinaryProtocol) WriteBinary(value []byte) error {
	e := p.WriteI32(int32(len(value)))
	if e != nil {
		return e
	}
	_, err := p.writer.Write(value)
	return NewTProtocolException(err)
}

/**
 * Reading methods
 */

func (p *TBinaryProtocol) ReadRichMessageBegin() (name string, typeId TMessageType, seqId int32, c context.Context, err error) {
	size, e := p.ReadI32()
	if e != nil {
		return "", typeId, 0, nil, NewTProtocolException(e)
	}
	if size < 0 {
		typeId = TMessageType(size & 0x0ff)
		version := int64(int64(size) & VERSION_MASK)
		if version != VERSION_1 {
			return name, typeId, seqId, nil, NewTProtocolExceptionWithType(BAD_VERSION, fmt.Errorf("Bad version in ReadMessageBegin"))
		}
		name, e = p.ReadString()
		nameStr := strings.Split(name, "@")
		name = nameStr[0]
		traces := make([]string, 0)
		if len(nameStr) > 1 {
			traces = strings.Split(nameStr[1], "|")
		}
		if e != nil {
			return name, typeId, seqId, nil, NewTProtocolException(e)
		}
		seqId, e = p.ReadI32()
		if e != nil {
			return name, typeId, seqId, nil, NewTProtocolException(e)
		}

		//var buf bytes.Buffer
		md := metadata2.MD{}
		//b,e:=p.ReadBinary()
		//if e != nil {
		//	return name, typeId, seqId,nil, NewTProtocolException(e)
		//}
		//_,e=buf.Write(b)
		//if e != nil {
		//	return name, typeId, seqId,nil, NewTProtocolException(e)
		//}
		//dec := gob.NewDecoder(&buf)
		//if err := dec.Decode(&md); err != nil {
		//	log.Fatal("decode error:", err)
		//}
		for i := 0; i < len(traces); i++ {
			md.Set("uber-trace-id", traces[i])
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500) //500
		defer cancel()
		c = metadata2.NewOutgoingContext(ctx, md)
		return name, typeId, seqId, c, nil
	}
	if p.strictRead {
		return name, typeId, seqId, nil, NewTProtocolExceptionWithType(BAD_VERSION, fmt.Errorf("Missing version in ReadMessageBegin"))
	}
	name, e2 := p.readStringBody(size)
	if e2 != nil {
		return name, typeId, seqId, nil, e2
	}
	b, e3 := p.ReadByte()
	if e3 != nil {
		return name, typeId, seqId, nil, e3
	}
	typeId = TMessageType(b)
	seqId, e4 := p.ReadI32()
	if e4 != nil {
		return name, typeId, seqId, nil, e4
	}

	var buf bytes.Buffer
	var md metadata2.MD
	by, e5 := p.ReadBinary()
	if e5 != nil {
		return name, typeId, seqId, nil, e5
	}
	_, e5 = buf.Write(by)
	if e5 != nil {
		return name, typeId, seqId, nil, e5
	}
	dec := gob.NewDecoder(&buf)
	if err := dec.Decode(&md); err != nil {
		log.Fatal("decode error:", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500) //500
	defer cancel()
	c = metadata2.NewOutgoingContext(ctx, md)
	return name, typeId, seqId, c, nil
}
func (p *TBinaryProtocol) ReadMessageBegin() (name string, typeId TMessageType, seqId int32, err error) {
	size, e := p.ReadI32()
	if e != nil {
		return "", typeId, 0, NewTProtocolException(e)
	}
	if size < 0 {
		typeId = TMessageType(size & 0x0ff)
		version := int64(int64(size) & VERSION_MASK)
		if version != VERSION_1 {
			return name, typeId, seqId, NewTProtocolExceptionWithType(BAD_VERSION, fmt.Errorf("Bad version in ReadMessageBegin"))
		}
		name, e = p.ReadString()
		if e != nil {
			return name, typeId, seqId, NewTProtocolException(e)
		}
		seqId, e = p.ReadI32()
		if e != nil {
			return name, typeId, seqId, NewTProtocolException(e)
		}
		return name, typeId, seqId, nil
	}
	if p.strictRead {
		return name, typeId, seqId, NewTProtocolExceptionWithType(BAD_VERSION, fmt.Errorf("Missing version in ReadMessageBegin"))
	}
	name, e2 := p.readStringBody(size)
	if e2 != nil {
		return name, typeId, seqId, e2
	}
	b, e3 := p.ReadByte()
	if e3 != nil {
		return name, typeId, seqId, e3
	}
	typeId = TMessageType(b)
	seqId, e4 := p.ReadI32()
	if e4 != nil {
		return name, typeId, seqId, e4
	}
	return name, typeId, seqId, nil
}

func (p *TBinaryProtocol) ReadMessageEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadStructBegin() (name string, err error) {
	return
}

func (p *TBinaryProtocol) ReadStructEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadFieldBegin() (name string, typeId TType, seqId int16, err error) {
	t, err := p.ReadByte()
	typeId = TType(t)
	if err != nil {
		return name, typeId, seqId, err
	}
	if t != STOP {
		seqId, err = p.ReadI16()
	}
	return name, typeId, seqId, err
}

func (p *TBinaryProtocol) ReadFieldEnd() error {
	return nil
}

var invalidDataLength = NewTProtocolExceptionWithType(INVALID_DATA, errors.New("Invalid data length"))

func (p *TBinaryProtocol) ReadMapBegin() (kType, vType TType, size int, err error) {
	k, e := p.ReadByte()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	kType = TType(k)
	v, e := p.ReadByte()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	vType = TType(v)
	size32, e := p.ReadI32()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	size = int(size32)
	return kType, vType, size, nil
}

func (p *TBinaryProtocol) ReadMapEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadListBegin() (elemType TType, size int, err error) {
	b, e := p.ReadByte()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	elemType = TType(b)
	size32, e := p.ReadI32()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	size = int(size32)

	return
}

func (p *TBinaryProtocol) ReadListEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadSetBegin() (elemType TType, size int, err error) {
	b, e := p.ReadByte()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	elemType = TType(b)
	size32, e := p.ReadI32()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	size = int(size32)
	return elemType, size, nil
}

func (p *TBinaryProtocol) ReadSetEnd() error {
	return nil
}

func (p *TBinaryProtocol) ReadBool() (bool, error) {
	b, e := p.ReadByte()
	v := true
	if b != 1 {
		v = false
	}
	return v, e
}

func (p *TBinaryProtocol) ReadByte() (int8, error) {
	v, err := p.trans.ReadByte()
	return int8(v), err
}

func (p *TBinaryProtocol) ReadI16() (value int16, err error) {
	buf := p.buffer[0:2]
	err = p.readAll(buf)
	value = int16(binary.BigEndian.Uint16(buf))
	return value, err
}

func (p *TBinaryProtocol) ReadI32() (value int32, err error) {
	buf := p.buffer[0:4]
	err = p.readAll(buf)
	value = int32(binary.BigEndian.Uint32(buf))
	return value, err
}

func (p *TBinaryProtocol) ReadI64() (value int64, err error) {
	buf := p.buffer[0:8]
	err = p.readAll(buf)
	value = int64(binary.BigEndian.Uint64(buf))
	return value, err
}

func (p *TBinaryProtocol) ReadDouble() (value float64, err error) {
	buf := p.buffer[0:8]
	err = p.readAll(buf)
	value = math.Float64frombits(binary.BigEndian.Uint64(buf))
	return value, err
}

func (p *TBinaryProtocol) ReadString() (value string, err error) {
	size, e := p.ReadI32()
	if e != nil {
		return "", e
	}
	if size < 0 {
		err = invalidDataLength
		return
	}

	return p.readStringBody(size)
}

func (p *TBinaryProtocol) ReadBinary() ([]byte, error) {
	size, e := p.ReadI32()
	if e != nil {
		return nil, e
	}
	if size < 0 {
		return nil, invalidDataLength
	}

	isize := int(size)
	buf := make([]byte, isize)
	_, err := io.ReadFull(p.trans, buf)
	return buf, NewTProtocolException(err)
}

func (p *TBinaryProtocol) Flush(ctx context.Context) (err error) {
	return NewTProtocolException(p.trans.Flush(ctx))
}

func (p *TBinaryProtocol) Skip(fieldType TType) (err error) {
	return SkipDefaultDepth(p, fieldType)
}

func (p *TBinaryProtocol) Transport() TTransport {
	return p.origTransport
}

func (p *TBinaryProtocol) readAll(buf []byte) error {
	_, err := io.ReadFull(p.reader, buf)
	return NewTProtocolException(err)
}

const readLimit = 32768

func (p *TBinaryProtocol) readStringBody(size int32) (value string, err error) {
	if size < 0 {
		return "", nil
	}

	var (
		buf bytes.Buffer
		e   error
		b   []byte
	)

	switch {
	case int(size) <= len(p.buffer):
		b = p.buffer[:size] // avoids allocation for small reads
	case int(size) < readLimit:
		b = make([]byte, size)
	default:
		b = make([]byte, readLimit)
	}

	for size > 0 {
		_, e = io.ReadFull(p.trans, b)
		buf.Write(b)
		if e != nil {
			break
		}
		size -= readLimit
		if size < readLimit && size > 0 {
			b = b[:size]
		}
	}
	return buf.String(), NewTProtocolException(e)
}
