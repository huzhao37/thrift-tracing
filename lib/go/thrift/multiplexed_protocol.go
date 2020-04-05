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
	"context"
	"fmt"
	metadata2 "github.com/apache/thrift/lib/go/thrift/metadata2"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"strings"
)

/*
TMultiplexedProtocol is a protocol-independent concrete decorator
that allows a Thrift client to communicate with a multiplexing Thrift server,
by prepending the service name to the function name during function calls.

NOTE: THIS IS NOT USED BY SERVERS.  On the server, use TMultiplexedProcessor to handle request
from a multiplexing client.

This example uses a single socket transport to invoke two services:

socket := thrift.NewTSocketFromAddrTimeout(addr, TIMEOUT)
transport := thrift.NewTFramedTransport(socket)
protocol := thrift.NewTBinaryProtocolTransport(transport)

mp := thrift.NewTMultiplexedProtocol(protocol, "Calculator")
service := Calculator.NewCalculatorClient(mp)

mp2 := thrift.NewTMultiplexedProtocol(protocol, "WeatherReport")
service2 := WeatherReport.NewWeatherReportClient(mp2)

err := transport.Open()
if err != nil {
	t.Fatal("Unable to open client socket", err)
}

fmt.Println(service.Add(2,2))
fmt.Println(service2.GetTemperature())
*/

type TMultiplexedProtocol struct {
	TProtocol
	serviceName string
}
type MDReaderWriter struct {
	metadata2.MD
}

// ForeachKey implements ForeachKey of opentracing.TextMapReader
func (c MDReaderWriter) ForeachKey(handler func(key, val string) error) error {
	for k, vs := range c.MD {
		for _, v := range vs {
			if err := handler(k, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// Set implements Set() of opentracing.TextMapWriter
func (c MDReaderWriter) Set(key, val string) {
	key = strings.ToLower(key)
	c.MD[key] = append(c.MD[key], val)
}

var tracer opentracing.Tracer

const MULTIPLEXED_SEPARATOR = ":"

//open trace
func (t *TMultiplexedProcessor) SetTrace(td opentracing.Tracer) {
	tracer = td
}

func NewTMultiplexedProtocol(protocol TProtocol, serviceName string) *TMultiplexedProtocol {
	return &TMultiplexedProtocol{
		TProtocol:   protocol,
		serviceName: serviceName,
	}
}

func (t *TMultiplexedProtocol) WriteMessageBegin(name string, typeId TMessageType, seqid int32) error {
	if typeId == CALL || typeId == ONEWAY {
		return t.TProtocol.WriteMessageBegin(t.serviceName+MULTIPLEXED_SEPARATOR+name, typeId, seqid)
	} else {
		return t.TProtocol.WriteMessageBegin(name, typeId, seqid)
	}
}

/*
TMultiplexedProcessor is a TProcessor allowing
a single TServer to provide multiple services.

To do so, you instantiate the processor and then register additional
processors with it, as shown in the following example:

var processor = thrift.NewTMultiplexedProcessor()

firstProcessor :=
processor.RegisterProcessor("FirstService", firstProcessor)

processor.registerProcessor(
  "Calculator",
  Calculator.NewCalculatorProcessor(&CalculatorHandler{}),
)

processor.registerProcessor(
  "WeatherReport",
  WeatherReport.NewWeatherReportProcessor(&WeatherReportHandler{}),
)

serverTransport, err := thrift.NewTServerSocketTimeout(addr, TIMEOUT)
if err != nil {
  t.Fatal("Unable to create server socket", err)
}
server := thrift.NewTSimpleServer2(processor, serverTransport)
server.Serve();
*/

type TMultiplexedProcessor struct {
	serviceProcessorMap map[string]TProcessor
	DefaultProcessor    TProcessor
}

func NewTMultiplexedProcessor() *TMultiplexedProcessor {
	return &TMultiplexedProcessor{
		serviceProcessorMap: make(map[string]TProcessor),
	}
}

func (t *TMultiplexedProcessor) RegisterDefault(processor TProcessor) {
	t.DefaultProcessor = processor
}

func (t *TMultiplexedProcessor) RegisterProcessor(name string, processor TProcessor) {
	if t.serviceProcessorMap == nil {
		t.serviceProcessorMap = make(map[string]TProcessor)
	}
	t.serviceProcessorMap[name] = processor
}

//add trace middleware
func (t *TMultiplexedProcessor) Process(ctx context.Context, in, out TProtocol) (bool, TException) {
	name, typeId, seqid, err := in.ReadMessageBegin()
	if err != nil {
		return false, err
	}
	if typeId != CALL && typeId != ONEWAY {
		return false, fmt.Errorf("Unexpected message type %v", typeId)
	}
	//extract the service name
	v := strings.SplitN(name, MULTIPLEXED_SEPARATOR, 2)
	if len(v) != 2 {
		if t.DefaultProcessor != nil {
			smb := NewStoredMessageProtocol(in, name, typeId, seqid)
			return t.DefaultProcessor.Process(ctx, smb, out)
		}
		return false, fmt.Errorf("Service name not found in message name: %s.  Did you forget to use a TMultiplexProtocol in your client?", name)
	}
	actualProcessor, ok := t.serviceProcessorMap[v[0]]
	if !ok {
		return false, fmt.Errorf("Service name not found: %s.  Did you forget to call registerProcessor()?", v[0])
	}
	smb := NewStoredMessageProtocol(in, v[1], typeId, seqid)
	fmt.Printf("1\n")
	parentContext, _, _ := serverInterceptor(ctx, smb, name)
	return actualProcessor.Process(parentContext, smb, out)
}

//Protocol that use stored message for ReadMessageBegin
type storedMessageProtocol struct {
	TProtocol
	name   string
	typeId TMessageType
	seqid  int32
}

func NewStoredMessageProtocol(protocol TProtocol, name string, typeId TMessageType, seqid int32) *storedMessageProtocol {
	return &storedMessageProtocol{protocol, name, typeId, seqid}
}

func (s *storedMessageProtocol) ReadMessageBegin() (name string, typeId TMessageType, seqid int32, err error) {
	return s.name, s.typeId, s.seqid, nil
}

//opentrace middleware
func serverInterceptor(ctx context.Context, req interface{}, info string) (parentContext context.Context, resp interface{}, err error) {
	//var parentContext context.Context
	md, ok := metadata2.FromIncomingContext(ctx)
	if !ok {
		md = metadata2.New(nil)
	}

	spanContext, err := tracer.Extract(opentracing.TextMap, MDReaderWriter{md})
	if err != nil && err != opentracing.ErrSpanContextNotFound {
		fmt.Printf("extract from metadata err: %v", err)
		return ctx, resp, err
	} else {
		span := tracer.StartSpan(
			"info.FullMethod",
			ext.RPCServerOption(spanContext),
			opentracing.Tag{Key: string(ext.Component), Value: "thrift"},
			ext.SpanKindRPCServer,
		)
		defer span.Finish()
		parentContext = opentracing.ContextWithSpan(ctx, span)
		return parentContext, resp, nil
	}

}
