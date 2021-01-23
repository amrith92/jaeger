// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"bytes"
	"encoding/json"
	"github.com/jaegertracing/jaeger/swagger-gen/models"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/model/converter/thrift/zipkin"
)

// Unmarshaller decodes a byte array to a span
type Unmarshaller interface {
	Unmarshal([]byte) ([]*model.Span, error)
}

// ProtobufUnmarshaller implements Unmarshaller
type ProtobufUnmarshaller struct{}

// NewProtobufUnmarshaller constructs a ProtobufUnmarshaller
func NewProtobufUnmarshaller() *ProtobufUnmarshaller {
	return &ProtobufUnmarshaller{}
}

// Unmarshal decodes a protobuf byte array to a span
func (h *ProtobufUnmarshaller) Unmarshal(msg []byte) ([]*model.Span, error) {
	newSpan := &model.Span{}
	err := proto.Unmarshal(msg, newSpan)
	spans := []*model.Span{newSpan}
	return spans, err
}

// JSONUnmarshaller implements Unmarshaller
type JSONUnmarshaller struct{}

// NewJSONUnmarshaller constructs a JSONUnmarshaller
func NewJSONUnmarshaller() *JSONUnmarshaller {
	return &JSONUnmarshaller{}
}

// Unmarshal decodes a json byte array to a span
func (h *JSONUnmarshaller) Unmarshal(msg []byte) ([]*model.Span, error) {
	newSpan := &model.Span{}
	err := jsonpb.Unmarshal(bytes.NewReader(msg), newSpan)
	spans := []*model.Span{newSpan}
	return spans, err
}

// ZipkinThriftUnmarshaller implements Unmarshaller
type ZipkinThriftUnmarshaller struct{}

// NewZipkinThriftUnmarshaller constructs a ZipkinThriftUnmarshaller
func NewZipkinThriftUnmarshaller() *ZipkinThriftUnmarshaller {
	return &ZipkinThriftUnmarshaller{}
}

// Unmarshal decodes a json byte array to a span
func (h *ZipkinThriftUnmarshaller) Unmarshal(msg []byte) ([]*model.Span, error) {
	tSpans, err := zipkin.DeserializeThrift(msg)
	if err != nil {
		return nil, err
	}
	mSpans, err := zipkin.ToDomainSpan(tSpans[0])
	if err != nil {
		return nil, err
	}
	return mSpans, err
}

// ZipkinJsonV2Unmarshaller implements Unmarshaller
type ZipkinJsonV2Unmarshaller struct{}

// NewZipkinJsonV2Unmarshaller constructs a ZipkinJsonV2Unmarshaller
func NewZipkinJsonV2Unmarshaller() *ZipkinJsonV2Unmarshaller {
	return &ZipkinJsonV2Unmarshaller{}
}

// Unmarshal decodes json v2 format data to a span
func (h *ZipkinJsonV2Unmarshaller) Unmarshal(msg []byte) ([]*model.Span, error) {
	var j2Spans []*models.Span
	err := json.Unmarshal(msg, &j2Spans)
	if err != nil {
		return nil, err
	}

	tSpans, err := zipkin.SpansV2ToThrift(j2Spans)
	if err != nil {
		return nil, err
	}

	spans := make([]*model.Span, 0)
	for _, tSpan := range tSpans {
		mSpans, err := zipkin.ToDomainSpan(tSpan)
		if err != nil {
			return nil, err
		}
		for _, span := range mSpans {
			spans = append(spans, span)
		}
	}

	return spans, nil
}
