// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

package ssegroup_test

import (
	"context"
	"errors"
	"io"
	"ssegroup"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/stretchr/testify/require"
)

func newSseClient(t *testing.T) *ssegroup.SseClient {
	t.Helper()
	client, err := ssegroup.NewSseClientWithNoCredential("http://localhost:3000", nil)
	require.NoError(t, err)
	return client
}

func drain[T any](t *testing.T, s *streaming.Event[T]) []T {
	t.Helper()
	var out []T
	for {
		v, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		out = append(out, v)
	}
	return out
}

func TestSseUnnamedClientReceive(t *testing.T) {
	stream, err := newSseClient(t).NewSseUnnamedClient().NewReceiveEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 3)
	for i, want := range []string{"one", "two", "three"} {
		require.NotNil(t, events[i].Info)
		require.Equal(t, want, *events[i].Info.Desc)
	}
}

func TestSseNamedClientReceive(t *testing.T) {
	stream, err := newSseClient(t).NewSseNamedClient().NewReceiveEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	// the terminal [DONE] event ends the stream and is not surfaced as a value
	events := drain(t, stream)
	require.Len(t, events, 3)

	require.NotNil(t, events[0].ResponseCreated)
	require.Equal(t, "resp_1", *events[0].ResponseCreated.ID)
	require.NotNil(t, events[1].ResponseDelta)
	require.Equal(t, "Hello", *events[1].ResponseDelta.Delta)
	require.NotNil(t, events[2].ResponseDelta)
	require.Equal(t, " world", *events[2].ResponseDelta.Delta)
}

func TestSseRetrieveClientStream(t *testing.T) {
	query := "what is typespec?"
	stream, err := newSseClient(t).NewSseRetrieveClient().NewStreamEventStream(context.Background(), ssegroup.RetrievalRequest{Query: &query}, nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 3)

	require.NotNil(t, events[0].PartialResult)
	require.Equal(t, "partial one", *events[0].PartialResult.Text)
	require.NotNil(t, events[1].PartialResult)
	require.Equal(t, "partial two", *events[1].PartialResult.Text)
	require.NotNil(t, events[2].FinalResult)
	require.Len(t, events[2].FinalResult.References, 2)
	require.Equal(t, "doc1", *events[2].FinalResult.References[0])
	require.Equal(t, "doc2", *events[2].FinalResult.References[1])
}

func TestSseProtocolDataWithEnvelope(t *testing.T) {
	stream, err := newSseClient(t).NewSseProtocolClient().NewSseProtocolDataClient().NewWithEnvelopeEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].WithEnvelope)
	require.Equal(t, "hello", *events[0].WithEnvelope.Contents)
}

func TestSseProtocolDataWithoutEnvelope(t *testing.T) {
	stream, err := newSseClient(t).NewSseProtocolClient().NewSseProtocolDataClient().NewWithoutEnvelopeEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].WithEnvelope1)
	require.Equal(t, "world", *events[0].WithEnvelope1.Contents)
	require.Equal(t, "test", *events[0].WithEnvelope1.Metadata["source"])
}

func TestSseProtocolID(t *testing.T) {
	stream, err := newSseClient(t).NewSseProtocolClient().NewIDEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
	require.Equal(t, "event-1", stream.LastEventID())
}

func TestSseProtocolInvalidID(t *testing.T) {
	stream, err := newSseClient(t).NewSseProtocolClient().NewInvalidIDEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
	// an id containing U+0000 NULL is ignored per the SSE parsing rules
	require.Empty(t, stream.LastEventID())
}

func TestSseProtocolRetry(t *testing.T) {
	stream, err := newSseClient(t).NewSseProtocolClient().NewRetryEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
}

func TestSseProtocolInvalidRetry(t *testing.T) {
	stream, err := newSseClient(t).NewSseProtocolClient().NewInvalidRetryEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
}

func TestSseProtocolReconnect(t *testing.T) {
	stream, err := newSseClient(t).NewSseProtocolClient().NewReconnectEventStream(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, stream)
	defer stream.Close()

	events := drain(t, stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
	require.Equal(t, "event-1", stream.LastEventID())
}
