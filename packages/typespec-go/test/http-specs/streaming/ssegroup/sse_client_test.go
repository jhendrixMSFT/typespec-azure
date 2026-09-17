// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

package ssegroup_test

import (
	"context"
	"errors"
	"io"
	"ssegroup"
	"testing"

	"github.com/stretchr/testify/require"
)

func newSseClient(t *testing.T) *ssegroup.SseClient {
	t.Helper()
	client, err := ssegroup.NewSseClientWithNoCredential("http://localhost:3000", nil)
	require.NoError(t, err)
	return client
}

func drain[T any](t *testing.T, s *ssegroup.EventStream[T]) []T {
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
	resp, err := newSseClient(t).NewSseUnnamedClient().Receive(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
	require.Len(t, events, 3)
	for i, want := range []string{"one", "two", "three"} {
		require.NotNil(t, events[i].Info)
		require.Equal(t, want, *events[i].Info.Desc)
	}
}

func TestSseNamedClientReceive(t *testing.T) {
	resp, err := newSseClient(t).NewSseNamedClient().Receive(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	// the terminal [DONE] event ends the stream and is not surfaced as a value
	events := drain(t, resp.Stream)
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
	resp, err := newSseClient(t).NewSseRetrieveClient().Stream(context.Background(), ssegroup.RetrievalRequest{Query: &query}, nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
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
	resp, err := newSseClient(t).NewSseProtocolClient().NewSseProtocolDataClient().WithEnvelope(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].WithEnvelope)
	require.Equal(t, "hello", *events[0].WithEnvelope.Contents)
}

func TestSseProtocolDataWithoutEnvelope(t *testing.T) {
	resp, err := newSseClient(t).NewSseProtocolClient().NewSseProtocolDataClient().WithoutEnvelope(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].WithEnvelope1)
	require.Equal(t, "world", *events[0].WithEnvelope1.Contents)
	require.Equal(t, "test", *events[0].WithEnvelope1.Metadata["source"])
}

func TestSseProtocolID(t *testing.T) {
	resp, err := newSseClient(t).NewSseProtocolClient().ID(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
	require.Equal(t, "event-1", resp.Stream.LastEventID())
}

func TestSseProtocolInvalidID(t *testing.T) {
	resp, err := newSseClient(t).NewSseProtocolClient().InvalidID(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
	// an id containing U+0000 NULL is ignored per the SSE parsing rules
	require.Empty(t, resp.Stream.LastEventID())
}

func TestSseProtocolRetry(t *testing.T) {
	resp, err := newSseClient(t).NewSseProtocolClient().Retry(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
	require.Equal(t, 1000, resp.Stream.RetryAfter())
}

func TestSseProtocolInvalidRetry(t *testing.T) {
	resp, err := newSseClient(t).NewSseProtocolClient().InvalidRetry(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
	// a non-ASCII-digit retry value is ignored, leaving no reconnection delay
	require.Equal(t, -1, resp.Stream.RetryAfter())
}

func TestSseProtocolReconnect(t *testing.T) {
	resp, err := newSseClient(t).NewSseProtocolClient().Reconnect(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)
	defer resp.Stream.Close()

	events := drain(t, resp.Stream)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].ProtocolInfo)
	require.Equal(t, "hello", *events[0].ProtocolInfo.Message)
	require.Equal(t, "event-1", resp.Stream.LastEventID())
}
