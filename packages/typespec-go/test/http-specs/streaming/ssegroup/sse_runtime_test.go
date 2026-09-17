// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

package ssegroup

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func collect[T any](t *testing.T, body string, decode func(Event) (T, bool, error)) ([]T, *EventStream[T]) {
	t.Helper()
	s := NewEventStream[T](io.NopCloser(strings.NewReader(body)), decode)
	var got []T
	for {
		v, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got = append(got, v)
	}
	return got, s
}

func TestUnnamedEvents(t *testing.T) {
	body := "data: {\"desc\": \"one\"}\n\ndata: {\"desc\": \"two\"}\n\ndata: {\"desc\": \"three\"}\n\n"
	got, _ := collect(t, body, decodeUnnamedEvents)
	want := []string{"one", "two", "three"}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Info == nil || got[i].Info.Desc == nil || *got[i].Info.Desc != w {
			t.Fatalf("event %d = %+v, want desc %q", i, got[i], w)
		}
	}
}

func TestNamedEventsWithTerminal(t *testing.T) {
	body := "event: responseCreated\ndata: {\"id\": \"resp_1\"}\n\n" +
		"event: responseDelta\ndata: {\"delta\": \"Hello\"}\n\n" +
		"event: responseDelta\ndata: {\"delta\": \" world\"}\n\n" +
		"data: [DONE]\n\n"
	got, _ := collect(t, body, decodeResponseEvents)
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3 (terminal [DONE] ends the stream)", len(got))
	}
	if got[0].ResponseCreated == nil || *got[0].ResponseCreated.ID != "resp_1" {
		t.Fatalf("event 0 = %+v", got[0])
	}
	if got[1].ResponseDelta == nil || *got[1].ResponseDelta.Delta != "Hello" {
		t.Fatalf("event 1 = %+v", got[1])
	}
	if got[2].ResponseDelta == nil || *got[2].ResponseDelta.Delta != " world" {
		t.Fatalf("event 2 = %+v", got[2])
	}
}

func TestProtocolEnvelopeMetadata(t *testing.T) {
	body := "id: event-1\nevent: message\ndata: {\"message\": \"hello\"}\n\n"
	got, s := collect(t, body, decodeProtocolEvents)
	if len(got) != 1 || got[0].ProtocolInfo == nil || *got[0].ProtocolInfo.Message != "hello" {
		t.Fatalf("got %+v", got)
	}
	if s.LastEventID() != "event-1" {
		t.Fatalf("LastEventID = %q, want event-1", s.LastEventID())
	}
}

func TestProtocolRetryValid(t *testing.T) {
	body := "retry: 1000\nevent: message\ndata: {\"message\": \"hello\"}\n\n"
	_, s := collect(t, body, decodeProtocolEvents)
	if s.RetryAfter() != 1000 {
		t.Fatalf("RetryAfter = %d, want 1000", s.RetryAfter())
	}
}

func TestProtocolRetryInvalidIgnored(t *testing.T) {
	body := "retry: not-a-number\nevent: message\ndata: {\"message\": \"hello\"}\n\n"
	_, s := collect(t, body, decodeProtocolEvents)
	if s.RetryAfter() != -1 {
		t.Fatalf("RetryAfter = %d, want -1 for invalid retry", s.RetryAfter())
	}
}

func TestProtocolInvalidIDIgnored(t *testing.T) {
	body := "id: invalid\x00id\nevent: message\ndata: {\"message\": \"hello\"}\n\n"
	_, s := collect(t, body, decodeProtocolEvents)
	if s.LastEventID() != "" {
		t.Fatalf("LastEventID = %q, want empty for id containing NUL", s.LastEventID())
	}
}

func TestDataWithEnvelopeNoTrailingBlank(t *testing.T) {
	body := "event: withEnvelope\ndata: hello"
	got, _ := collect(t, body, decodeDataEvents)
	if len(got) != 1 || got[0].WithEnvelope == nil || *got[0].WithEnvelope.Contents != "hello" {
		t.Fatalf("got %+v", got)
	}
}

func TestDataWithoutEnvelope(t *testing.T) {
	body := "event: withoutEnvelope\ndata: {\"metadata\": {\"source\": \"test\"}, \"contents\": \"world\"}\n"
	got, _ := collect(t, body, decodeDataEvents)
	if len(got) != 1 || got[0].WithEnvelope1 == nil || *got[0].WithEnvelope1.Contents != "world" {
		t.Fatalf("got %+v", got)
	}
	if got[0].WithEnvelope1.Metadata["source"] == nil || *got[0].WithEnvelope1.Metadata["source"] != "test" {
		t.Fatalf("metadata = %+v", got[0].WithEnvelope1.Metadata)
	}
}

func TestCRLFLineEndings(t *testing.T) {
	body := "event: responseDelta\r\ndata: {\"delta\": \"crlf\"}\r\n\r\n"
	got, _ := collect(t, body, decodeResponseEvents)
	if len(got) != 1 || got[0].ResponseDelta == nil || *got[0].ResponseDelta.Delta != "crlf" {
		t.Fatalf("got %+v", got)
	}
}

func TestMultilineData(t *testing.T) {
	// Two data lines concatenate with a newline; the JSON stays valid.
	body := "event: responseDelta\ndata: {\"delta\":\ndata: \"multi\"}\n\n"
	got, _ := collect(t, body, decodeResponseEvents)
	if len(got) != 1 || got[0].ResponseDelta == nil || *got[0].ResponseDelta.Delta != "multi" {
		t.Fatalf("got %+v", got)
	}
}

func TestEventsIterator(t *testing.T) {
	body := "data: {\"desc\": \"a\"}\n\ndata: {\"desc\": \"b\"}\n\n"
	s := NewEventStream[UnnamedEvents](io.NopCloser(strings.NewReader(body)), decodeUnnamedEvents)
	var descs []string
	for ev, err := range s.Events() {
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		descs = append(descs, *ev.Info.Desc)
	}
	if strings.Join(descs, ",") != "a,b" {
		t.Fatalf("descs = %v", descs)
	}
}

// TestProducerRoundTrip validates the fake/server path: typed events are encoded
// to SSE wire bytes and decoded back through the client reader unchanged.
func TestProducerRoundTrip(t *testing.T) {
	created := "resp_1"
	hello, world := "Hello", " world"
	done := "[DONE]"
	producer, err := NewResponseEventsStream([]ResponseEvents{
		{ResponseCreated: &ResponseCreated{ID: &created}},
		{ResponseDelta: &ResponseDelta{Delta: &hello}},
		{ResponseDelta: &ResponseDelta{Delta: &world}},
		{LiteralString: &done},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wire, err := MarshalEventStream(producer)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	client := NewEventStream[ResponseEvents](wire, decodeResponseEvents)
	var got []ResponseEvents
	for {
		v, err := client.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3 (terminal [DONE] ends the stream)", len(got))
	}
	if *got[0].ResponseCreated.ID != "resp_1" || *got[1].ResponseDelta.Delta != "Hello" || *got[2].ResponseDelta.Delta != " world" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// TestFramesPreserveEnvelopeMetadata validates that a frame-based producer
// (used for id/retry scenarios) renders envelope metadata that the reader surfaces.
func TestFramesPreserveEnvelopeMetadata(t *testing.T) {
	producer := NewEventStreamFromFrames[ProtocolEvents]([]Event{
		{ID: "event-1", Type: "message", Retry: 1000, Data: []byte(`{"message": "hello"}`)},
	})
	wire, err := MarshalEventStream(producer)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, s := collect(t, mustReadAll(t, wire), decodeProtocolEvents)
	if len(got) != 1 || *got[0].ProtocolInfo.Message != "hello" {
		t.Fatalf("got %+v", got)
	}
	if s.LastEventID() != "event-1" || s.RetryAfter() != 1000 {
		t.Fatalf("metadata lost: id=%q retry=%d", s.LastEventID(), s.RetryAfter())
	}
}

func mustReadAll(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}
