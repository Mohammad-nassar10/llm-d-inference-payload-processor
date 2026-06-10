/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	basepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	envoytest "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/envoy/test"
	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
)

const testPluginValue = "done"

// fakeResponsePlugin implements requesthandling.PayloadProcessor for testing response plugin execution.
type fakeResponsePlugin struct {
	name     string
	mutateFn func(ctx context.Context, cycleState *plugin.CycleState, response *requesthandling.InferenceResponse) error
}

func (p *fakeResponsePlugin) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "fake", Name: p.name}
}

func (p *fakeResponsePlugin) ProcessResponse(ctx context.Context, cycleState *plugin.CycleState, response *requesthandling.InferenceResponse) error {
	return p.mutateFn(ctx, cycleState, response)
}

var _ requesthandling.ResponseProcessor = &fakeResponsePlugin{}

func newTestRequestContext(profiles map[string]*requesthandling.Profile) *RequestContext {
	return &RequestContext{
		Profile:    profiles[testProfileName],
		CycleState: plugin.NewCycleState(),
		Request:    requesthandling.NewInferenceRequest(),
		Response:   requesthandling.NewInferenceResponse(),
	}
}

func TestHandleResponseBody_NoPlugins(t *testing.T) {
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	profiles := newTestProfiles()
	server := newServerForTest(profiles)
	responseBody := []byte(`{"choices":[{"text":"Hello!"}]}`)
	resp, err := server.HandleResponseBody(ctx, newTestRequestContext(profiles), responseBody)
	if err != nil {
		t.Fatalf("HandleResponseBody returned unexpected error: %v", err)
	}

	// With STREAMED response_body_mode, the body has already been forwarded
	// downstream via the per-chunk acks issued in Process. The EoS reply is
	// just an empty BodyResponse ack.
	want := []*extProcPb.ProcessingResponse{
		{
			Response: &extProcPb.ProcessingResponse_ResponseBody{
				ResponseBody: &extProcPb.BodyResponse{},
			},
		},
	}

	if diff := cmp.Diff(want, resp, protocmp.Transform()); diff != "" {
		t.Errorf("HandleResponseBody returned unexpected response, diff(-want, +got): %v", diff)
	}
}

func TestHandleResponseBody_SinglePlugin(t *testing.T) {
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	mutatePlugin := &fakeResponsePlugin{
		name: "mutator",
		mutateFn: func(_ context.Context, _ *plugin.CycleState, response *requesthandling.InferenceResponse) error {
			response.SetBodyField("mutated", true)
			return nil
		},
	}

	profiles := newTestProfiles()
	withResponsePlugins(profiles, mutatePlugin)
	server := newServerForTest(profiles)
	responseBody := []byte(`{"choices":[{"text":"Hello!"}]}`)
	resp, err := server.HandleResponseBody(ctx, newTestRequestContext(profiles), responseBody)
	if err != nil {
		t.Fatalf("HandleResponseBody returned unexpected error: %v", err)
	}

	wantBody, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"text": "Hello!"}},
		"mutated": true,
	})
	want := expectedStreamedResponseBodyMutation(wantBody)

	envoytest.SortSetHeadersInResponses(want)
	envoytest.SortSetHeadersInResponses(resp)
	if diff := cmp.Diff(want, resp, protocmp.Transform()); diff != "" {
		t.Errorf("HandleResponseBody returned unexpected response, diff(-want, +got): %v", diff)
	}
}

func TestHandleResponseBody_MultiplePlugins(t *testing.T) {
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	plugin1 := &fakeResponsePlugin{
		name: "plugin1",
		mutateFn: func(_ context.Context, _ *plugin.CycleState, response *requesthandling.InferenceResponse) error {
			response.SetBodyField("p1", testPluginValue)
			return nil
		},
	}
	plugin2 := &fakeResponsePlugin{
		name: "plugin2",
		mutateFn: func(_ context.Context, _ *plugin.CycleState, response *requesthandling.InferenceResponse) error {
			response.SetBodyField("p2", testPluginValue)
			return nil
		},
	}

	profiles := newTestProfiles()
	withResponsePlugins(profiles, plugin1, plugin2)
	server := newServerForTest(profiles)
	responseBody := []byte(`{"original":true}`)
	resp, err := server.HandleResponseBody(ctx, newTestRequestContext(profiles), responseBody)
	if err != nil {
		t.Fatalf("HandleResponseBody returned unexpected error: %v", err)
	}

	wantBody, _ := json.Marshal(map[string]any{
		"original": true,
		"p1":       testPluginValue,
		"p2":       testPluginValue,
	})
	want := expectedStreamedResponseBodyMutation(wantBody)

	envoytest.SortSetHeadersInResponses(want)
	envoytest.SortSetHeadersInResponses(resp)
	if diff := cmp.Diff(want, resp, protocmp.Transform()); diff != "" {
		t.Errorf("HandleResponseBody returned unexpected response, diff(-want, +got): %v", diff)
	}
}

func TestHandleResponseBody_PluginError(t *testing.T) {
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	failingPlugin := &fakeResponsePlugin{
		name: "failing",
		mutateFn: func(_ context.Context, _ *plugin.CycleState, _ *requesthandling.InferenceResponse) error {
			return errors.New("failed to execute plugin")
		},
	}

	profiles := newTestProfiles()
	withResponsePlugins(profiles, failingPlugin)
	server := newServerForTest(profiles)
	responseBody := []byte(`{"choices":[{"text":"some response"}]}`)
	_, err := server.HandleResponseBody(ctx, newTestRequestContext(profiles), responseBody)
	if err == nil {
		t.Fatal("HandleResponseBody should have returned an error")
	}

	if got := err.Error(); got == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestHandleResponseBody_StreamingWithPlugin(t *testing.T) {
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	mutatePlugin := &fakeResponsePlugin{
		name: "mutator",
		mutateFn: func(_ context.Context, _ *plugin.CycleState, response *requesthandling.InferenceResponse) error {
			response.SetBodyField("mutated", true)
			return nil
		},
	}

	profiles := newTestProfiles()
	withResponsePlugins(profiles, mutatePlugin)
	server := newServerForTest(profiles)
	responseBody := []byte(`{"choices":[{"text":"Hello!"}]}`)
	resp, err := server.HandleResponseBody(ctx, newTestRequestContext(profiles), responseBody)
	if err != nil {
		t.Fatalf("HandleResponseBody returned unexpected error: %v", err)
	}

	wantBody, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"text": "Hello!"}},
		"mutated": true,
	})
	want := expectedStreamedResponseBodyMutation(wantBody)

	envoytest.SortSetHeadersInResponses(want)
	envoytest.SortSetHeadersInResponses(resp)
	if diff := cmp.Diff(want, resp, protocmp.Transform()); diff != "" {
		t.Errorf("HandleResponseBody returned unexpected response, diff(-want, +got): %v", diff)
	}
}

func TestHandleResponseBody_PluginNoBodyMutation(t *testing.T) {
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	headerOnlyPlugin := &fakeResponsePlugin{
		name: "header-only",
		mutateFn: func(_ context.Context, _ *plugin.CycleState, response *requesthandling.InferenceResponse) error {
			response.SetHeader("X-Custom-Response", "added")
			return nil
		},
	}

	responseBody := []byte(`{"choices":[{"text":"Hello!"}]}`)
	want := []*extProcPb.ProcessingResponse{
		{
			Response: &extProcPb.ProcessingResponse_ResponseHeaders{
				ResponseHeaders: &extProcPb.HeadersResponse{
					Response: &extProcPb.CommonResponse{
						ClearRouteCache: true,
						HeaderMutation: &extProcPb.HeaderMutation{
							SetHeaders: []*basepb.HeaderValueOption{
								{
									Header: &basepb.HeaderValue{
										Key:      "X-Custom-Response",
										RawValue: []byte("added"),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			Response: &extProcPb.ProcessingResponse_ResponseBody{
				ResponseBody: &extProcPb.BodyResponse{
					Response: &extProcPb.CommonResponse{
						BodyMutation: &extProcPb.BodyMutation{
							Mutation: &extProcPb.BodyMutation_StreamedResponse{
								StreamedResponse: &extProcPb.StreamedBodyResponse{
									Body:        responseBody,
									EndOfStream: true,
								},
							},
						},
					},
				},
			},
		},
	}

	profiles := newTestProfiles()
	withResponsePlugins(profiles, headerOnlyPlugin)
	server := newServerForTest(profiles)
	resp, err := server.HandleResponseBody(ctx, newTestRequestContext(profiles), responseBody)
	if err != nil {
		t.Fatalf("HandleResponseBody returned unexpected error: %v", err)
	}

	envoytest.SortSetHeadersInResponses(want)
	envoytest.SortSetHeadersInResponses(resp)
	if diff := cmp.Diff(want, resp, protocmp.Transform()); diff != "" {
		t.Errorf("HandleResponseBody returned unexpected response, diff(-want, +got): %v", diff)
	}
}

// expectedStreamedResponseBodyMutation builds the expected streamed response for a mutated body:
// a deferred ResponseHeaders with header mutation, then a ResponseBody with StreamedBodyResponse.
func expectedStreamedResponseBodyMutation(bodyBytes []byte) []*extProcPb.ProcessingResponse {
	return []*extProcPb.ProcessingResponse{
		{
			Response: &extProcPb.ProcessingResponse_ResponseHeaders{
				ResponseHeaders: &extProcPb.HeadersResponse{
					Response: &extProcPb.CommonResponse{
						ClearRouteCache: true,
						HeaderMutation: &extProcPb.HeaderMutation{
							SetHeaders: []*basepb.HeaderValueOption{
								{
									Header: &basepb.HeaderValue{
										Key:      contentLengthHeader,
										RawValue: []byte(strconv.Itoa(len(bodyBytes))),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			Response: &extProcPb.ProcessingResponse_ResponseBody{
				ResponseBody: &extProcPb.BodyResponse{
					Response: &extProcPb.CommonResponse{
						BodyMutation: &extProcPb.BodyMutation{
							Mutation: &extProcPb.BodyMutation_StreamedResponse{
								StreamedResponse: &extProcPb.StreamedBodyResponse{
									Body:        bodyBytes,
									EndOfStream: true,
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestParseSSEResponseBody(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		want      map[string]any
		wantError bool
	}{
		{
			name:      "empty input",
			body:      "",
			wantError: true,
		},
		{
			name:      "only [DONE] marker",
			body:      "data: [DONE]\n\n",
			wantError: true,
		},
		{
			name: "single event with top-level model and usage (OpenAI / Anthropic chat style)",
			body: "data: {\"model\":\"gpt-4\",\"usage\":{\"completion_tokens\":12,\"prompt_tokens\":34}}\n\n",
			want: map[string]any{
				"model": "gpt-4",
				"usage": map[string]any{
					"completion_tokens": float64(12),
					"prompt_tokens":     float64(34),
				},
			},
		},
		{
			name: "event nested under response (OpenAI Responses API style)",
			body: "data: {\"response\":{\"model\":\"gpt-4o\",\"usage\":{\"completion_tokens\":7}}}\n\n",
			want: map[string]any{
				"model": "gpt-4o",
				"usage": map[string]any{
					"completion_tokens": float64(7),
				},
			},
		},
		{
			name: "multiple events: later usage overrides earlier",
			body: "data: {\"model\":\"gpt-4\",\"usage\":{\"prompt_tokens\":10}}\n\n" +
				"data: {\"usage\":{\"completion_tokens\":20}}\n\n" +
				"data: [DONE]\n\n",
			want: map[string]any{
				"model": "gpt-4",
				"usage": map[string]any{
					"prompt_tokens":     float64(10),
					"completion_tokens": float64(20),
				},
			},
		},
		{
			name: "multi-line data: joined before JSON decoding",
			body: "data: {\"model\":\"gpt-4\",\n" +
				"data: \"usage\":{\"completion_tokens\":5}}\n\n",
			want: map[string]any{
				"model": "gpt-4",
				"usage": map[string]any{
					"completion_tokens": float64(5),
				},
			},
		},
		{
			name: "malformed JSON event is skipped, valid event is kept",
			body: "data: not-json\n\n" +
				"data: {\"model\":\"gpt-4\"}\n\n",
			want: map[string]any{
				"model": "gpt-4",
			},
		},
		{
			name: "CRLF line endings",
			body: "data: {\"model\":\"gpt-4\"}\r\n\r\n",
			want: map[string]any{
				"model": "gpt-4",
			},
		},
		{
			name: "non-data lines (event:/id:/comments) are ignored",
			body: "event: message\n" +
				"id: 1\n" +
				": keep-alive\n" +
				"data: {\"model\":\"gpt-4\"}\n\n",
			want: map[string]any{
				"model": "gpt-4",
			},
		},
		{
			name:      "events with no model or usage produce no result",
			body:      "data: {\"foo\":\"bar\"}\n\n",
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSSEResponseBody([]byte(tc.body))
			if tc.wantError {
				if err == nil {
					t.Fatalf("parseSSEResponseBody: expected error, got result %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSSEResponseBody: unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("parseSSEResponseBody mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
