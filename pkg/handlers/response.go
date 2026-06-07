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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	eppb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"

	envoy "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/envoy"
	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	datasource "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/datasource"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/metrics"
)

// HandleResponseHeaders extracts response headers into reqCtx and returns
// the ext-proc header response.
func (s *Server) HandleResponseHeaders(ctx context.Context, reqCtx *RequestContext, headers *eppb.HttpHeaders) []*eppb.ProcessingResponse {
	if headers != nil && headers.Headers != nil {
		for _, header := range headers.Headers.Headers {
			reqCtx.Response.Headers[header.Key] = envoy.GetHeaderValue(header)
		}
	}

	if !headers.GetEndOfStream() {
		log.FromContext(ctx).V(logutil.VERBOSE).Info("captured response headers, deferring response until body arrives...")
	}
	// Always respond to response headers so Envoy proceeds with body chunks.
	// In STREAMED/FULL_DUPLEX_STREAMED mode, Envoy blocks until we respond.
	return []*eppb.ProcessingResponse{
		{
			Response: &eppb.ProcessingResponse_ResponseHeaders{
				ResponseHeaders: &eppb.HeadersResponse{},
			},
		},
	}
}

// HandleResponseBody handles response bodies by executing response plugins in order.
func (s *Server) HandleResponseBody(ctx context.Context, reqCtx *RequestContext, responseBodyBytes []byte) ([]*eppb.ProcessingResponse, error) {
	reqCtx.ResponseCompleteTimestamp = time.Now()

	logger := log.FromContext(ctx)
	if err := json.Unmarshal(responseBodyBytes, &reqCtx.Response.Body); err != nil {
		// Streaming responses arrive as SSE (data: {...}\n\n). Try to extract
		// usage/model fields by parsing the SSE event stream.
		if sseBody, sseErr := parseSSEResponseBody(responseBodyBytes); sseErr == nil && sseBody != nil {
			reqCtx.Response.Body = sseBody
			logger.V(logutil.VERBOSE).Info("parsed SSE response body for response plugins")
		} else {
			logger.V(logutil.VERBOSE).Info("response body is not JSON or SSE, skipping response plugins")
		}
	}

	ttft := reqCtx.ResponseFirstChunkTimestamp.Sub(reqCtx.RequestSentTimestamp)
	duration := reqCtx.ResponseCompleteTimestamp.Sub(reqCtx.RequestReceivedTimestamp)
	decodeTime := duration - ttft
	logger.Info("response timing",
		"chunks", reqCtx.responseChunkCount+1,
		"ttft", ttft.Seconds(),
		"decodeTime", decodeTime.Seconds(),
		"totalDuration", duration.Seconds(),
	)

	// Notify the data layer after the body is parsed so extractors can read Response.Body fields
	// (e.g. usage.completion_tokens for TPOT).
	if s.eventNotifier != nil {
		s.eventNotifier.Notify(datasource.Event{
			Type: datasource.ResponseEventType,
			Payload: datasource.ResponsePayload{
				Request:  reqCtx.Request,
				Response: reqCtx.Response,
				Duration: duration,
				TTFT:     ttft,
			},
		})
	}

	if len(s.responsePlugins) == 0 || reqCtx.Response.Body == nil {
		return s.generateEmptyResponseBodyResponse(reqCtx, responseBodyBytes), nil
	}

	if err := s.runResponsePlugins(ctx, reqCtx.CycleState, reqCtx.Response); err != nil {
		return nil, err
	}

	bodyMutated := reqCtx.Response.BodyMutated()
	var mutatedBytes []byte
	if bodyMutated {
		var err error
		mutatedBytes, err = json.Marshal(reqCtx.Response.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal mutated response body - %w", err)
		}
		reqCtx.Response.SetHeader(contentLengthHeader, strconv.Itoa(len(mutatedBytes)))
	}

	var ret []*eppb.ProcessingResponse
	ret = append(ret, &eppb.ProcessingResponse{
		Response: &eppb.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &eppb.HeadersResponse{
				Response: &eppb.CommonResponse{
					ClearRouteCache: true,
					HeaderMutation: &eppb.HeaderMutation{
						SetHeaders:    envoy.GenerateHeadersMutation(reqCtx.Response.MutatedHeaders()),
						RemoveHeaders: reqCtx.Response.RemovedHeaders(),
					},
				},
			},
		},
	})
	if bodyMutated {
		ret = envoy.AddStreamedResponseBody(ret, mutatedBytes)
	} else {
		ret = envoy.AddStreamedResponseBody(ret, responseBodyBytes)
	}
	return ret, nil
}

// generateEmptyResponseBodyResponse builds a pass-through body response.
// It only prepends a ResponseHeaders response when headers processing was deferred
// (reqCtx.ResponseHeadersDeferred=true), i.e. when responseHeaders mode is SEND and
// HandleResponseHeaders returned nil waiting for the body. Sending ResponseHeaders when
// Envoy never requested it (SKIP mode) corrupts chunked transfer encoding.
func (s *Server) generateEmptyResponseBodyResponse(reqCtx *RequestContext, responseBodyBytes []byte) []*eppb.ProcessingResponse {
	var responses []*eppb.ProcessingResponse
	if reqCtx.ResponseHeadersDeferred {
		responses = append(responses, &eppb.ProcessingResponse{
			Response: &eppb.ProcessingResponse_ResponseHeaders{
				ResponseHeaders: &eppb.HeadersResponse{},
			},
		})
	}
	return envoy.AddStreamedResponseBody(responses, responseBodyBytes)
}

// parseSSEResponseBody extracts a composite response body from an SSE (Server-Sent Events)
// stream. It parses by SSE event boundaries instead of individual lines because one logical
// event may legally contain multiple consecutive `data:` lines that must be joined before
// JSON decoding. It merges usage and model fields from all events into a single map that
// response plugins can process, supporting both Anthropic (top-level usage) and OpenAI
// (nested in response) formats.
func parseSSEResponseBody(body []byte) (map[string]any, error) {
	result := map[string]any{}
	lines := bytes.Split(body, []byte("\n"))
	eventDataLines := make([][]byte, 0)

	flushEvent := func() {
		if len(eventDataLines) == 0 {
			return
		}
		data := bytes.Join(eventDataLines, []byte("\n"))
		eventDataLines = eventDataLines[:0]
		data = bytes.TrimSpace(data)
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			return
		}
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil {
			return
		}
		if model, ok := event["model"].(string); ok && model != "" {
			result["model"] = model
		}
		// Check for usage at top level (Anthropic) or nested in response (OpenAI Responses API)
		usage, _ := event["usage"].(map[string]any)
		if usage == nil {
			if resp, ok := event["response"].(map[string]any); ok {
				usage, _ = resp["usage"].(map[string]any)
				if m, ok := resp["model"].(string); ok && m != "" {
					result["model"] = m
				}
			}
		}
		if usage != nil {
			existing, _ := result["usage"].(map[string]any)
			if existing == nil {
				existing = map[string]any{}
			}
			for k, v := range usage {
				existing[k] = v
			}
			result["usage"] = existing
		}
	}

	for _, line := range lines {
		if bytes.HasPrefix(line, []byte("data: ")) {
			eventDataLines = append(eventDataLines, line[len("data: "):])
		} else if len(bytes.TrimSpace(line)) == 0 {
			// Blank line signals end of one SSE event.
			flushEvent()
		}
		// Other SSE fields (id:, event:, retry:) are ignored.
	}
	flushEvent() // flush any trailing event not terminated by a blank line

	if len(result) == 0 {
		return nil, errors.New("no parseable SSE data events found")
	}
	return result, nil
}

// HandleResponseTrailers handles response trailers.
func (s *Server) HandleResponseTrailers(trailers *eppb.HttpTrailers) ([]*eppb.ProcessingResponse, error) {
	return []*eppb.ProcessingResponse{
		{
			Response: &eppb.ProcessingResponse_ResponseTrailers{
				ResponseTrailers: &eppb.TrailersResponse{},
			},
		},
	}, nil
}

// runResponsePlugins executes response plugins in the order they were registered.
func (s *Server) runResponsePlugins(ctx context.Context, cycleState *plugin.CycleState, response *requesthandling.InferenceResponse) error {
	logger := log.FromContext(ctx).V(logutil.DEFAULT)

	// Cache verbose logger and check Enabled() once to avoid per-iteration
	// allocations from argument boxing when logging at that level is disabled.
	verboseLogger := logger.V(logutil.VERBOSE)
	verboseEnabled := verboseLogger.Enabled()

	var err error
	for _, plugin := range s.responsePlugins {
		if verboseEnabled {
			verboseLogger.Info("Executing response plugin", "plugin", plugin.TypedName())
		}
		before := time.Now()
		err = plugin.ProcessResponse(ctx, cycleState, response)
		metrics.RecordPluginProcessingLatency(responsePluginExtensionPoint, plugin.TypedName().Type, plugin.TypedName().Name, time.Since(before))
		if err != nil {
			logger.Error(err, "Failed to execute response plugin", "plugin", plugin.TypedName())
			return err
		}
	}

	return nil
}
