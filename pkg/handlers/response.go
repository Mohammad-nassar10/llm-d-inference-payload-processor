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
	"fmt"
	"strconv"
	"strings"
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
		reqCtx.ResponseHeadersDeferred = true
		return nil
	}
	// EndOfStream means no body is expected, return HeadersResponse immediately
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
		// the last JSON chunk which vLLM places the usage field in.
		if body := parseSSEBody(responseBodyBytes); body != nil {
			reqCtx.Response.Body = body
		} else {
			logger.V(logutil.VERBOSE).Info("response body is not JSON or SSE, skipping response plugins")
		}
	}

	// Notify the data layer after the body is parsed so extractors can read Response.Body fields
	// (e.g. usage.completion_tokens for TPOT).
	if s.eventNotifier != nil {
		s.eventNotifier.Notify(datasource.Event{
			Type: datasource.ResponseEventType,
			Payload: datasource.ResponsePayload{
				Request:  reqCtx.Request,
				Response: reqCtx.Response,
				Duration: reqCtx.ResponseCompleteTimestamp.Sub(reqCtx.RequestReceivedTimestamp),
				TTFT:     reqCtx.ResponseFirstChunkTimestamp.Sub(reqCtx.RequestSentTimestamp),
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

// parseSSEBody scans an SSE stream (data: {...}\n\n) and returns the body map
// from the last non-[DONE] data line, or nil if none can be parsed.
// vLLM places the usage field in the final data chunk before [DONE].
func parseSSEBody(b []byte) map[string]any {
	const prefix = "data: "
	var last map[string]any
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		payload := line[len(prefix):]
		if payload == "[DONE]" {
			continue
		}
		var chunk map[string]any
		if json.Unmarshal([]byte(payload), &chunk) == nil {
			last = chunk
		}
	}
	return last
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
