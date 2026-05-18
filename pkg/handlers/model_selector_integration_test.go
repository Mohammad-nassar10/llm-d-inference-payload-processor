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
	"testing"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/datastore"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datalayer"
	inflightscorer "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/modelselector/scorer/inflightrequests"
	modelselectorsvc "github.com/llm-d/llm-d-inference-payload-processor/pkg/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/modelselector/picker/maxscore"
	inflightrequests "github.com/llm-d/llm-d-inference-payload-processor/pkg/plugins/datalayer/inflightrequests"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/plugins/datalayer/notificationsource"
)

// TestModelSelectorIntegration verifies the end-to-end flow:
// data layer events → DataStore → InflightRequestsScorer → MaxScorePicker → x-selected-model header.
func TestModelSelectorIntegration(t *testing.T) {
	t.Run("selects least loaded model", func(t *testing.T) {
		ctx, cancel := context.WithCancel(logutil.NewTestLoggerIntoContext(context.Background()))
		defer cancel()

		ds, srv := newModelSelectorServer(t, ctx)

		// llama-3 is heavily loaded, gpt-4 is lightly loaded
		fireAndWait(t, srv.eventNotifier, ds, "llama-3", 5)
		fireAndWait(t, srv.eventNotifier, ds, "gpt-4", 1)

		responses := callHandleRequestBody(t, ctx, srv, `{"model":"any","prompt":"hello"}`)
		if got := findSetHeader(responses, selectedModelHeader); got != "gpt-4" {
			t.Errorf("expected x-selected-model=gpt-4 (least loaded), got %q", got)
		}
	})

	t.Run("no candidates skips selection gracefully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(logutil.NewTestLoggerIntoContext(context.Background()))
		defer cancel()

		_, srv := newModelSelectorServer(t, ctx)

		// DataStore is empty — candidateModels() returns nothing
		responses := callHandleRequestBody(t, ctx, srv, `{"model":"any","prompt":"hello"}`)
		if got := findSetHeader(responses, selectedModelHeader); got != "" {
			t.Errorf("expected no %s header with empty DataStore, got %q", selectedModelHeader, got)
		}
	})

	t.Run("equal load selects one of the candidates", func(t *testing.T) {
		ctx, cancel := context.WithCancel(logutil.NewTestLoggerIntoContext(context.Background()))
		defer cancel()

		ds, srv := newModelSelectorServer(t, ctx)

		// Both models carry identical load — any selection is valid
		fireAndWait(t, srv.eventNotifier, ds, "llama-3", 2)
		fireAndWait(t, srv.eventNotifier, ds, "gpt-4", 2)

		responses := callHandleRequestBody(t, ctx, srv, `{"model":"any","prompt":"hello"}`)
		got := findSetHeader(responses, selectedModelHeader)
		if got != "llama-3" && got != "gpt-4" {
			t.Errorf("expected x-selected-model to be llama-3 or gpt-4, got %q", got)
		}
	})
}

// newModelSelectorServer builds the full stack and returns the DataStore and wired Server.
func newModelSelectorServer(t *testing.T, ctx context.Context) (datastore.Datastore, *Server) {
	t.Helper()

	ds := datastore.NewStore()

	notifSrc, err := notificationsource.New("test", inflightrequests.NewInflightRequestsExtractor(ds))
	if err != nil {
		t.Fatalf("notificationsource.New: %v", err)
	}
	if err := notifSrc.Start(ctx); err != nil {
		t.Fatalf("notifSrc.Start: %v", err)
	}
	t.Cleanup(notifSrc.Stop)

	profile := modelselectorsvc.NewModelSelectorProfile().
		WithScorers(modelselectorsvc.NewWeightedScorer(inflightscorer.NewInflightRequestsScorer(), 1.0)).
		WithPicker(maxscore.NewMaxScorePicker())

	candidateModels := func() []datalayer.Model {
		names := ds.Models()
		models := make([]datalayer.Model, 0, len(names))
		for _, name := range names {
			models = append(models, ds.GetOrCreateModel(name))
		}
		return models
	}

	srv := NewServer(nil, nil).
		WithModelSelector(profile, candidateModels).
		WithEventNotifier(notifSrc)

	return ds, srv
}

// fireAndWait fires count RequestEvents for the given model and blocks until
// the DataStore reflects the expected in-flight count.
func fireAndWait(t *testing.T, notifier datalayer.EventNotifier, ds datalayer.DataStore, model string, count int) {
	t.Helper()
	req := framework.NewInferenceRequest()
	req.Body["model"] = model
	event := datalayer.Event{Type: datalayer.RequestEventType, Payload: datalayer.RequestPayload{Request: req}}
	for i := 0; i < count; i++ {
		notifier.Notify(event)
	}
	deadline := time.After(2 * time.Second)
	for {
		val, ok := ds.GetOrCreateModel(model).GetAttributes().Get(inflightrequests.InflightRequestsAttributeKey)
		if ok {
			if rc, ok := val.(inflightrequests.InflightRequestsCount); ok && rc.Requests == int64(count) {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: model %q did not reach %d in-flight requests", model, count)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// callHandleRequestBody calls HandleRequestBody and returns the responses.
func callHandleRequestBody(t *testing.T, ctx context.Context, srv *Server, body string) []*extProcPb.ProcessingResponse {
	t.Helper()
	reqCtx := &RequestContext{
		CycleState: framework.NewCycleState(),
		Request:    framework.NewInferenceRequest(),
	}
	responses, err := srv.HandleRequestBody(ctx, reqCtx, []byte(body))
	if err != nil {
		t.Fatalf("HandleRequestBody: %v", err)
	}
	return responses
}

// findSetHeader extracts a set header value from ProcessingResponses, or "" if not found.
func findSetHeader(responses []*extProcPb.ProcessingResponse, key string) string {
	for _, resp := range responses {
		if rh := resp.GetRequestHeaders(); rh != nil {
			for _, h := range rh.GetResponse().GetHeaderMutation().GetSetHeaders() {
				if h.GetHeader().GetKey() == key {
					return string(h.GetHeader().GetRawValue())
				}
			}
		}
	}
	return ""
}
