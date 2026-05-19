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

	eppb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/datastore"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/plugins/bodyfieldtoheader"
	inflightscorer "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/modelselector/scorer/inflightrequests"
	modelselectorsvc "github.com/llm-d/llm-d-inference-payload-processor/pkg/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/modelselector/picker/maxscore"
	inflightrequests "github.com/llm-d/llm-d-inference-payload-processor/pkg/plugins/datalayer/inflightrequests"
	notificationsource "github.com/llm-d/llm-d-inference-payload-processor/pkg/plugins/datalayer/notificationsource"
)

// newModelSelectorServer builds a Server wired with inflight-requests scorer + max-score picker.
// It returns the server and a datastore that tests can pre-populate.
func newModelSelectorServer(ctx context.Context, t *testing.T) (*Server, datastore.Datastore) {
	t.Helper()
	ds := datastore.NewStore()
	extractor := inflightrequests.NewInflightRequestsExtractor(ds)
	notifSrc, err := notificationsource.New("test", extractor)
	if err != nil {
		t.Fatalf("failed to create notification source: %v", err)
	}
	if err := notifSrc.Start(ctx); err != nil {
		t.Fatalf("failed to start notification source: %v", err)
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

	modelToHeaderPlugin, err := bodyfieldtoheader.NewBodyFieldToHeaderPlugin("model", bodyfieldtoheader.ModelHeader)
	if err != nil {
		t.Fatalf("failed to create bodyfieldtoheader plugin: %v", err)
	}

	srv := NewServer([]framework.RequestProcessor{modelToHeaderPlugin}, nil).
		WithModelSelector(profile, candidateModels).
		WithEventNotifier(notifSrc)

	return srv, ds
}

// setInflight directly sets the in-flight count on a model in the datastore.
func setInflight(ds datastore.Datastore, name string, count int64) {
	m := ds.GetOrCreateModel(name)
	m.GetAttributes().Put(inflightrequests.InflightRequestsAttributeKey, inflightrequests.InflightRequestsCount{Requests: count})
}

// callHandleRequestBody calls HandleRequestBody with a JSON body containing the given model name.
func callHandleRequestBody(t *testing.T, srv *Server, model string) []*eppb.ProcessingResponse {
	t.Helper()
	ctx := context.Background()
	reqCtx := &RequestContext{
		Request:    framework.NewInferenceRequest(),
		Response:   framework.NewInferenceResponse(),
		CycleState: framework.NewCycleState(),
	}
	body := []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"hello"}]}`)
	responses, err := srv.HandleRequestBody(ctx, reqCtx, body)
	if err != nil {
		t.Fatalf("HandleRequestBody failed: %v", err)
	}
	return responses
}

// findSetHeader searches a slice of ProcessingResponses for a set-header mutation
// and returns the value for the given key, or empty string if not found.
func findSetHeader(responses []*eppb.ProcessingResponse, key string) string {
	for _, r := range responses {
		rh, ok := r.Response.(*eppb.ProcessingResponse_RequestHeaders)
		if !ok {
			continue
		}
		if rh.RequestHeaders.GetResponse().GetHeaderMutation() == nil {
			continue
		}
		for _, h := range rh.RequestHeaders.Response.HeaderMutation.SetHeaders {
			if h.GetHeader().GetKey() == key {
				return string(h.GetHeader().GetRawValue())
			}
		}
	}
	return ""
}

func TestModelSelectorIntegration(t *testing.T) {
	t.Run("selects model with fewest in-flight requests", func(t *testing.T) {
		ctx := context.Background()
		srv, ds := newModelSelectorServer(ctx, t)

		// llama-3 has 5 in-flight; gpt-4 has 1 — expect gpt-4 to be selected.
		setInflight(ds, "llama-3", 5)
		setInflight(ds, "gpt-4", 1)

		responses := callHandleRequestBody(t, srv, "llama-3")

		selected := findSetHeader(responses, bodyfieldtoheader.ModelHeader)
		if selected != "gpt-4" {
			t.Errorf("expected x-selected-model=gpt-4, got %q", selected)
		}
	})

	t.Run("selects the only available model", func(t *testing.T) {
		ctx := context.Background()
		srv, ds := newModelSelectorServer(ctx, t)

		setInflight(ds, "llama-3", 0)

		responses := callHandleRequestBody(t, srv, "llama-3")

		selected := findSetHeader(responses, bodyfieldtoheader.ModelHeader)
		if selected != "llama-3" {
			t.Errorf("expected x-selected-model=llama-3, got %q", selected)
		}
	})

	t.Run("no candidate models: falls back to original requested model", func(t *testing.T) {
		ctx := context.Background()
		srv, _ := newModelSelectorServer(ctx, t) //nolint:dogsled
		// Datastore is empty — selector is skipped, bodyfieldtoheader routes the original model.

		responses := callHandleRequestBody(t, srv, "llama-3")

		selected := findSetHeader(responses, bodyfieldtoheader.ModelHeader)
		if selected != "llama-3" {
			t.Errorf("expected x-gateway-model-name=llama-3 (fallback), got %q", selected)
		}
	})
}
