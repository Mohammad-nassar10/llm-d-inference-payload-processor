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

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/datastore/inmemory"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
	inflightscorer "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/modelselector/scorer/inflightrequests"
	notificationsource "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/notificationsource"
	requestmetadata "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/requestmetadata"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/modelselector/picker/maxscore"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/requesthandling/bodyfieldtoheader"
	modelselectorsvc "github.com/llm-d/llm-d-inference-payload-processor/pkg/modelselector"
)

// newModelSelectorServer builds a Server wired with inflight-requests scorer + max-score picker.
// It returns the server and a datastore that tests can pre-populate.
func newModelSelectorServer(ctx context.Context, t *testing.T) (*Server, datalayer.Datastore) {
	t.Helper()
	ds := inmemory.NewDatastore()
	extractor := requestmetadata.NewRequestMetadataExtractor(ds)
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

	srv := NewServer([]requesthandling.RequestProcessor{modelToHeaderPlugin}, nil).
		WithModelSelector(profile, candidateModels).
		WithEventNotifier(notifSrc)

	return srv, ds
}

// setInflight directly sets the in-flight count on a model in the datastore.
func setInflight(ds datalayer.Datastore, name string, count int64) {
	m := ds.GetOrCreateModel(name)
	m.GetAttributes().Put(requestmetadata.RequestMetadataAttributeKey, requestmetadata.RequestMetadataCount{Requests: count})
}

// callHandleRequestBody calls HandleRequestBody with a JSON body containing the given model name.
func callHandleRequestBody(t *testing.T, srv *Server, model string) []*eppb.ProcessingResponse {
	t.Helper()
	ctx := context.Background()
	reqCtx := &RequestContext{
		Request:    requesthandling.NewInferenceRequest(),
		Response:   requesthandling.NewInferenceResponse(),
		CycleState: plugin.NewCycleState(),
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
