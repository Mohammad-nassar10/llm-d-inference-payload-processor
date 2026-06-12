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

package modelname

import (
	"context"
	"encoding/json"
	"sort"
	"testing"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
)

// candidateModels builds the datalayer models handed to the filter, simulating
// the models configured in the data store.
func candidateModels(names ...string) []datalayer.Model {
	models := make([]datalayer.Model, 0, len(names))
	for _, n := range names {
		models = append(models, datalayer.NewModel(n))
	}
	return models
}

// names extracts the sorted model names of a filter result, for order-insensitive
// comparison against the expected names.
func names(models []datalayer.Model) []string {
	out := make([]string, 0, len(models))
	for _, m := range models {
		out = append(out, m.GetName())
	}
	sort.Strings(out)
	return out
}

// requestWithModel builds an inference request whose body holds the given value
// under the given field; a nil value leaves the field absent.
func requestWithModel(field string, value any) *requesthandling.InferenceRequest {
	r := requesthandling.NewInferenceRequest()
	if value != nil {
		r.Body[field] = value
	}
	return r
}

// TestModelNameFilterFactory verifies the factory's config handling: empty
// parameters fall back to the default "model" field, a configured
// requestModelField is honored, invalid JSON is rejected with an error, and
// the created plugin carries the instance name and registered type.
func TestModelNameFilterFactory(t *testing.T) {
	tests := []struct {
		name       string
		pluginName string
		rawParams  json.RawMessage
		wantErr    bool
		wantField  string
	}{
		// Omitting the parameters entirely must fall back to inspecting the
		// default "model" body field.
		{
			name:       "empty params defaults to model field",
			pluginName: "my-filter",
			rawParams:  json.RawMessage(``),
			wantField:  defaultRequestModelField,
		},
		// A configured requestModelField must replace the default field.
		{
			name:       "custom model field",
			pluginName: "my-filter",
			rawParams:  json.RawMessage(`{"requestModelField":"requestedModel"}`),
			wantField:  "requestedModel",
		},
		// Parameters that are not valid JSON must fail plugin creation.
		{
			name:       "invalid JSON",
			pluginName: "my-filter",
			rawParams:  json.RawMessage(`{invalid`),
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ModelNameFilterFactory(tt.pluginName, tt.rawParams, nil)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			f := p.(*ModelNameFilter)
			if f.requestModelField != tt.wantField {
				t.Errorf("requestModelField = %s, want %s", f.requestModelField, tt.wantField)
			}
			if got := f.TypedName().Name; got != tt.pluginName {
				t.Errorf("Name = %s, want %s", got, tt.pluginName)
			}
			if got := f.TypedName().Type; got != ModelNameFilterType {
				t.Errorf("Type = %s, want %s", got, ModelNameFilterType)
			}
		})
	}
}

// TestModelNameFilter_Filter verifies the filtering semantics for every shape
// the request-body model field can take: a configured name pins the candidates
// to that model, a string-encoded JSON array keeps only its configured subset,
// an absent or empty field passes all candidates through, and an unconfigured
// name or malformed field (non-string type, unparsable or non-string-array
// encoded value) yields an empty result, which the pipeline turns into a
// request error.
func TestModelNameFilter_Filter(t *testing.T) {
	registered := []string{"qwen3", "llama3", "mistral"}

	tests := []struct {
		name      string
		modelBody any // value stored at request.Body["model"]
		want      []string
	}{
		// A model name that is configured in the data store pins the
		// candidates to that single model.
		{
			name:      "single registered model keeps only it",
			modelBody: "qwen3",
			want:      []string{"qwen3"},
		},
		// A model name that is not configured eliminates all candidates;
		// the pipeline rejects the request.
		{
			name:      "single unregistered model yields empty (pipeline error)",
			modelBody: "gpt-4",
			want:      []string{},
		},
		// An absent model field does not constrain the request; every
		// configured model remains a candidate.
		{
			name:      "missing model field passes all through",
			modelBody: nil,
			want:      registered,
		},
		// An empty-string model name is treated like an absent field.
		{
			name:      "empty string model passes all through",
			modelBody: "",
			want:      registered,
		},
		// A model field of a non-string type is malformed and eliminates
		// all candidates rather than being silently ignored.
		{
			name:      "non-string model field yields empty (malformed)",
			modelBody: 42,
			want:      []string{},
		},
		// A real JSON array is no longer accepted — the model field must be
		// a string; the list form is expressed as a string-encoded array.
		{
			name:      "JSON array model field yields empty (malformed)",
			modelBody: []any{"qwen3", "mistral"},
			want:      []string{},
		},
		// A string holding a JSON-encoded array is "choose from the list":
		// configured names are kept as candidates, unconfigured ones dropped.
		{
			name:      "string-encoded array keeps only the registered ones",
			modelBody: `["qwen3", "mistral", "gpt-4"]`,
			want:      []string{"mistral", "qwen3"},
		},
		// Leading whitespace before the encoded array is tolerated.
		{
			name:      "string-encoded array with leading whitespace is parsed",
			modelBody: `  ["qwen3"]`,
			want:      []string{"qwen3"},
		},
		// A string-encoded empty array is treated like an absent field.
		{
			name:      "string-encoded empty array passes all through",
			modelBody: `[]`,
			want:      registered,
		},
		// A '['-prefixed string that is not parsable JSON is malformed and
		// eliminates all candidates.
		{
			name:      "string-encoded array with invalid JSON yields empty (malformed)",
			modelBody: `["qwen3"`,
			want:      []string{},
		},
		// Valid JSON that is not an array of strings (here: a number element)
		// is malformed as a whole.
		{
			name:      "string-encoded array with non-string element yields empty (malformed)",
			modelBody: `["qwen3", 42]`,
			want:      []string{},
		},
		// A string-encoded array holding an empty-string element is malformed.
		{
			name:      "string-encoded array with empty element yields empty (malformed)",
			modelBody: `["qwen3", ""]`,
			want:      []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewModelNameFilter("")
			req := requestWithModel(defaultRequestModelField, tt.modelBody)

			got := names(f.Filter(context.Background(), nil, req, candidateModels(registered...)))

			want := append([]string{}, tt.want...)
			sort.Strings(want)
			if len(got) != len(want) {
				t.Fatalf("Filter() = %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("Filter() = %v, want %v", got, want)
					break
				}
			}
		})
	}
}

// TestModelNameFilter_CustomModelField verifies that the filter reads the model
// name from the configured requestModelField instead of the default "model".
func TestModelNameFilter_CustomModelField(t *testing.T) {
	f := NewModelNameFilter("requestedModel")
	req := requestWithModel("requestedModel", "llama3")

	got := names(f.Filter(context.Background(), nil, req, candidateModels("qwen3", "llama3")))
	if len(got) != 1 || got[0] != "llama3" {
		t.Errorf("Filter() = %v, want [llama3]", got)
	}
}
