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

// Package modelname implements a modelselector filter that restricts the
// candidate models to the model name(s) in the request body.
//
// For detailed behavioral intent and configuration, see the package README.
package modelname

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
)

const (
	// ModelNameFilterType is the registered name of the model-name filter plugin.
	ModelNameFilterType = "model-name-filter"

	// defaultRequestModelField is the request-body field inspected when none is configured.
	defaultRequestModelField = "model"
)

// compile-time type validation
var _ modelselector.Filter = &ModelNameFilter{}

// ModelNameFilterConfig defines the JSON configuration structure for the plugin.
type ModelNameFilterConfig struct {
	// RequestModelField is the request-body field that holds the requested model name.
	// Defaults to "model" when empty.
	RequestModelField string `json:"requestModelField,omitempty"`
}

// ModelNameFilterFactory defines the factory function for ModelNameFilter.
func ModelNameFilterFactory(name string, rawParameters json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	var config ModelNameFilterConfig

	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' filter - %w", ModelNameFilterType, err)
		}
	}

	return NewModelNameFilter(config.RequestModelField).WithName(name), nil
}

// NewModelNameFilter initializes a new ModelNameFilter and returns its pointer.
// An empty requestModelField defaults to "model".
func NewModelNameFilter(requestModelField string) *ModelNameFilter {
	if requestModelField == "" {
		requestModelField = defaultRequestModelField
	}

	return &ModelNameFilter{
		typedName:         plugin.TypedName{Type: ModelNameFilterType, Name: ModelNameFilterType},
		requestModelField: requestModelField,
	}
}

// ModelNameFilter restricts the candidate models to the model name(s) in the request body.
type ModelNameFilter struct {
	typedName         plugin.TypedName
	requestModelField string
}

// TypedName returns the type and name tuple of this plugin instance.
func (f *ModelNameFilter) TypedName() plugin.TypedName {
	return f.typedName
}

// WithName sets the name of the plugin instance.
func (f *ModelNameFilter) WithName(name string) *ModelNameFilter {
	f.typedName.Name = name
	return f
}

// Filter returns the candidate models whose name was requested by the request body.
func (f *ModelNameFilter) Filter(ctx context.Context, _ *plugin.CycleState, request *requesthandling.InferenceRequest, models []datalayer.Model) []datalayer.Model {
	logger := log.FromContext(ctx)

	requested, ok := requestBodyModelName(request.Body[f.requestModelField])
	if !ok {
		logger.V(logutil.VERBOSE).Info("malformed model field in request body, no available model candidates", "field", f.requestModelField)
		return nil
	}
	if requested.Len() == 0 {
		logger.V(logutil.VERBOSE).Info("no model in request body. All available models are considered as candidates", "field", f.requestModelField)
		return models
	}

	filtered := make([]datalayer.Model, 0, min(len(models), requested.Len()))
	kept := sets.New[string]()
	for _, model := range models {
		if requested.Has(model.GetName()) {
			filtered = append(filtered, model)
			kept.Insert(model.GetName())
		}
	}

	if dropped := requested.Difference(kept); dropped.Len() > 0 {
		logger.V(logutil.VERBOSE).Info("request model name(s) dropped, not available in the data store", "dropped", dropped.UnsortedList())
	}
	if len(filtered) == 0 {
		logger.V(logutil.VERBOSE).Info("request body model(s) are not configured", "requested", requested.UnsortedList())
	} else {
		logger.V(logutil.DEBUG).Info("model-name filter applied", "requested", requested.UnsortedList())
	}

	return filtered
}

// requestBodyModelName extracts the model name from a request-body model
// field, which must be a string: either a single model name, or — when
// starting with '[' — a JSON-encoded array of model names ("choose from the
// list"). An absent field (nil), an empty string, or an encoded empty array
// yield an empty set, meaning the request does not constrain the candidates.
// Any other shape — a non-string field, or a '['-prefixed string that does
// not parse as a JSON array of non-empty strings — is malformed and reported
// by the second return value being false.
func requestBodyModelName(raw any) (sets.Set[string], bool) {
	names := sets.New[string]()

	switch value := raw.(type) {
	case nil:
	case string:
		if strings.HasPrefix(strings.TrimSpace(value), "[") {
			return encodedModelNames(value)
		}
		if value != "" {
			names.Insert(value)
		}
	default:
		return nil, false
	}

	return names, true
}

// encodedModelNames parses a JSON-encoded array of non-empty model names out
// of a string value. A parse failure or an empty-string element is malformed,
// reported by the second return value being false.
func encodedModelNames(value string) (sets.Set[string], bool) {
	var parsed []string
	if err := json.Unmarshal([]byte(value), &parsed); err != nil {
		return nil, false
	}

	names := sets.New[string]()
	for _, name := range parsed {
		if name == "" {
			return nil, false
		}
		names.Insert(name)
	}

	return names, true
}
