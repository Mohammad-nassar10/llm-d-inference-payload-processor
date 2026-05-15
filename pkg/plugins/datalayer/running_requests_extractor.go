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

package datalayer

import (
	"context"
	"encoding/json"

	fwdatalayer "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datalayer"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework"
)

const RunningRequestsExtractorPluginType = "running-requests-extractor"

// compile-time interface assertion
var _ framework.Extractor = &RunningRequestsExtractor{}

// RunningRequestsExtractorFactory is the factory function for RunningRequestsExtractor.
func RunningRequestsExtractorFactory(name string, _ json.RawMessage, _ framework.Handle) (framework.Plugin, error) {
	return NewRunningRequestsExtractor().WithName(name), nil
}

// RunningRequestsCount holds in-flight request and token counts for one model.
type RunningRequestsCount struct {
	Requests int64
	Tokens   int64
}

func (r RunningRequestsCount) Clone() fwdatalayer.Cloneable { return r }

// RunningRequestsExtractor tracks in-flight request counts and token sums per model.
// It writes RunningRequestsCount to each model's "running-requests" attribute.
//
// Extract is assumed to be called from a single goroutine (the NotificationSource event loop).
// If parallel dispatch is introduced, add a sync.Mutex around counters and the DataStore write.
//
// TODO: counters leak if a request fails without a corresponding ResponseEventType (e.g. connection
// drop, upstream error, context cancellation). The call site should fire a
// synthetic ResponseEventType in its error/EOF path to keep counts accurate.
type RunningRequestsExtractor struct {
	name     framework.TypedName
	counters map[string]RunningRequestsCount
}

func NewRunningRequestsExtractor() *RunningRequestsExtractor {
	return &RunningRequestsExtractor{
		name:     framework.TypedName{Type: RunningRequestsExtractorPluginType, Name: RunningRequestsExtractorPluginType},
		counters: make(map[string]RunningRequestsCount),
	}
}

func (e *RunningRequestsExtractor) TypedName() framework.TypedName { return e.name }

// WithName sets the instance name, used by the factory when the plugin is configured by name.
func (e *RunningRequestsExtractor) WithName(name string) *RunningRequestsExtractor {
	e.name.Name = name
	return e
}

func (e *RunningRequestsExtractor) Extract(_ context.Context, ds framework.DataStore, events []framework.Event) error {
	updated := map[string]RunningRequestsCount{}

	for _, ev := range events {
		switch ev.Type {
		case framework.RequestEventType:
			p, ok := ev.Payload.(framework.RequestPayload)
			if !ok {
				continue
			}
			model, _ := p.Request.Body["model"].(string)
			if model == "" {
				continue
			}
			maxTokens, _ := p.Request.Body["max_tokens"].(float64)
			c := e.counters[model]
			c.Requests++
			c.Tokens += int64(maxTokens)
			e.counters[model] = c
			updated[model] = c

		case framework.ResponseEventType:
			p, ok := ev.Payload.(framework.ResponsePayload)
			if !ok {
				continue
			}
			model, _ := p.Request.Body["model"].(string)
			if model == "" {
				continue
			}
			maxTokens, _ := p.Request.Body["max_tokens"].(float64)
			c := e.counters[model]
			floorDecrement(&c.Requests, 1)
			floorDecrement(&c.Tokens, int64(maxTokens))
			e.counters[model] = c
			updated[model] = c
		}
	}

	for model, c := range updated {
		ds.GetOrCreateModel(model).GetAttributes().Put("running-requests", c)
	}
	return nil
}

// floorDecrement decrements v by delta, flooring at zero.
func floorDecrement(v *int64, delta int64) {
	*v -= delta
	if *v < 0 {
		*v = 0
	}
}
