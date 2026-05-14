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

package runningrequests

import (
	"context"
	"encoding/json"
	"math"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/modelselector"
	plugindatalayer "github.com/llm-d/llm-d-inference-payload-processor/pkg/plugins/datalayer"
)

const RunningRequestsScorerPluginType = "running-requests-scorer"

// compile-time interface assertion
var _ modelselector.Scorer = &RunningRequestsScorer{}

// RunningRequestsScorer scores models based on their in-flight request count.
// The least loaded model scores 1.0; the most loaded scores 0.0.
// Models without a "running-requests" attribute are treated as idle (0 requests).
// If all models have the same count, all score 1.0.
type RunningRequestsScorer struct {
	name framework.TypedName
}

// RunningRequestsScorerFactory is the factory function for RunningRequestsScorer.
func RunningRequestsScorerFactory(name string, _ json.RawMessage, _ framework.Handle) (framework.Plugin, error) {
	return NewRunningRequestsScorer().WithName(name), nil
}

// NewRunningRequestsScorer initializes a new RunningRequestsScorer and returns its pointer.
func NewRunningRequestsScorer() *RunningRequestsScorer {
	return &RunningRequestsScorer{
		name: framework.TypedName{Type: RunningRequestsScorerPluginType, Name: RunningRequestsScorerPluginType},
	}
}

// TypedName returns the type and name tuple of this plugin instance.
func (s *RunningRequestsScorer) TypedName() framework.TypedName { return s.name }

// WithName sets the instance name.
func (s *RunningRequestsScorer) WithName(name string) *RunningRequestsScorer {
	s.name.Name = name
	return s
}

// Score returns a score in [0,1] for each model based on its running request count.
// Formula: score = (max - count) / (max - min)
func (s *RunningRequestsScorer) Score(_ context.Context, _ *framework.CycleState, _ *framework.InferenceRequest, models []datalayer.Model) map[datalayer.Model]float64 {
	var minCount int64 = math.MaxInt64
	var maxCount int64 = math.MinInt64

	counts := make(map[datalayer.Model]int64, len(models))
	for _, m := range models {
		n := runningRequests(m)
		counts[m] = n
		if n < minCount {
			minCount = n
		}
		if n > maxCount {
			maxCount = n
		}
	}

	scores := make(map[datalayer.Model]float64, len(models))
	for _, m := range models {
		if maxCount == minCount {
			scores[m] = 1.0
		} else {
			scores[m] = float64(maxCount-counts[m]) / float64(maxCount-minCount)
		}
	}
	return scores
}

// runningRequests returns the in-flight request count for a model.
// Returns 0 if the attribute is missing or has an unexpected type.
func runningRequests(m datalayer.Model) int64 {
	val, ok := m.GetAttributes().Get("running-requests")
	if !ok {
		return 0
	}
	rc, ok := val.(plugindatalayer.RunningRequestsCount)
	if !ok {
		return 0
	}
	return rc.Requests
}
