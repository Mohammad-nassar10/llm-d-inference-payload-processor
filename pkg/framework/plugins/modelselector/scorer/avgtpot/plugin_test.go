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

package avgtpot

import (
	"context"
	"testing"
	"time"

	fwdatalayer "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	requestmetadata "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/requestmetadata"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/modelselector/scorer/internal/decay"
)

func modelWithAvgTPOT(name string, avgTPOT float64) fwdatalayer.Model {
	model := fwdatalayer.NewModel(name)
	model.GetAttributes().Put(requestmetadata.RequestMetadataAttributeKey, requestmetadata.ModelMetrics{
		AvgTPOT: avgTPOT,
	})
	return model
}

func modelWithNoAttribute(name string) fwdatalayer.Model {
	return fwdatalayer.NewModel(name)
}

func modelWithMetrics(name string, requests int64, lastObservedAt time.Time) fwdatalayer.Model {
	model := fwdatalayer.NewModel(name)
	model.GetAttributes().Put(requestmetadata.RequestMetadataAttributeKey, requestmetadata.ModelMetrics{
		AvgTPOT:        0.1,
		Requests:       requests,
		LastObservedAt: lastObservedAt.UnixNano(),
	})
	return model
}

func TestAvgTPOTScorer(t *testing.T) {
	scorer := NewAvgTPOTScorer()

	tests := []struct {
		name           string
		models         []fwdatalayer.Model
		expectedScores []float64
	}{
		{
			name: "lower TPOT gets higher score",
			models: []fwdatalayer.Model{
				modelWithAvgTPOT("fast", 0.02),
				modelWithAvgTPOT("slow", 0.1),
			},
			expectedScores: []float64{1.0, 0.0},
		},
		{
			name: "equal TPOT — all score 1.0",
			models: []fwdatalayer.Model{
				modelWithAvgTPOT("m1", 0.05),
				modelWithAvgTPOT("m2", 0.05),
			},
			expectedScores: []float64{1.0, 1.0},
		},
		{
			name: "no attribute scores optimistically (treated as 0)",
			models: []fwdatalayer.Model{
				modelWithAvgTPOT("observed", 0.05),
				modelWithNoAttribute("unobserved"),
			},
			expectedScores: []float64{0.0, 1.0},
		},
		{
			name: "three models — intermediate score is normalised",
			// min=0.25, max=0.75; middle=0.5 → (0.75-0.5)/(0.75-0.25) = 0.5
			// 0.25, 0.5, 0.75 are exact in float64 so the comparison is safe without epsilon.
			models: []fwdatalayer.Model{
				modelWithAvgTPOT("fast", 0.25),
				modelWithAvgTPOT("mid", 0.5),
				modelWithAvgTPOT("slow", 0.75),
			},
			expectedScores: []float64{1.0, 0.5, 0.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scores := scorer.Score(context.Background(), nil, nil, tt.models)
			for i, model := range tt.models {
				got := scores[model]
				want := tt.expectedScores[i]
				if got != want {
					t.Errorf("model[%d] %q: expected score %f, got %f", i, model.GetName(), want, got)
				}
			}
		})
	}
}

// TestStalenessDecay verifies the decay recovers a stale idle model.
func TestStalenessDecay(t *testing.T) {
	scorer := NewAvgTPOTScorer()
	now := time.Now()

	// LastObservedAt = 60s ago (2× threshold), Requests=0 → decay=1.0 → effective TPOT=0
	stale := modelWithMetrics("stale", 0, now.Add(-60*time.Second))
	other := modelWithAvgTPOT("other", 0.05)
	scores := scorer.Score(context.Background(), nil, nil, []fwdatalayer.Model{stale, other})
	if scores[stale] != 1.0 {
		t.Errorf("fully stale idle model: expected score 1.0, got %f", scores[stale])
	}
}

// TestDecayDisabled verifies DecayWeight=0 ignores staleness entirely.
func TestDecayDisabled(t *testing.T) {
	scorer := NewAvgTPOTScorer().WithDecay(decay.Config{Weight: 0, Threshold: 30 * time.Second})
	now := time.Now()

	stale := modelWithMetrics("stale", 0, now.Add(-60*time.Second))
	other := modelWithAvgTPOT("other", 0.05)
	scores := scorer.Score(context.Background(), nil, nil, []fwdatalayer.Model{stale, other})
	if scores[stale] != 0.0 {
		t.Errorf("decay-disabled stale model: expected score 0.0 (raw EMA), got %f", scores[stale])
	}
	if scores[other] != 1.0 {
		t.Errorf("decay-disabled other model: expected score 1.0, got %f", scores[other])
	}
}
