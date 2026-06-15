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

package avgttft

import (
	"context"
	"testing"
	"time"

	fwdatalayer "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	requestmetadata "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/requestmetadata"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/modelselector/scorer/internal/decay"
)

func modelWithAvgTTFT(name string, avgTTFT float64) fwdatalayer.Model {
	model := fwdatalayer.NewModel(name)
	model.GetAttributes().Put(requestmetadata.RequestMetadataAttributeKey, requestmetadata.ModelMetrics{
		AvgTTFT: avgTTFT,
	})
	return model
}

func modelWithNoAttribute(name string) fwdatalayer.Model {
	return fwdatalayer.NewModel(name)
}

func modelWithMetrics(name string, requests int64, lastObservedAt time.Time) fwdatalayer.Model {
	model := fwdatalayer.NewModel(name)
	model.GetAttributes().Put(requestmetadata.RequestMetadataAttributeKey, requestmetadata.ModelMetrics{
		AvgTTFT:        1.0,
		Requests:       requests,
		LastObservedAt: lastObservedAt.UnixNano(),
	})
	return model
}

func modelWithFullMetrics(name string, m requestmetadata.ModelMetrics) fwdatalayer.Model {
	model := fwdatalayer.NewModel(name)
	model.GetAttributes().Put(requestmetadata.RequestMetadataAttributeKey, m)
	return model
}

func TestAvgTTFTScorer(t *testing.T) {
	scorer := NewAvgTTFTScorer()

	tests := []struct {
		name           string
		models         []fwdatalayer.Model
		expectedScores []float64
	}{
		{
			name: "lower TTFT gets higher score",
			models: []fwdatalayer.Model{
				modelWithAvgTTFT("fast", 0.2),
				modelWithAvgTTFT("slow", 1.0),
			},
			expectedScores: []float64{1.0, 0.0},
		},
		{
			name: "equal TTFT — all score 1.0",
			models: []fwdatalayer.Model{
				modelWithAvgTTFT("m1", 0.5),
				modelWithAvgTTFT("m2", 0.5),
			},
			expectedScores: []float64{1.0, 1.0},
		},
		{
			name: "no attribute scores optimistically (treated as 0)",
			models: []fwdatalayer.Model{
				modelWithAvgTTFT("observed", 0.5),
				modelWithNoAttribute("unobserved"),
			},
			expectedScores: []float64{0.0, 1.0},
		},
		{
			name: "three models — intermediate score is normalised",
			// min=0.2, max=1.0; middle=0.6 → (1.0-0.6)/(1.0-0.2) = 0.5
			models: []fwdatalayer.Model{
				modelWithAvgTTFT("fast", 0.2),
				modelWithAvgTTFT("mid", 0.6),
				modelWithAvgTTFT("slow", 1.0),
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

// TestStalenessDecay verifies the decay formula for recovering models.
func TestStalenessDecay(t *testing.T) {
	scorer := NewAvgTTFTScorer()
	now := time.Now()

	t.Run("fresh model — no decay applied", func(t *testing.T) {
		// LastObservedAt = now → staleness = 0 → effective TTFT = raw AvgTTFT
		fresh := modelWithMetrics("fresh", 0, now)
		other := modelWithNoAttribute("other") // AvgTTFT=0, scores 1.0
		scores := scorer.Score(context.Background(), nil, nil, []fwdatalayer.Model{fresh, other})
		if scores[fresh] != 0.0 {
			t.Errorf("fresh model: expected score 0.0, got %f", scores[fresh])
		}
	})

	t.Run("fully stale idle model — full decay, scores 1.0", func(t *testing.T) {
		// LastObservedAt = 60s ago (2× threshold), Requests=0 → decay=1.0 → effective TTFT=0
		stale := modelWithMetrics("stale", 0, now.Add(-60*time.Second))
		other := modelWithAvgTTFT("other", 0.5)
		scores := scorer.Score(context.Background(), nil, nil, []fwdatalayer.Model{stale, other})
		if scores[stale] != 1.0 {
			t.Errorf("fully stale idle model: expected score 1.0, got %f", scores[stale])
		}
	})

}

// TestExcessPenaltyFlipsWinner verifies that a burst above AvgRequests raises predictedTTFT
// enough to flip the winner. fast has lower AvgTTFT but carries excess load; slow is idle.
//
//	fast: AvgTTFT=0.2s, Requests=5, AvgRequests=0 → excess=5 → predicted = 0.2×(1+1×5) = 1.2s
//	slow: AvgTTFT=1.0s, Requests=0, AvgRequests=0 → excess=0 → predicted = 1.0s
//	1.2 > 1.0 → slow wins despite higher base TTFT.
func TestExcessPenaltyFlipsWinner(t *testing.T) {
	scorer := NewAvgTTFTScorer().WithInflight(1.0)
	fast := modelWithFullMetrics("fast", requestmetadata.ModelMetrics{
		AvgTTFT: 0.2, Requests: 5, AvgRequests: 0,
	})
	slow := modelWithFullMetrics("slow", requestmetadata.ModelMetrics{
		AvgTTFT: 1.0, Requests: 0, AvgRequests: 0,
	})
	scores := scorer.Score(context.Background(), nil, nil, []fwdatalayer.Model{fast, slow})
	if scores[fast] != 0.0 {
		t.Errorf("fast (overloaded): expected score 0.0, got %f", scores[fast])
	}
	if scores[slow] != 1.0 {
		t.Errorf("slow (idle): expected score 1.0, got %f", scores[slow])
	}
}

// TestSteadyStateNoExcessPenalty verifies that excess=0 when requests==AvgRequests,
// so the fast model still wins at its normal operating load.
//
//	loaded: AvgTTFT=0.2s, Requests=5, AvgRequests=5 → excess=0 → predicted = 0.2s
//	idle:   AvgTTFT=1.0s, Requests=0, AvgRequests=0 → excess=0 → predicted = 1.0s
func TestSteadyStateNoExcessPenalty(t *testing.T) {
	scorer := NewAvgTTFTScorer().WithInflight(1.0)
	loaded := modelWithFullMetrics("loaded", requestmetadata.ModelMetrics{
		AvgTTFT: 0.2, Requests: 5, AvgRequests: 5,
	})
	idle := modelWithFullMetrics("idle", requestmetadata.ModelMetrics{
		AvgTTFT: 1.0, Requests: 0, AvgRequests: 0,
	})
	scores := scorer.Score(context.Background(), nil, nil, []fwdatalayer.Model{loaded, idle})
	if scores[loaded] != 1.0 {
		t.Errorf("loaded (no excess): expected score 1.0, got %f", scores[loaded])
	}
	if scores[idle] != 0.0 {
		t.Errorf("idle: expected score 0.0, got %f", scores[idle])
	}
}

// TestMaxIdleProbesAllowsDecay verifies that raising MaxIdleProbes permits staleness
// decay while the model carries a small probe burst. With MaxIdleProbes=2 and requests=2
// the decay still applies; with MaxIdleProbes=0 it does not.
func TestMaxIdleProbesAllowsDecay(t *testing.T) {
	now := time.Now()
	staleAt := now.Add(-60 * time.Second) // well beyond any threshold

	// Scorer A: MaxIdleProbes=0 (default) — busy guard stops decay at requests=2.
	scorerNoProbes := NewAvgTTFTScorer().
		WithDecay(decay.Config{Weight: 1.0, Threshold: 5 * time.Second, MaxIdleProbes: 0}).
		WithInflight(0)

	// Scorer B: MaxIdleProbes=2 — decay allowed while requests <= 2.
	scorerWithProbes := NewAvgTTFTScorer().
		WithDecay(decay.Config{Weight: 1.0, Threshold: 5 * time.Second, MaxIdleProbes: 2}).
		WithInflight(0)

	// Model with requests=2 and stale observations.
	stale := modelWithMetrics("stale", 2, staleAt)
	other := modelWithAvgTTFT("other", 0.5)
	models := []fwdatalayer.Model{stale, other}

	// Without MaxIdleProbes: requests=2 > 0 → decay suppressed → stale keeps its TTFT → scores 0.
	scoresNoProbes := scorerNoProbes.Score(context.Background(), nil, nil, models)
	if scoresNoProbes[stale] != 0.0 {
		t.Errorf("MaxIdleProbes=0: stale model expected score 0.0 (decay suppressed), got %f", scoresNoProbes[stale])
	}

	// With MaxIdleProbes=2: requests=2 <= 2 → decay applies → TTFT decays to 0 → scores 1.0.
	scoresWithProbes := scorerWithProbes.Score(context.Background(), nil, nil, models)
	if scoresWithProbes[stale] != 1.0 {
		t.Errorf("MaxIdleProbes=2: stale model expected score 1.0 (decay applied), got %f", scoresWithProbes[stale])
	}
}

// TestDecayDisabled verifies DecayWeight=0 ignores staleness entirely.
func TestDecayDisabled(t *testing.T) {
	scorer := NewAvgTTFTScorer().WithDecay(decay.Config{Weight: 0, Threshold: 30 * time.Second})
	now := time.Now()

	// With decay off, the stale model keeps its raw TTFT and scores 0.0 (not 1.0).
	stale := modelWithMetrics("stale", 0, now.Add(-60*time.Second))
	other := modelWithAvgTTFT("other", 0.5)
	scores := scorer.Score(context.Background(), nil, nil, []fwdatalayer.Model{stale, other})
	if scores[stale] != 0.0 {
		t.Errorf("decay-disabled stale model: expected score 0.0 (raw EMA), got %f", scores[stale])
	}
	if scores[other] != 1.0 {
		t.Errorf("decay-disabled other model: expected score 1.0, got %f", scores[other])
	}
}
