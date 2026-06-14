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
	"encoding/json"
	"fmt"
	"math"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
	requestmetadata "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/requestmetadata"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/modelselector/scorer/internal/decay"
)

const (
	PluginType = "avg-ttft-scorer"

	defaultDecayWeight        = 1.0
	defaultStalenessThreshold = 30 * time.Second
	defaultInflightWeight     = 1.0
)

// compile-time interface assertion
var _ modelselector.Scorer = &AvgTTFTScorer{}

// AvgTTFTScorerConfig holds the scorer's JSON parameters.
type AvgTTFTScorerConfig struct {
	// DecayWeight scales the staleness decay in [0,1]; 0 disables. Default 1.0.
	// Decay is only applied when the model has at most MaxIdleProbes in-flight requests,
	// so it cannot accidentally reward a saturated model.
	DecayWeight *float64 `json:"decayWeight,omitempty"`
	// StalenessThreshold is the elapsed time for full staleness (e.g. "30s"). Default "30s".
	StalenessThreshold string `json:"stalenessThreshold,omitempty"`
	// InflightWeight scales the queue-depth penalty.
	// predicted = baseTTFT * (1 + inflightWeight * requests)
	// This is self-normalizing: a fast model (low AvgTTFT) incurs a small penalty per
	// inflight request; a slow model incurs a proportionally larger one.
	// 0 disables the inflight term (pure AvgTTFT behavior). Default 1.0.
	InflightWeight *float64 `json:"inflightWeight,omitempty"`
	// MaxIdleProbes is the maximum number of in-flight requests while staleness decay
	// is still applied. Default 0 (stop decay at the first inflight request).
	// Raising this to 2–3 lets a small parallel probe burst land before decay is
	// suppressed, giving the EMA enough signal to recover quickly from staleness.
	MaxIdleProbes *int64 `json:"maxIdleProbes,omitempty"`
}

// AvgTTFTScorer scores models on predicted TTFT = baseTTFT × (1 + inflightWeight × requests),
// where baseTTFT is the EMA with optional staleness decay (applied only when the model is idle).
// The inflight term self-normalizes: a fast model incurs a small queue penalty per request,
// a slow model a proportionally larger one — no explicit capacity needed.
type AvgTTFTScorer struct {
	typedName      plugin.TypedName
	decayCfg       decay.Config
	inflightWeight float64
}

func ScorerFactory(name string, parameters json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	config := AvgTTFTScorerConfig{
		StalenessThreshold: defaultStalenessThreshold.String(),
	}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse parameters for plugin %q: %w", name, err)
		}
	}
	decayWeight := defaultDecayWeight
	if config.DecayWeight != nil {
		decayWeight = *config.DecayWeight
	}
	if decayWeight < 0 || decayWeight > 1 {
		return nil, fmt.Errorf("invalid decayWeight %v for plugin %q: must be in [0, 1]", decayWeight, name)
	}
	threshold, err := time.ParseDuration(config.StalenessThreshold)
	if err != nil {
		return nil, fmt.Errorf("invalid stalenessThreshold %q for plugin %q: %w", config.StalenessThreshold, name, err)
	}
	inflightWeight := defaultInflightWeight
	if config.InflightWeight != nil {
		inflightWeight = *config.InflightWeight
	}
	var maxIdleProbes int64
	if config.MaxIdleProbes != nil {
		maxIdleProbes = *config.MaxIdleProbes
	}
	return NewAvgTTFTScorer().
		WithName(name).
		WithDecay(decay.Config{Weight: decayWeight, Threshold: threshold, MaxIdleProbes: maxIdleProbes}).
		WithInflight(inflightWeight), nil
}

func NewAvgTTFTScorer() *AvgTTFTScorer {
	return &AvgTTFTScorer{
		typedName:      plugin.TypedName{Type: PluginType, Name: PluginType},
		decayCfg:       decay.Config{Weight: defaultDecayWeight, Threshold: defaultStalenessThreshold},
		inflightWeight: defaultInflightWeight,
	}
}

func (s *AvgTTFTScorer) TypedName() plugin.TypedName { return s.typedName }

func (s *AvgTTFTScorer) WithName(name string) *AvgTTFTScorer {
	s.typedName.Name = name
	return s
}

// WithDecay overrides the decay configuration.
func (s *AvgTTFTScorer) WithDecay(cfg decay.Config) *AvgTTFTScorer {
	s.decayCfg = cfg
	return s
}

// WithInflight overrides the inflight weight.
func (s *AvgTTFTScorer) WithInflight(weight float64) *AvgTTFTScorer {
	s.inflightWeight = weight
	return s
}

// Score returns score = (max - avgTTFT)/(max - min) per model, with optional staleness decay.
func (s *AvgTTFTScorer) Score(ctx context.Context, _ *plugin.CycleState, _ *requesthandling.InferenceRequest, models []datalayer.Model) map[datalayer.Model]float64 {
	now := time.Now()
	ttfts := make(map[datalayer.Model]float64, len(models))
	minTTFT := math.MaxFloat64
	maxTTFT := 0.0

	for _, model := range models {
		v := s.predictedTTFT(model, now)
		ttfts[model] = v
		if v > maxTTFT {
			maxTTFT = v
		}
		if v < minTTFT {
			minTTFT = v
		}
	}

	scores := make(map[datalayer.Model]float64, len(models))
	for _, model := range models {
		if maxTTFT == minTTFT {
			scores[model] = 1.0
		} else {
			scores[model] = (maxTTFT - ttfts[model]) / (maxTTFT - minTTFT)
		}
	}

	if debugLogger := log.FromContext(ctx).V(logutil.DEBUG); debugLogger.Enabled() {
		for _, model := range models {
			debugLogger.Info("avg-ttft score", "model", model.GetName(), "predictedTTFT", ttfts[model], "score", scores[model])
		}
	}

	return scores
}

// predictedTTFT returns baseTTFT × (1 + inflightWeight × excess), where:
//   - baseTTFT is the EMA with decay applied only when the model has at most MaxIdleProbes requests.
//   - excess = max(requests − AvgRequests, 0): load above the model's normal operating point.
//
// The EMA already prices in latency at the typical load (AvgRequests), so penalising only
// the excess avoids double-counting steady-state concurrency while still reacting immediately
// to unexpected bursts. At cold start (AvgRequests==0) excess == requests, matching the
// previous behaviour.
// Returns 0 if no TTFT has been observed yet (model is treated as idle/new).
func (s *AvgTTFTScorer) predictedTTFT(model datalayer.Model, now time.Time) float64 {
	val, ok := model.GetAttributes().Get(requestmetadata.RequestMetadataAttributeKey)
	if !ok {
		return 0
	}
	rc, ok := val.(requestmetadata.ModelMetrics)
	if !ok {
		return 0
	}
	if rc.AvgTTFT == 0 {
		return 0
	}
	var lastObservedAt time.Time
	if rc.LastObservedAt > 0 {
		lastObservedAt = time.Unix(0, rc.LastObservedAt)
	}
	baseTTFT := decay.Apply(rc.AvgTTFT, lastObservedAt, rc.Requests, now, s.decayCfg)
	excess := math.Max(float64(rc.Requests)-rc.AvgRequests, 0)
	return baseTTFT * (1 + s.inflightWeight*excess)
}
