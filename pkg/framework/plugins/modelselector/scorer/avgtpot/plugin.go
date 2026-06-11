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
	PluginType = "avg-tpot-scorer"

	defaultDecayWeight        = 1.0
	defaultStalenessThreshold = 30 * time.Second
)

// compile-time interface assertion
var _ modelselector.Scorer = &AvgTPOTScorer{}

// AvgTPOTScorerConfig holds the scorer's JSON parameters.
type AvgTPOTScorerConfig struct {
	// DecayWeight scales the staleness decay in [0,1]; 0 disables. Default 1.0.
	DecayWeight *float64 `json:"decayWeight,omitempty"`
	// StalenessThreshold is the elapsed time for full staleness (e.g. "30s"). Default "30s".
	StalenessThreshold string `json:"stalenessThreshold,omitempty"`
}

// AvgTPOTScorer scores models based on their exponential moving average TPOT.
// The model with the lowest AvgTPOT scores 1.0; the highest scores 0.0.
// Models with no observed TPOT yet (AvgTPOT == 0) are treated as idle and score 1.0.
// If all models have the same AvgTPOT, all score 1.0.
// Stale EMAs are decayed toward zero (see the decay package); set DecayWeight=0 to disable.
type AvgTPOTScorer struct {
	typedName plugin.TypedName
	decayCfg  decay.Config
}

func ScorerFactory(name string, parameters json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	config := AvgTPOTScorerConfig{
		StalenessThreshold: defaultStalenessThreshold.String(),
	}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse parameters for plugin %q: %w", name, err)
		}
	}
	weight := defaultDecayWeight
	if config.DecayWeight != nil {
		weight = *config.DecayWeight
	}
	if weight < 0 || weight > 1 {
		return nil, fmt.Errorf("invalid decayWeight %v for plugin %q: must be in [0, 1]", weight, name)
	}
	threshold, err := time.ParseDuration(config.StalenessThreshold)
	if err != nil {
		return nil, fmt.Errorf("invalid stalenessThreshold %q for plugin %q: %w", config.StalenessThreshold, name, err)
	}
	return NewAvgTPOTScorer().
		WithName(name).
		WithDecay(decay.Config{Weight: weight, Threshold: threshold}), nil
}

func NewAvgTPOTScorer() *AvgTPOTScorer {
	return &AvgTPOTScorer{
		typedName: plugin.TypedName{Type: PluginType, Name: PluginType},
		decayCfg:  decay.Config{Weight: defaultDecayWeight, Threshold: defaultStalenessThreshold},
	}
}

func (s *AvgTPOTScorer) TypedName() plugin.TypedName { return s.typedName }

func (s *AvgTPOTScorer) WithName(name string) *AvgTPOTScorer {
	s.typedName.Name = name
	return s
}

// WithDecay overrides the decay configuration.
func (s *AvgTPOTScorer) WithDecay(cfg decay.Config) *AvgTPOTScorer {
	s.decayCfg = cfg
	return s
}

// Score returns score = (max - avgTPOT)/(max - min) per model, with optional staleness decay.
func (s *AvgTPOTScorer) Score(ctx context.Context, _ *plugin.CycleState, _ *requesthandling.InferenceRequest, models []datalayer.Model) map[datalayer.Model]float64 {
	now := time.Now()
	tpots := make(map[datalayer.Model]float64, len(models))
	minTPOT := math.MaxFloat64
	maxTPOT := 0.0

	for _, model := range models {
		v := s.avgTPOT(model, now)
		tpots[model] = v
		if v > maxTPOT {
			maxTPOT = v
		}
		if v < minTPOT {
			minTPOT = v
		}
	}

	scores := make(map[datalayer.Model]float64, len(models))
	for _, model := range models {
		if maxTPOT == minTPOT {
			scores[model] = 1.0
		} else {
			scores[model] = (maxTPOT - tpots[model]) / (maxTPOT - minTPOT)
		}
	}

	if debugLogger := log.FromContext(ctx).V(logutil.DEBUG); debugLogger.Enabled() {
		for _, model := range models {
			debugLogger.Info("avg-tpot score", "model", model.GetName(), "avgTPOT", tpots[model], "score", scores[model])
		}
	}

	return scores
}

// avgTPOT returns the decay-adjusted AvgTPOT, or 0 if unobserved.
func (s *AvgTPOTScorer) avgTPOT(model datalayer.Model, now time.Time) float64 {
	val, ok := model.GetAttributes().Get(requestmetadata.RequestMetadataAttributeKey)
	if !ok {
		return 0
	}
	rc, ok := val.(requestmetadata.ModelMetrics)
	if !ok {
		return 0
	}
	if rc.AvgTPOT == 0 {
		return 0
	}
	var lastObservedAt time.Time
	if rc.LastObservedAt > 0 {
		lastObservedAt = time.Unix(0, rc.LastObservedAt)
	}
	return decay.Apply(rc.AvgTPOT, lastObservedAt, rc.Requests, now, s.decayCfg)
}
