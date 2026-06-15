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
	"math"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
	requestmetadata "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/requestmetadata"
)

const PluginType = "avg-tpot-scorer"

// compile-time interface assertion
var _ modelselector.Scorer = &AvgTPOTScorer{}

// AvgTPOTScorer scores models based on their exponential moving average TPOT.
// The model with the lowest AvgTPOT scores 1.0; the highest scores 0.0.
// Models with no observed TPOT yet (AvgTPOT == 0) are treated as idle and score 1.0.
// If all models have the same AvgTPOT, all score 1.0.
type AvgTPOTScorer struct {
	typedName plugin.TypedName
}

func ScorerFactory(name string, _ json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	return NewAvgTPOTScorer().WithName(name), nil
}

func NewAvgTPOTScorer() *AvgTPOTScorer {
	return &AvgTPOTScorer{
		typedName: plugin.TypedName{Type: PluginType, Name: PluginType},
	}
}

func (s *AvgTPOTScorer) TypedName() plugin.TypedName { return s.typedName }

func (s *AvgTPOTScorer) WithName(name string) *AvgTPOTScorer {
	s.typedName.Name = name
	return s
}

// Score returns a score in [0,1] for each model.
// Formula: score = (max - avgTPOT) / (max - min)
func (s *AvgTPOTScorer) Score(ctx context.Context, _ *plugin.CycleState, _ *requesthandling.InferenceRequest, models []datalayer.Model) map[datalayer.Model]float64 {
	tpots := make(map[datalayer.Model]float64, len(models))
	minTPOT := math.MaxFloat64
	maxTPOT := 0.0

	for _, model := range models {
		v := avgTPOT(model)
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

// avgTPOT returns the AvgTPOT for a model, or 0 if not yet observed.
func avgTPOT(model datalayer.Model) float64 {
	val, ok := model.GetAttributes().Get(requestmetadata.RequestMetadataAttributeKey)
	if !ok {
		return 0
	}
	rc, ok := val.(requestmetadata.ModelMetrics)
	if !ok {
		return 0
	}
	return rc.AvgTPOT
}
