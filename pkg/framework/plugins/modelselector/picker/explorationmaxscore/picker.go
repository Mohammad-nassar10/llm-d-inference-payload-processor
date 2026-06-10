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

package explorationmaxscore

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/modelselector/picker"
)

const (
	// PickerType is the registered name of this picker plugin.
	PickerType = "exploration-max-score-picker"

	// defaultExplorationRatio is the fraction of requests sent to stale models for probing.
	defaultExplorationRatio = 0.05

	// defaultStalenessThreshold is how long a model's score must be unchanged before it is
	// eligible for exploration probing.
	defaultStalenessThreshold = 30 * time.Second
)

// compile-time interface assertion
var _ modelselector.Picker = &ExplorationMaxScorePicker{}

// ExplorationMaxScorePickerConfig holds the JSON-configurable parameters.
type ExplorationMaxScorePickerConfig struct {
	// ExplorationRatio is the probability [0,1] of probing a stale model instead of exploiting the best.
	ExplorationRatio float64 `json:"explorationRatio,omitempty"`
	// StalenessThreshold is how long a model's score must be unchanged before it qualifies for probing (e.g. "30s").
	StalenessThreshold string `json:"stalenessThreshold,omitempty"`
}

// PickerFactory is the registered factory function for ExplorationMaxScorePicker.
func PickerFactory(name string, rawParameters json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	config := ExplorationMaxScorePickerConfig{
		ExplorationRatio:   defaultExplorationRatio,
		StalenessThreshold: defaultStalenessThreshold.String(),
	}
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse parameters for plugin %q: %w", name, err)
		}
	}
	threshold, err := time.ParseDuration(config.StalenessThreshold)
	if err != nil {
		return nil, fmt.Errorf("invalid stalenessThreshold %q for plugin %q: %w", config.StalenessThreshold, name, err)
	}
	return NewExplorationMaxScorePicker().
		WithName(name).
		WithExplorationRatio(config.ExplorationRatio).
		WithStalenessThreshold(threshold), nil
}

// ExplorationMaxScorePicker normally routes to the highest-scored model (exploitation), but with
// probability explorationRatio it instead probes a model whose score has been unchanged for longer
// than stalenessThreshold. The probe generates real latency observations, unfreezing any EMA that
// has been frozen due to the model receiving zero traffic.
//
// When explorationRatio=0 this degrades to a pure max-score picker.
// Pick may be called concurrently; internal state is protected by a mutex.
type ExplorationMaxScorePicker struct {
	typedName          plugin.TypedName
	explorationRatio   float64
	stalenessThreshold time.Duration
	nowFunc            func() time.Time // injectable for tests

	mu                sync.Mutex
	lastScore         map[string]float64
	lastScoreChangeAt map[string]time.Time
}

// NewExplorationMaxScorePicker creates an ExplorationMaxScorePicker with default parameters.
func NewExplorationMaxScorePicker() *ExplorationMaxScorePicker {
	return &ExplorationMaxScorePicker{
		typedName:          plugin.TypedName{Type: PickerType, Name: PickerType},
		explorationRatio:   defaultExplorationRatio,
		stalenessThreshold: defaultStalenessThreshold,
		nowFunc:            time.Now,
		lastScore:          make(map[string]float64),
		lastScoreChangeAt:  make(map[string]time.Time),
	}
}

// WithName sets the instance name.
func (p *ExplorationMaxScorePicker) WithName(name string) *ExplorationMaxScorePicker {
	p.typedName.Name = name
	return p
}

// WithExplorationRatio sets the probability of exploration on each request.
func (p *ExplorationMaxScorePicker) WithExplorationRatio(r float64) *ExplorationMaxScorePicker {
	p.explorationRatio = r
	return p
}

// WithStalenessThreshold sets how long a score must be frozen before the model is eligible for probing.
func (p *ExplorationMaxScorePicker) WithStalenessThreshold(d time.Duration) *ExplorationMaxScorePicker {
	p.stalenessThreshold = d
	return p
}

// TypedName returns the type and name tuple for this plugin instance.
func (p *ExplorationMaxScorePicker) TypedName() plugin.TypedName { return p.typedName }

// Pick selects a model. With probability explorationRatio it probes a stale model (score frozen for
// longer than stalenessThreshold); otherwise it picks the max-scored model with random tie-breaking.
func (p *ExplorationMaxScorePicker) Pick(ctx context.Context, _ *plugin.CycleState, scoredModels []*modelselector.ScoredModel) *modelselector.PipelineRunResult {
	if len(scoredModels) == 0 {
		return nil
	}

	now := p.nowFunc()

	// Update score history and collect stale candidates under a single lock.
	var stale []*modelselector.ScoredModel
	p.mu.Lock()
	for _, sm := range scoredModels {
		name := sm.GetName()
		prev, seen := p.lastScore[name]
		if !seen || prev != sm.Score {
			p.lastScore[name] = sm.Score
			p.lastScoreChangeAt[name] = now
		} else if now.Sub(p.lastScoreChangeAt[name]) > p.stalenessThreshold {
			stale = append(stale, sm)
		}
	}
	p.mu.Unlock()

	if debugLogger := log.FromContext(ctx).V(logutil.DEBUG); debugLogger.Enabled() {
		debugLogger.Info("exploration-max-score-picker picking",
			"numCandidates", len(scoredModels),
			"numStale", len(stale),
			"explorationRatio", p.explorationRatio)
	}

	// Exploration: probe a randomly selected stale model.
	if len(stale) > 0 && picker.PickerRand.Float64() < p.explorationRatio {
		picker.ShuffleScoredModels(stale)
		return &modelselector.PipelineRunResult{TargetModel: stale[0].Model}
	}

	// Exploitation: pick the max-scored model with random tie-breaking.
	picker.ShuffleScoredModels(scoredModels)
	slices.SortStableFunc(scoredModels, func(i, j *modelselector.ScoredModel) int {
		if i.Score > j.Score {
			return -1
		}
		if i.Score < j.Score {
			return 1
		}
		return 0
	})
	return &modelselector.PipelineRunResult{TargetModel: scoredModels[0].Model}
}
