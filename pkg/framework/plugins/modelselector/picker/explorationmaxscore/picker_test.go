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
	"testing"
	"time"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/modelselector"
)

func scored(name string, score float64) *modelselector.ScoredModel {
	return &modelselector.ScoredModel{Model: datalayer.NewModel(name), Score: score}
}

func pickName(p *ExplorationMaxScorePicker, models ...*modelselector.ScoredModel) string {
	result := p.Pick(context.Background(), nil, models)
	if result == nil {
		return ""
	}
	return result.TargetModel.GetName()
}

// TestExploitsMaxScoreWhenExplorationDisabled verifies that explorationRatio=0 always picks max.
func TestExploitsMaxScoreWhenExplorationDisabled(t *testing.T) {
	p := NewExplorationMaxScorePicker().WithExplorationRatio(0)
	for range 10 {
		got := pickName(p, scored("low", 0.2), scored("high", 0.9))
		if got != "high" {
			t.Fatalf("expected high, got %s", got)
		}
	}
}

// TestExploitsWhenNoStaleModels verifies the picker falls back to exploitation when no stale candidates exist.
func TestExploitsWhenNoStaleModels(t *testing.T) {
	p := NewExplorationMaxScorePicker().WithExplorationRatio(1.0).WithStalenessThreshold(time.Hour)
	got := pickName(p, scored("low", 0.2), scored("high", 0.9))
	if got != "high" {
		t.Errorf("expected high (no stale models on first call), got %s", got)
	}
}

// TestExploresUniqueStaleModel verifies a model with a frozen score is probed when explorationRatio is high.
func TestExploresUniqueStaleModel(t *testing.T) {
	now := time.Now()
	p := NewExplorationMaxScorePicker().
		WithExplorationRatio(1.0).
		WithStalenessThreshold(time.Second)
	p.nowFunc = func() time.Time { return now }

	high := scored("high", 0.9)
	low := scored("low", 0.2)

	// Seed both models.
	pickName(p, high, low)

	// Advance clock: both stale. Then change "low" score → it resets; only "high" remains stale.
	now = now.Add(2 * time.Second)
	low.Score = 0.95

	got := pickName(p, high, low)
	if got != "high" {
		t.Errorf("expected to explore stale model 'high', got %s", got)
	}
}

// TestPickerFactoryDefaults verifies an empty config yields the documented default values.
func TestPickerFactoryDefaults(t *testing.T) {
	p, err := PickerFactory("test", json.RawMessage(`{}`), nil)
	if err != nil {
		t.Fatalf("PickerFactory error: %v", err)
	}
	ep := p.(*ExplorationMaxScorePicker)
	if ep.explorationRatio != defaultExplorationRatio {
		t.Errorf("expected explorationRatio=%f, got %f", defaultExplorationRatio, ep.explorationRatio)
	}
	if ep.stalenessThreshold != defaultStalenessThreshold {
		t.Errorf("expected stalenessThreshold=%s, got %s", defaultStalenessThreshold, ep.stalenessThreshold)
	}
	if ep.TypedName().Name != "test" {
		t.Errorf("expected name 'test', got %q", ep.TypedName().Name)
	}
}

// TestPickerFactoryCustomConfig verifies the factory parses a fully-specified JSON config.
func TestPickerFactoryCustomConfig(t *testing.T) {
	p, err := PickerFactory("test", json.RawMessage(`{"explorationRatio":0.1,"stalenessThreshold":"1m"}`), nil)
	if err != nil {
		t.Fatalf("PickerFactory error: %v", err)
	}
	ep := p.(*ExplorationMaxScorePicker)
	if ep.explorationRatio != 0.1 {
		t.Errorf("expected explorationRatio=0.1, got %f", ep.explorationRatio)
	}
	if ep.stalenessThreshold != time.Minute {
		t.Errorf("expected stalenessThreshold=1m, got %s", ep.stalenessThreshold)
	}
}

// TestPickerFactoryInvalidStalenessThreshold verifies the factory rejects unparsable durations.
func TestPickerFactoryInvalidStalenessThreshold(t *testing.T) {
	_, err := PickerFactory("test", json.RawMessage(`{"stalenessThreshold":"not-a-duration"}`), nil)
	if err == nil {
		t.Fatal("expected error for invalid stalenessThreshold, got nil")
	}
}
