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

// Package decay reduces an EMA-based metric toward zero as it goes stale.
package decay

import (
	"math"
	"time"
)

// Config controls the decay applied by Apply.
type Config struct {
	Weight    float64       // [0,1]; 0 disables decay.
	Threshold time.Duration // staleness reaches full strength after this elapsed time.
}

// Apply returns ema * (1 - weight * staleness * idleness), where
// staleness = min(elapsed/threshold, 1) and idleness = 1/(1+requests).
// Returns ema unchanged when weight or threshold are non-positive, or lastObservedAt is zero.
func Apply(ema float64, lastObservedAt time.Time, requests int64, now time.Time, cfg Config) float64 {
	if cfg.Weight <= 0 || cfg.Threshold <= 0 || lastObservedAt.IsZero() {
		return ema
	}
	elapsed := now.Sub(lastObservedAt)
	if elapsed <= 0 {
		return ema
	}
	staleness := math.Min(float64(elapsed)/float64(cfg.Threshold), 1.0)
	idleness := 1.0 / (1.0 + float64(requests))
	d := cfg.Weight * staleness * idleness
	return ema * (1 - d)
}
