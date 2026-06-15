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

package decay

import (
	"math"
	"testing"
	"time"
)

// TestApply covers the decay formula across weight/threshold/staleness/idleness combinations.
func TestApply(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	threshold := 30 * time.Second

	tests := []struct {
		name           string
		ema            float64
		lastObservedAt time.Time
		requests       int64
		cfg            Config
		want           float64
	}{
		{
			name:           "weight zero — raw ema returned",
			ema:            1.0,
			lastObservedAt: now.Add(-60 * time.Second),
			requests:       0,
			cfg:            Config{Weight: 0, Threshold: threshold},
			want:           1.0,
		},
		{
			name:           "threshold zero — raw ema returned",
			ema:            1.0,
			lastObservedAt: now.Add(-60 * time.Second),
			requests:       0,
			cfg:            Config{Weight: 1, Threshold: 0},
			want:           1.0,
		},
		{
			name:           "never observed — raw ema returned",
			ema:            1.0,
			lastObservedAt: time.Time{},
			requests:       0,
			cfg:            Config{Weight: 1, Threshold: threshold},
			want:           1.0,
		},
		{
			name:           "fresh observation — no decay",
			ema:            1.0,
			lastObservedAt: now,
			requests:       0,
			cfg:            Config{Weight: 1, Threshold: threshold},
			want:           1.0,
		},
		{
			name:           "fully stale idle — full decay to zero",
			ema:            1.0,
			lastObservedAt: now.Add(-60 * time.Second),
			requests:       0,
			cfg:            Config{Weight: 1, Threshold: threshold},
			want:           0.0,
		},
		{
			name:           "busy model — decay suppressed when requests > MaxIdleProbes",
			ema:            1.0,
			lastObservedAt: now.Add(-60 * time.Second),
			requests:       1,
			cfg:            Config{Weight: 1, Threshold: threshold, MaxIdleProbes: 0},
			want:           1.0,
		},
		{
			name:           "MaxIdleProbes — decay applies when requests <= MaxIdleProbes",
			ema:            1.0,
			lastObservedAt: now.Add(-60 * time.Second),
			requests:       2,
			cfg:            Config{Weight: 1, Threshold: threshold, MaxIdleProbes: 2},
			want:           0.0, // fully stale, decay = 1.0 → result = 0.0
		},
		{
			name:           "MaxIdleProbes — decay suppressed when requests exceed limit",
			ema:            1.0,
			lastObservedAt: now.Add(-60 * time.Second),
			requests:       3,
			cfg:            Config{Weight: 1, Threshold: threshold, MaxIdleProbes: 2},
			want:           1.0, // requests=3 > MaxIdleProbes=2 → no decay
		},
		{
			name:           "half threshold idle — half decay",
			ema:            1.0,
			lastObservedAt: now.Add(-15 * time.Second),
			requests:       0,
			cfg:            Config{Weight: 1, Threshold: threshold},
			// staleness = 0.5, idleness = 1, weight = 1 → decay = 0.5 → result = 0.5
			want: 0.5,
		},
		{
			name:           "half weight scales decay",
			ema:            1.0,
			lastObservedAt: now.Add(-60 * time.Second),
			requests:       0,
			cfg:            Config{Weight: 0.5, Threshold: threshold},
			// staleness = 1, idleness = 1, weight = 0.5 → decay = 0.5 → result = 0.5
			want: 0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Apply(tt.ema, tt.lastObservedAt, tt.requests, now, tt.cfg)
			if math.Abs(got-tt.want) > 1e-9 {
				t.Errorf("Apply() = %f, want %f", got, tt.want)
			}
		})
	}
}
