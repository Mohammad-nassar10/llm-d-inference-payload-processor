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

package pricing

import (
	"testing"

	"github.com/caio/go-tdigest/v5"
)

// newDigest builds a tdigest with the proposal's default compression and
// adds the given samples. It fails the test on any library error so that
// the call sites stay free of error handling.
func newDigest(t *testing.T, samples ...float64) *tdigest.TDigest {
	t.Helper()
	d, err := tdigest.New(tdigest.Compression(200))
	if err != nil {
		t.Fatalf("tdigest.New: %v", err)
	}
	for _, s := range samples {
		if err := d.Add(s); err != nil {
			t.Fatalf("tdigest.Add(%v): %v", s, err)
		}
	}
	return d
}
