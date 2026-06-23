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

// Package metricsendpoint defines the shared type and AttributeMap key used to
// attach the URL of a model's pool-aggregated Prometheus metrics endpoint to a
// Model in the datalayer.
//
// modelconfigcollector is the sole producer: when a model entry in the config
// file carries a non-empty metricsURL, the collector attaches a MetricsEndpoint
// attribute keyed by AttributeKey. The kv-cache-collector (and any future pool
// metrics consumer) reads it back to know where to scrape.
//
// The URL is contractually a *pool-aggregated* endpoint — not a single pod's
// /metrics — so that one HTTP scrape per poll yields one number per model. See
// docs/proposals/context-compaction/step-1-kv-cache-collector.md for the
// rationale and operator guidance.
package metricsendpoint

import "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"

// AttributeKey is the AttributeMap key under which a model's MetricsEndpoint is
// stored. Absence of the attribute means the operator did not configure a
// metricsURL for this model; consumers must treat absence as "no data" rather
// than failing.
const AttributeKey = "metrics-endpoint"

// MetricsEndpoint is the cloneable value stored on the Model's AttributeMap
// under AttributeKey.
type MetricsEndpoint struct {
	URL string
}

// Clone implements datalayer.Cloneable.
func (e MetricsEndpoint) Clone() datalayer.Cloneable { return e }
