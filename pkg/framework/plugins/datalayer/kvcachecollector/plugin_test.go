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

package kvcachecollector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/datastore"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/metricsendpoint"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
)

// fakeHandle satisfies plugin.Handle for unit tests. Only Datastore() is
// meaningful for this plugin; the rest return nil/empty.
type fakeHandle struct{ ds datalayer.Datastore }

func (f *fakeHandle) Context() context.Context                         { return context.Background() }
func (f *fakeHandle) Client() client.Client                            { return nil }
func (f *fakeHandle) ReconcilerBuilder() *ctrlbuilder.Builder          { return nil }
func (f *fakeHandle) Datastore() datalayer.Datastore                   { return f.ds }
func (f *fakeHandle) EventNotifier() datalayer.EventNotifier           { return nil }
func (f *fakeHandle) Plugin(string) plugin.Plugin                      { return nil }
func (f *fakeHandle) AddPlugin(string, plugin.Plugin)                  {}
func (f *fakeHandle) GetAllPlugins() []plugin.Plugin                   { return nil }
func (f *fakeHandle) GetAllPluginsWithNames() map[string]plugin.Plugin { return nil }

// seedModel registers a model and (optionally) attaches a metrics-endpoint
// attribute carrying url. An empty url means "no endpoint attribute".
func seedModel(t *testing.T, ds datalayer.Datastore, name, url string) {
	t.Helper()
	mdl := ds.GetOrCreateModel(name)
	if url != "" {
		mdl.GetAttributes().Put(metricsendpoint.AttributeKey, metricsendpoint.MetricsEndpoint{URL: url})
	}
}

// readKVCacheMetrics returns the stored KVCacheMetrics for a model, or
// (zero, false) if the attribute is absent. Fails the test if present but of
// the wrong type.
func readKVCacheMetrics(t *testing.T, ds datalayer.Datastore, name string) (KVCacheMetrics, bool) {
	t.Helper()
	v, ok := ds.GetOrCreateModel(name).GetAttributes().Get(KVCacheMetricsAttributeKey)
	if !ok {
		return KVCacheMetrics{}, false
	}
	m, ok := v.(KVCacheMetrics)
	if !ok {
		t.Fatalf("model %q: attribute %q has type %T, want KVCacheMetrics",
			name, KVCacheMetricsAttributeKey, v)
	}
	return m, true
}

// metricsBody renders a minimal Prometheus text-format body. Pass "" for a
// metric to omit it.
func metricsBody(utilName string, util float64, queueName string, queue int) string {
	out := ""
	if utilName != "" {
		out += fmt.Sprintf("# TYPE %s gauge\n%s %g\n", utilName, utilName, util)
	}
	if queueName != "" {
		out += fmt.Sprintf("# TYPE %s gauge\n%s %d\n", queueName, queueName, queue)
	}
	return out
}

// --- Factory tests ---

// TestFactory_DefaultsApplied verifies that an empty config block produces a
// collector with all five documented defaults in place.
func TestFactory_DefaultsApplied(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	p, err := CollectorFactory("kv", json.RawMessage(`{}`), &fakeHandle{ds: ds})
	if err != nil {
		t.Fatalf("CollectorFactory: %v", err)
	}
	c := p.(*KVCacheCollector)
	if got, want := c.CollectorFrequency(), defaultInterval; got != want {
		t.Errorf("interval = %v, want %v", got, want)
	}
	if got, want := c.timeout, defaultTimeout; got != want {
		t.Errorf("timeout = %v, want %v", got, want)
	}
	if got, want := c.maxConcurrent, defaultMaxConcurrent; got != want {
		t.Errorf("maxConcurrent = %d, want %d", got, want)
	}
	if got, want := c.utilizationMetric, defaultUtilizationMetric; got != want {
		t.Errorf("utilizationMetric = %q, want %q", got, want)
	}
	if got, want := c.queueDepthMetric, defaultQueueDepthMetric; got != want {
		t.Errorf("queueDepthMetric = %q, want %q", got, want)
	}
}

// TestFactory_InvalidInterval verifies that the factory rejects an
// unparseable or non-positive interval rather than silently substituting a
// default — bad config should be loud at startup.
func TestFactory_InvalidInterval(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"unparseable", `{"interval":"bogus"}`},
		{"zero", `{"interval":"0s"}`},
		{"negative", `{"interval":"-5s"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := datastore.NewFakeDataStore()
			_, err := CollectorFactory("kv", json.RawMessage(tc.raw), &fakeHandle{ds: ds})
			if err == nil {
				t.Errorf("expected error for interval %q, got nil", tc.raw)
			}
		})
	}
}

// TestFactory_InvalidTimeout verifies the same rejection behavior for timeout.
func TestFactory_InvalidTimeout(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"unparseable", `{"timeout":"bogus"}`},
		{"zero", `{"timeout":"0s"}`},
		{"negative", `{"timeout":"-1s"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := datastore.NewFakeDataStore()
			_, err := CollectorFactory("kv", json.RawMessage(tc.raw), &fakeHandle{ds: ds})
			if err == nil {
				t.Errorf("expected error for timeout %q, got nil", tc.raw)
			}
		})
	}
}

// TestFactory_OverridesApplied verifies that JSON-supplied values flow through
// to the collector, including the metric-name overrides that operators need
// when running against engines that use different metric names than vLLM.
func TestFactory_OverridesApplied(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	raw := json.RawMessage(`{
		"interval":"7s",
		"timeout":"1s",
		"utilizationMetric":"sglang:kv_cache_usage",
		"queueDepthMetric":"sglang:queue_size",
		"maxConcurrent":3
	}`)
	p, err := CollectorFactory("kv", raw, &fakeHandle{ds: ds})
	if err != nil {
		t.Fatalf("CollectorFactory: %v", err)
	}
	c := p.(*KVCacheCollector)
	if got, want := c.CollectorFrequency(), 7*time.Second; got != want {
		t.Errorf("interval = %v, want %v", got, want)
	}
	if got, want := c.timeout, 1*time.Second; got != want {
		t.Errorf("timeout = %v, want %v", got, want)
	}
	if got, want := c.utilizationMetric, "sglang:kv_cache_usage"; got != want {
		t.Errorf("utilizationMetric = %q, want %q", got, want)
	}
	if got, want := c.queueDepthMetric, "sglang:queue_size"; got != want {
		t.Errorf("queueDepthMetric = %q, want %q", got, want)
	}
	if got, want := c.maxConcurrent, 3; got != want {
		t.Errorf("maxConcurrent = %d, want %d", got, want)
	}
}

// --- Poll tests ---

// TestPoll_NoModels verifies that an empty datastore yields a no-op Poll.
func TestPoll_NoModels(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	c := NewKVCacheCollector(ds)
	if _, err := c.Poll(context.Background()); err != nil {
		t.Errorf("Poll on empty datastore returned error: %v", err)
	}
}

// TestPoll_ModelsWithoutEndpoint_Skipped verifies that models without a
// metrics-endpoint attribute are left alone — the contract that lets an
// operator opt only a subset of models into scraping.
func TestPoll_ModelsWithoutEndpoint_Skipped(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	seedModel(t, ds, "m1", "") // no endpoint
	c := NewKVCacheCollector(ds)

	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if _, ok := readKVCacheMetrics(t, ds, "m1"); ok {
		t.Errorf("expected no KVCacheMetrics attribute for model without endpoint")
	}
}

// TestPoll_HappyPath_WritesAttribute verifies the end-to-end flow: a model
// with a metrics-endpoint attribute is scraped, the response is parsed, and
// the resulting KVCacheMetrics attribute carries the right values.
func TestPoll_HappyPath_WritesAttribute(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(metricsBody(defaultUtilizationMetric, 0.42, defaultQueueDepthMetric, 7)))
	}))
	t.Cleanup(srv.Close)

	ds := datastore.NewFakeDataStore()
	seedModel(t, ds, "m1", srv.URL+"/metrics")

	c := NewKVCacheCollector(ds).WithHTTPClient(srv.Client())
	before := time.Now().UnixNano()
	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	got, ok := readKVCacheMetrics(t, ds, "m1")
	if !ok {
		t.Fatalf("expected KVCacheMetrics to be present")
	}
	if got.Utilization != 0.42 {
		t.Errorf("Utilization = %v, want 0.42", got.Utilization)
	}
	if got.QueueDepth != 7 {
		t.Errorf("QueueDepth = %d, want 7", got.QueueDepth)
	}
	if got.LastObservedAt < before {
		t.Errorf("LastObservedAt = %d, want >= %d", got.LastObservedAt, before)
	}
}

// TestPoll_AggregatesAcrossLabelSets verifies that multi-label-set responses
// (e.g. when the source endpoint happens to expose one row per engine) are
// collapsed correctly: max for the ratio (utilization), sum for the count
// (queue depth).
func TestPoll_AggregatesAcrossLabelSets(t *testing.T) {
	body := "# TYPE vllm:gpu_cache_usage_perc gauge\n" +
		"vllm:gpu_cache_usage_perc{engine=\"a\"} 0.30\n" +
		"vllm:gpu_cache_usage_perc{engine=\"b\"} 0.91\n" +
		"vllm:gpu_cache_usage_perc{engine=\"c\"} 0.55\n" +
		"# TYPE vllm:num_requests_waiting gauge\n" +
		"vllm:num_requests_waiting{engine=\"a\"} 3\n" +
		"vllm:num_requests_waiting{engine=\"b\"} 8\n" +
		"vllm:num_requests_waiting{engine=\"c\"} 1\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	ds := datastore.NewFakeDataStore()
	seedModel(t, ds, "m1", srv.URL+"/metrics")

	c := NewKVCacheCollector(ds).WithHTTPClient(srv.Client())
	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	got, ok := readKVCacheMetrics(t, ds, "m1")
	if !ok {
		t.Fatalf("expected KVCacheMetrics to be present")
	}
	if got.Utilization != 0.91 {
		t.Errorf("Utilization (max) = %v, want 0.91", got.Utilization)
	}
	if got.QueueDepth != 12 {
		t.Errorf("QueueDepth (sum) = %d, want 12 (3+8+1)", got.QueueDepth)
	}
}

// TestPoll_MissingMetric_DefaultsToZero verifies that when the scrape succeeds
// but the configured metric name is absent, the field is set to 0 and
// LastObservedAt still advances — consumers can distinguish "endpoint is alive
// but the metric is missing" from "endpoint failed entirely".
func TestPoll_MissingMetric_DefaultsToZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Only the queue-depth metric is present; utilization is omitted.
		_, _ = w.Write([]byte(metricsBody("", 0, defaultQueueDepthMetric, 4)))
	}))
	t.Cleanup(srv.Close)

	ds := datastore.NewFakeDataStore()
	seedModel(t, ds, "m1", srv.URL+"/metrics")

	c := NewKVCacheCollector(ds).WithHTTPClient(srv.Client())
	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	got, ok := readKVCacheMetrics(t, ds, "m1")
	if !ok {
		t.Fatalf("expected KVCacheMetrics to be present")
	}
	if got.Utilization != 0 {
		t.Errorf("Utilization = %v, want 0 (metric missing)", got.Utilization)
	}
	if got.QueueDepth != 4 {
		t.Errorf("QueueDepth = %d, want 4", got.QueueDepth)
	}
	if got.LastObservedAt == 0 {
		t.Errorf("LastObservedAt = 0, want > 0 (scrape succeeded)")
	}
}

// TestPoll_HTTPError_LeavesAttributeStale verifies the staleness contract:
// when a scrape fails, any previously stored value is preserved unchanged.
// Consumers that need fresh data must check LastObservedAt themselves.
func TestPoll_HTTPError_LeavesAttributeStale(t *testing.T) {
	// First scrape returns OK; second returns 500.
	var hitCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitCount++
		if hitCount == 1 {
			_, _ = w.Write([]byte(metricsBody(defaultUtilizationMetric, 0.70, defaultQueueDepthMetric, 5)))
			return
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	ds := datastore.NewFakeDataStore()
	seedModel(t, ds, "m1", srv.URL+"/metrics")

	c := NewKVCacheCollector(ds).WithHTTPClient(srv.Client())
	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll 1: %v", err)
	}
	first, ok := readKVCacheMetrics(t, ds, "m1")
	if !ok || first.Utilization != 0.70 {
		t.Fatalf("first poll did not seed expected value: got=%+v ok=%v", first, ok)
	}

	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll 2: %v", err)
	}
	second, ok := readKVCacheMetrics(t, ds, "m1")
	if !ok {
		t.Fatalf("expected attribute to remain after failed scrape")
	}
	if second.Utilization != first.Utilization || second.QueueDepth != first.QueueDepth {
		t.Errorf("expected stale values preserved on failure; first=%+v second=%+v", first, second)
	}
	if second.LastObservedAt != first.LastObservedAt {
		t.Errorf("LastObservedAt should not advance on failure; first=%d second=%d",
			first.LastObservedAt, second.LastObservedAt)
	}
}

// TestPoll_PartialFailure_OtherModelsSucceed verifies that one failing scrape
// does not block the rest — the per-model Poll goroutines are independent.
func TestPoll_PartialFailure_OtherModelsSucceed(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(metricsBody(defaultUtilizationMetric, 0.20, defaultQueueDepthMetric, 1)))
	}))
	t.Cleanup(good.Close)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)

	ds := datastore.NewFakeDataStore()
	seedModel(t, ds, "good", good.URL+"/metrics")
	seedModel(t, ds, "bad", bad.URL+"/metrics")

	c := NewKVCacheCollector(ds).WithHTTPClient(good.Client())
	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	gotGood, ok := readKVCacheMetrics(t, ds, "good")
	if !ok {
		t.Errorf("expected attribute written for good model")
	}
	if gotGood.Utilization != 0.20 {
		t.Errorf("good Utilization = %v, want 0.20", gotGood.Utilization)
	}
	if _, ok := readKVCacheMetrics(t, ds, "bad"); ok {
		t.Errorf("expected no attribute for bad model (failed scrape)")
	}
}
