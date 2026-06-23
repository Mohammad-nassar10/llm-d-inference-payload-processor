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
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	dlsrc "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/datasource"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/metricsendpoint"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	ippmetrics "github.com/llm-d/llm-d-inference-payload-processor/pkg/metrics"
)

// Failure-reason labels for kv_cache_scrape_failures_total. Closed set to keep
// the metric's cardinality bounded.
const (
	failReasonRequestBuild = "request_build"
	failReasonTimeout      = "timeout"
	failReasonDial         = "dial"
	failReasonHTTPStatus   = "http_status"
	failReasonParse        = "parse"
)

const (
	PluginType                 = "kv-cache-collector"
	KVCacheMetricsAttributeKey = "kv-cache-metrics"

	defaultInterval      = 30 * time.Second
	defaultTimeout       = 5 * time.Second
	defaultMaxConcurrent = 8
	// defaultUtilizationMetric matches llm-d-inference-sim and recent vLLM.
	// Older vLLM exposes the same gauge as "vllm:gpu_cache_usage_perc"; on those
	// versions override via the utilizationMetric parameter rather than patching.
	defaultUtilizationMetric = "vllm:kv_cache_usage_perc"
	defaultQueueDepthMetric  = "vllm:num_requests_waiting"
)

// compile-time interface assertion
var _ dlsrc.Collector = &KVCacheCollector{}

// KVCacheMetrics is the per-model attribute written after a successful scrape.
// A failed scrape leaves the previous value in place; consumers gate freshness
// on LastObservedAt.
type KVCacheMetrics struct {
	Utilization    float64 // pool-wide KV-cache pressure in [0, 1]
	QueueDepth     int64   // sum of waiting requests across the pool
	LastObservedAt int64   // unix-nanos of the most recent successful scrape; 0 if never
}

func (m KVCacheMetrics) Clone() datalayer.Cloneable { return m }

// CollectorConfig is the JSON-configurable subset of KVCacheCollector. All
// fields are optional; defaults are applied in CollectorFactory.
type CollectorConfig struct {
	Interval          string `json:"interval,omitempty"`
	Timeout           string `json:"timeout,omitempty"`
	UtilizationMetric string `json:"utilizationMetric,omitempty"`
	QueueDepthMetric  string `json:"queueDepthMetric,omitempty"`
	MaxConcurrent     int    `json:"maxConcurrent,omitempty"`
}

// KVCacheCollector polls each model's metricsURL on a ticker and writes a
// KVCacheMetrics attribute per model. Poll is called serially by the framework
// so internal state needs no locking.
type KVCacheCollector struct {
	typedName  plugin.TypedName
	ds         datalayer.Datastore
	httpClient *http.Client

	interval          time.Duration
	timeout           time.Duration
	utilizationMetric string
	queueDepthMetric  string
	maxConcurrent     int
}

// NewKVCacheCollector returns a KVCacheCollector with default settings.
func NewKVCacheCollector(ds datalayer.Datastore) *KVCacheCollector {
	return &KVCacheCollector{
		typedName:         plugin.TypedName{Type: PluginType, Name: PluginType},
		ds:                ds,
		httpClient:        newDefaultClient(defaultMaxConcurrent),
		interval:          defaultInterval,
		timeout:           defaultTimeout,
		utilizationMetric: defaultUtilizationMetric,
		queueDepthMetric:  defaultQueueDepthMetric,
		maxConcurrent:     defaultMaxConcurrent,
	}
}

// CollectorFactory parses raw JSON config, applies defaults and validations,
// and returns a configured KVCacheCollector.
func CollectorFactory(name string, raw json.RawMessage, h plugin.Handle) (plugin.Plugin, error) {
	cfg := CollectorConfig{
		Interval:          defaultInterval.String(),
		Timeout:           defaultTimeout.String(),
		UtilizationMetric: defaultUtilizationMetric,
		QueueDepthMetric:  defaultQueueDepthMetric,
		MaxConcurrent:     defaultMaxConcurrent,
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse parameters for plugin %q: %w", name, err)
		}
	}

	interval, err := time.ParseDuration(cfg.Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval %q for plugin %q: %w", cfg.Interval, name, err)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("interval %v for plugin %q must be positive", interval, name)
	}
	timeout, err := time.ParseDuration(cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("invalid timeout %q for plugin %q: %w", cfg.Timeout, name, err)
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout %v for plugin %q must be positive", timeout, name)
	}
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	if cfg.UtilizationMetric == "" {
		cfg.UtilizationMetric = defaultUtilizationMetric
	}
	if cfg.QueueDepthMetric == "" {
		cfg.QueueDepthMetric = defaultQueueDepthMetric
	}

	return NewKVCacheCollector(h.Datastore()).
		WithName(name).
		WithInterval(interval).
		WithTimeout(timeout).
		WithMetricNames(cfg.UtilizationMetric, cfg.QueueDepthMetric).
		WithMaxConcurrent(cfg.MaxConcurrent), nil
}

// newDefaultClient sizes the connection pool to the collector's concurrency.
// Per-request timeout is set via context.WithTimeout in scrape (not Client.Timeout)
// so it also interrupts response body reads, not just the dial.
func newDefaultClient(maxConcurrent int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxConcurrent * 2,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func (c *KVCacheCollector) WithName(name string) *KVCacheCollector {
	c.typedName.Name = name
	return c
}

func (c *KVCacheCollector) WithInterval(d time.Duration) *KVCacheCollector {
	c.interval = d
	return c
}

func (c *KVCacheCollector) WithTimeout(d time.Duration) *KVCacheCollector {
	c.timeout = d
	return c
}

// WithMaxConcurrent caps in-flight scrapes per poll and resizes the http.Client
// connection pool to match.
func (c *KVCacheCollector) WithMaxConcurrent(n int) *KVCacheCollector {
	if n < 1 {
		n = 1
	}
	c.maxConcurrent = n
	c.httpClient = newDefaultClient(n)
	return c
}

func (c *KVCacheCollector) WithMetricNames(utilization, queueDepth string) *KVCacheCollector {
	if utilization != "" {
		c.utilizationMetric = utilization
	}
	if queueDepth != "" {
		c.queueDepthMetric = queueDepth
	}
	return c
}

// WithHTTPClient overrides the http.Client used for scraping; intended for tests.
func (c *KVCacheCollector) WithHTTPClient(client *http.Client) *KVCacheCollector {
	if client != nil {
		c.httpClient = client
	}
	return c
}

func (c *KVCacheCollector) TypedName() plugin.TypedName { return c.typedName }

func (c *KVCacheCollector) CollectorFrequency() time.Duration { return c.interval }

// Poll iterates models with a metrics-endpoint attribute and scrapes each one
// concurrently (bounded by maxConcurrent). Per-model failures are logged but
// do not fail the overall poll; failed scrapes leave the prior attribute
// untouched so consumers see stale data rather than zeros.
func (c *KVCacheCollector) Poll(ctx context.Context) (any, error) {
	logger := log.FromContext(ctx).WithName("kv-cache-collector")

	targets := c.collectTargets()
	if len(targets) == 0 {
		return nil, nil
	}

	sem := make(chan struct{}, c.maxConcurrent)
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(t target) {
			defer wg.Done()
			defer func() { <-sem }()

			scrapeCtx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()

			start := time.Now()
			result, reason, err := c.scrape(scrapeCtx, t.url)
			ippmetrics.RecordKVCacheScrapeDuration(t.modelName, time.Since(start))

			if err != nil {
				logger.Error(err, "scrape failed",
					"model", t.modelName, "url", t.url, "reason", reason)
				ippmetrics.RecordKVCacheScrapeFailure(t.modelName, reason)
				return
			}
			ippmetrics.RecordKVCacheUtilization(t.modelName, result.utilization)
			c.ds.GetOrCreateModel(t.modelName).GetAttributes().Put(
				KVCacheMetricsAttributeKey,
				KVCacheMetrics{
					Utilization:    result.utilization,
					QueueDepth:     result.queueDepth,
					LastObservedAt: time.Now().UnixNano(),
				},
			)
		}(t)
	}
	wg.Wait()
	return nil, nil
}

type target struct {
	modelName string
	url       string
}

func (c *KVCacheCollector) collectTargets() []target {
	models := c.ds.GetModels(func(_ datalayer.Model) bool { return true })
	targets := make([]target, 0, len(models))
	for _, m := range models {
		v, ok := m.GetAttributes().Get(metricsendpoint.AttributeKey)
		if !ok {
			continue
		}
		ep, ok := v.(metricsendpoint.MetricsEndpoint)
		if !ok || ep.URL == "" {
			continue
		}
		targets = append(targets, target{modelName: m.GetName(), url: ep.URL})
	}
	return targets
}

type scrapeResult struct {
	utilization float64
	queueDepth  int64
}

// scrape returns (result, "", nil) on success and (zero, reason, err) on failure.
// reason is one of the failReason* constants used as a label on
// kv_cache_scrape_failures_total.
func (c *KVCacheCollector) scrape(ctx context.Context, url string) (scrapeResult, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return scrapeResult{}, failReasonRequestBuild, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		reason := failReasonDial
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			reason = failReasonTimeout
		}
		return scrapeResult{}, reason, err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return scrapeResult{}, failReasonHTTPStatus, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// LegacyValidation allows traditional metric names that contain colons
	// (e.g. "vllm:kv_cache_usage_perc"). The strict UTF8 scheme rejects them.
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return scrapeResult{}, failReasonParse, fmt.Errorf("parse: %w", err)
	}

	return scrapeResult{
		utilization: readGaugeMax(families, c.utilizationMetric),
		queueDepth:  int64(readGaugeSum(families, c.queueDepthMetric)),
	}, "", nil
}

// readGaugeMax returns the max gauge value across all label sets, or 0 if absent.
// Used for ratio-like signals (utilization) where the hottest engine sets pressure.
func readGaugeMax(fams map[string]*dto.MetricFamily, name string) float64 {
	fam, ok := fams[name]
	if !ok || len(fam.Metric) == 0 {
		return 0
	}
	var maxVal float64
	var seen bool
	for _, m := range fam.Metric {
		if m.Gauge == nil {
			continue
		}
		v := m.Gauge.GetValue()
		if !seen || v > maxVal {
			maxVal = v
			seen = true
		}
	}
	return maxVal
}

// readGaugeSum returns the sum of gauge values across all label sets, or 0 if
// absent. Used for count-like signals (queue depth) where total backlog matters.
func readGaugeSum(fams map[string]*dto.MetricFamily, name string) float64 {
	fam, ok := fams[name]
	if !ok || len(fam.Metric) == 0 {
		return 0
	}
	var sum float64
	for _, m := range fam.Metric {
		if m.Gauge == nil {
			continue
		}
		sum += m.Gauge.GetValue()
	}
	return sum
}
