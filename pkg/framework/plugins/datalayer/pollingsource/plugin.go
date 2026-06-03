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

package pollingsource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/llm-d/llm-d-inference-payload-processor/apix/config/v1alpha1"
	dlsrc "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/datasource"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
)

const PluginType = "polling-source"

// PollingSourceConfig is the JSON configuration structure for the PollingSource plugin.
type PollingSourceConfig struct {
	Collectors []CollectorConfig `json:"collectors"`
}

// CollectorConfig references a collector plugin and the polling interval in whole seconds.
type CollectorConfig struct {
	v1alpha1.PluginRef
	Frequency int64 `json:"frequency"` // seconds
}

// compile-time interface assertion
var _ dlsrc.PollingSource = &PollingSource{}

type collectorEntry struct {
	collector dlsrc.Collector
	frequency time.Duration
}

// PollingSource drives one goroutine per registered Collector, each ticking at
// its configured frequency. Collectors are polled after a random jitter in
// [0, frequency) to spread initial load, then on every subsequent tick.
// RegisterCollector may be called before or after Start; Start and Stop must
// each be called at most once.
type PollingSource struct {
	name       plugin.TypedName
	collectors []collectorEntry
	mu         sync.Mutex

	started bool
	stopped bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Factory is the factory function for PollingSource.
func Factory(name string, rawParameters json.RawMessage, handle plugin.Handle) (plugin.Plugin, error) {
	var config PollingSourceConfig
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' plugin - %w", PluginType, err)
		}
	}

	collectors := make([]dlsrc.Collector, 0, len(config.Collectors))
	for _, cc := range config.Collectors {
		if cc.Frequency <= 0 {
			return nil, fmt.Errorf("%s plugin: collector %q frequency must be positive, got %d", PluginType, cc.PluginRef.PluginRef, cc.Frequency)
		}
		p := handle.Plugin(cc.PluginRef.PluginRef)
		if p == nil {
			return nil, fmt.Errorf("%s plugin: collector %q not found", PluginType, cc.PluginRef.PluginRef)
		}
		c, ok := p.(dlsrc.Collector)
		if !ok {
			return nil, fmt.Errorf("%s plugin: plugin %q does not implement Collector", PluginType, cc.PluginRef.PluginRef)
		}
		collectors = append(collectors, c)
	}

	src, err := New(name)
	if err != nil {
		return nil, fmt.Errorf("%s plugin: failed to create: %w", PluginType, err)
	}

	for i, cc := range config.Collectors {
		src.RegisterCollector(collectors[i], time.Duration(cc.Frequency)*time.Second)
	}

	return src, nil
}

// New creates a PollingSource that drives each registered Collector at its configured frequency.
func New(name string) (dlsrc.PollingSource, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required for plugin '%s'", PluginType)
	}
	return &PollingSource{
		name: plugin.TypedName{Type: PluginType, Name: name},
	}, nil
}

func (p *PollingSource) TypedName() plugin.TypedName { return p.name }

// RegisterCollector adds a Collector to be polled at the given frequency.
// Safe to call before or after Start. No-op for the goroutine if called after Stop.
// Logs an error and skips registration if frequency is not positive.
func (p *PollingSource) RegisterCollector(c dlsrc.Collector, frequency time.Duration) {
	if frequency <= 0 {
		log.Log.Error(nil, "pollingsource: skipping collector with non-positive frequency",
			"collector", c.TypedName(), "frequency", frequency)
		return
	}
	p.mu.Lock()
	p.collectors = append(p.collectors, collectorEntry{collector: c, frequency: frequency})
	if p.started && !p.stopped {
		// Single late-registered collector: jitter capped at its own frequency.
		p.wg.Add(1)
		go p.runCollector(p.ctx, c, frequency, frequency)
	}
	p.mu.Unlock()
}

// Start launches one polling goroutine per registered Collector. Returns an error if called more than once or after Stop.
func (p *PollingSource) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return errors.New("PollingSource already started")
	}
	if p.stopped {
		p.mu.Unlock()
		return errors.New("PollingSource already stopped")
	}
	ctx, p.cancel = context.WithCancel(ctx)
	p.ctx = ctx
	p.started = true
	snapshot := make([]collectorEntry, len(p.collectors))
	copy(snapshot, p.collectors)
	p.mu.Unlock()

	// Spread startups: allow at most 5 collectors per second on average,
	// capped at each collector's own frequency so no collector waits longer
	// than one full interval before its first poll.
	maxJitter := time.Duration(len(snapshot)) * 200 * time.Millisecond
	for _, entry := range snapshot {
		jitterCap := maxJitter
		if entry.frequency < jitterCap {
			jitterCap = entry.frequency
		}
		p.wg.Add(1)
		go p.runCollector(ctx, entry.collector, entry.frequency, jitterCap)
	}
	return nil
}

// Stop cancels all polling goroutines and waits for them to exit.
func (p *PollingSource) Stop() {
	p.mu.Lock()
	p.stopped = true
	cancel := p.cancel
	p.mu.Unlock()

	if cancel != nil {
		cancel()
		p.wg.Wait()
	}
}

func (p *PollingSource) runCollector(ctx context.Context, c dlsrc.Collector, freq, jitterCap time.Duration) {
	defer p.wg.Done()
	logger := log.FromContext(ctx).WithName("polling-source").
		WithValues("collector", c.TypedName(), "frequency", freq)

	poll := func() {
		if _, err := c.Poll(ctx); err != nil {
			logger.Error(err, "collector poll failed")
		}
	}

	// Spread initial polls across [0, jitterCap) to avoid thundering herd.
	if jitterCap > 0 {
		jitter := time.Duration(rand.Int64N(int64(jitterCap)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter):
		}
	}

	poll()

	ticker := time.NewTicker(freq)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}
