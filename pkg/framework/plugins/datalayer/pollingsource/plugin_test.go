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
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	dlsrc "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/datasource"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fakeCollector counts how many times Poll has been called.
type fakeCollector struct {
	name      string
	pollCount atomic.Int64
}

func (f *fakeCollector) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "fake-collector", Name: f.name}
}

func (f *fakeCollector) Poll(_ context.Context) (any, error) {
	f.pollCount.Add(1)
	return nil, nil
}

// TestNew_EmptyName verifies that New returns an error when given an empty name.
func TestNew_EmptyName(t *testing.T) {
	_, err := New("")
	if err == nil {
		t.Error("expected error for empty name, got nil")
	}
}

// TestStart_AlreadyStarted verifies that calling Start on an already-running PollingSource returns an error.
func TestStart_AlreadyStarted(t *testing.T) {
	src, err := New("test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := src.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer src.Stop()

	if err := src.Start(ctx); err == nil {
		t.Error("expected error on second Start, got nil")
	}
}

// fakeHandle implements plugin.Handle for unit tests.
type fakeHandle struct {
	plugins map[string]plugin.Plugin
}

func newFakeHandle(plugins map[string]plugin.Plugin) *fakeHandle {
	if plugins == nil {
		plugins = map[string]plugin.Plugin{}
	}
	return &fakeHandle{plugins: plugins}
}

func (f *fakeHandle) Context() context.Context                         { return context.Background() }
func (f *fakeHandle) Client() client.Client                            { return nil }
func (f *fakeHandle) ReconcilerBuilder() *ctrlbuilder.Builder          { return nil }
func (f *fakeHandle) Datastore() datalayer.Datastore                   { return nil }
func (f *fakeHandle) Plugin(name string) plugin.Plugin                 { return f.plugins[name] }
func (f *fakeHandle) AddPlugin(name string, p plugin.Plugin)           { f.plugins[name] = p }
func (f *fakeHandle) GetAllPlugins() []plugin.Plugin                   { return nil }
func (f *fakeHandle) GetAllPluginsWithNames() map[string]plugin.Plugin { return f.plugins }

// notACollector satisfies plugin.Plugin but not dlsrc.Collector.
type notACollector struct{}

func (n *notACollector) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "not-a-collector", Name: "not-a-collector"}
}

// Factory with nil parameters creates a PollingSource with no collectors registered.
func TestFactoryNoParameters(t *testing.T) {
	h := newFakeHandle(nil)
	p, err := Factory("src", nil, h)
	if err != nil {
		t.Fatalf("Factory returned unexpected error: %v", err)
	}
	src, ok := p.(*PollingSource)
	if !ok {
		t.Fatalf("expected *PollingSource, got %T", p)
	}
	if len(src.collectors) != 0 {
		t.Errorf("expected 0 collectors, got %d", len(src.collectors))
	}
}

// Factory with an empty JSON object creates a PollingSource with no collectors registered.
func TestFactoryEmptyParameters(t *testing.T) {
	h := newFakeHandle(nil)
	p, err := Factory("src", json.RawMessage(`{}`), h)
	if err != nil {
		t.Fatalf("Factory returned unexpected error: %v", err)
	}
	src := p.(*PollingSource)
	if len(src.collectors) != 0 {
		t.Errorf("expected 0 collectors, got %d", len(src.collectors))
	}
}

// Factory resolves pluginRefs to Collector instances and registers them in order with their configured frequencies.
func TestFactoryWithCollectors(t *testing.T) {
	c1 := &fakeCollector{name: "col-a"}
	c2 := &fakeCollector{name: "col-b"}
	h := newFakeHandle(map[string]plugin.Plugin{
		"col-a": c1,
		"col-b": c2,
	})

	params := json.RawMessage(`{"collectors":[{"pluginRef":"col-a","frequency":1},{"pluginRef":"col-b","frequency":2}]}`)
	p, err := Factory("src", params, h)
	if err != nil {
		t.Fatalf("Factory returned unexpected error: %v", err)
	}
	src := p.(*PollingSource)
	if len(src.collectors) != 2 {
		t.Fatalf("expected 2 collectors, got %d", len(src.collectors))
	}
	if src.collectors[0].collector != dlsrc.Collector(c1) {
		t.Errorf("expected collectors[0] to be c1")
	}
	if src.collectors[1].collector != dlsrc.Collector(c2) {
		t.Errorf("expected collectors[1] to be c2")
	}
	if src.collectors[0].frequency != 1*time.Second {
		t.Errorf("expected collectors[0] frequency 1s, got %v", src.collectors[0].frequency)
	}
	if src.collectors[1].frequency != 2*time.Second {
		t.Errorf("expected collectors[1] frequency 2s, got %v", src.collectors[1].frequency)
	}
}

// Factory returns an error when the parameters JSON is malformed.
func TestFactoryInvalidJSON(t *testing.T) {
	h := newFakeHandle(nil)
	_, err := Factory("src", json.RawMessage(`{invalid`), h)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

// Factory returns an error when a pluginRef names a plugin not registered in the handle.
func TestFactoryUnknownCollectorRef(t *testing.T) {
	h := newFakeHandle(nil)
	params := json.RawMessage(`{"collectors":[{"pluginRef":"does-not-exist","frequency":1}]}`)
	_, err := Factory("src", params, h)
	if err == nil {
		t.Fatal("expected error for unknown collector ref, got nil")
	}
}

// Factory returns an error when a referenced plugin does not implement the Collector interface.
func TestFactoryPluginNotCollector(t *testing.T) {
	h := newFakeHandle(map[string]plugin.Plugin{
		"wrong-type": &notACollector{},
	})
	params := json.RawMessage(`{"collectors":[{"pluginRef":"wrong-type","frequency":1}]}`)
	_, err := Factory("src", params, h)
	if err == nil {
		t.Fatal("expected error when plugin does not implement Collector, got nil")
	}
}

// Factory returns an error when a collector has a zero frequency.
func TestFactoryZeroFrequency(t *testing.T) {
	c := &fakeCollector{name: "col"}
	h := newFakeHandle(map[string]plugin.Plugin{"col": c})
	params := json.RawMessage(`{"collectors":[{"pluginRef":"col","frequency":0}]}`)
	_, err := Factory("src", params, h)
	if err == nil {
		t.Fatal("expected error for zero frequency, got nil")
	}
}

// Factory returns an error when a collector is configured with a negative frequency.
func TestFactoryNegativeFrequency(t *testing.T) {
	c := &fakeCollector{name: "col"}
	h := newFakeHandle(map[string]plugin.Plugin{"col": c})
	params := json.RawMessage(`{"collectors":[{"pluginRef":"col","frequency":-5}]}`)
	_, err := Factory("src", params, h)
	if err == nil {
		t.Fatal("expected error for negative frequency, got nil")
	}
}

// TestRegisterCollector_InvalidFrequency verifies that a non-positive frequency is rejected and the collector is not registered.
func TestRegisterCollector_InvalidFrequency(t *testing.T) {
	src, _ := New("test")
	c := &fakeCollector{name: "c"}
	src.RegisterCollector(c, 0)
	src.RegisterCollector(c, -1*time.Second)

	ps := src.(*PollingSource)
	if len(ps.collectors) != 0 {
		t.Errorf("expected 0 collectors after invalid registrations, got %d", len(ps.collectors))
	}
}

// TestStopBeforeStart verifies that calling Start after Stop returns an error.
func TestStopBeforeStart(t *testing.T) {
	src, err := New("test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.Stop()
	if err := src.Start(context.Background()); err == nil {
		t.Error("expected error when calling Start after Stop, got nil")
	}
}

// TestRegisterCollectorAfterStart verifies that a collector registered after Start is still polled.
func TestRegisterCollectorAfterStart(t *testing.T) {
	src, _ := New("test")
	pre := &fakeCollector{name: "pre"}
	src.RegisterCollector(pre, 10*time.Millisecond)

	if err := src.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	post := &fakeCollector{name: "post"}
	src.RegisterCollector(post, 10*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	src.Stop()

	if got := post.pollCount.Load(); got < 1 {
		t.Errorf("expected post-start collector to be polled at least once, got %d", got)
	}
}
