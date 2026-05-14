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

package v1alpha1

import (
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true

// PayloadProcessorConfig is the Schema for the payloadprocessorconfig API
type PayloadProcessorConfig struct {
	metav1.TypeMeta `json:",inline"`

	// +required
	// +kubebuilder:validation:Required
	// Plugins is the list of plugins that will be instantiated.
	Plugins []PluginSpec `json:"plugins"`
}

func (cfg PayloadProcessorConfig) String() string {
	var parts []string

	if len(cfg.Plugins) > 0 {
		parts = append(parts, fmt.Sprintf("Plugins: %v", cfg.Plugins))
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

// PluginSpec contains the information that describes a plugin that
// will be instantiated.
type PluginSpec struct {
	// +optional
	// Name provides a name for plugin entries to reference. If
	// omitted, the value of the Plugin's Type field will be used.
	Name string `json:"name,omitempty"`

	// +required
	// +kubebuilder:validation:Required
	// Type specifies the plugin type to be instantiated.
	Type string `json:"type,omitempty"`

	// +optional
	// Parameters are the set of parameters to be passed to the plugin's
	// factory function. The factory function is responsible
	// to parse the parameters.
	Parameters json.RawMessage `json:"parameters"`
}

func (ps PluginSpec) String() string {
	var parts []string
	if ps.Name != "" {
		parts = append(parts, "Name: "+ps.Name)
	}
	parts = append(parts, "Type: "+ps.Type)
	if len(ps.Parameters) > 0 {
		parts = append(parts, "Parameters: "+string(ps.Parameters))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
