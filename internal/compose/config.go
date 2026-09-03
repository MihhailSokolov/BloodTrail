// SPDX-License-Identifier: Apache-2.0

// Package compose reads a resolved docker compose project and produces the
// override file and .env edits BloodTrail needs. It never rewrites the
// operator's own compose file.
package compose

import (
	"encoding/json"
	"fmt"
)

// Config is the subset of `docker compose config --format json` we use.
type Config struct {
	Name     string             `json:"name"`
	Services map[string]Service `json:"services"`
	Networks map[string]Network `json:"networks"`
}

type Service struct {
	Image       string            `json:"image"`
	Environment map[string]string `json:"environment"`
}

type Network struct {
	Name string `json:"name"`
}

// ParseConfig decodes the JSON printed by `docker compose config --format json`.
func ParseConfig(data []byte) (Config, error) {
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing compose config: %w", err)
	}
	if cfg.Name == "" {
		return Config{}, fmt.Errorf("compose config has no project name")
	}
	return cfg, nil
}

// DefaultNetworkName is the network the tool-API helper container joins.
func (c Config) DefaultNetworkName() string {
	if n, ok := c.Networks["default"]; ok && n.Name != "" {
		return n.Name
	}
	return c.Name + "_default"
}
