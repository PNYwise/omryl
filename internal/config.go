package internal

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config mewakili konfigurasi untuk node Raft.
type Config struct {
	NodeID      string `yaml:"node_id"`
	RaftAddr    string `yaml:"raft_addr"`
	HTTPAddr    string `yaml:"http_addr"`
	IsBootstrap bool   `yaml:"is_bootstrap"`
	DataDir     string `yaml:"data_dir"`

	// Public/simple tokens
	PublicAPIToken  string `yaml:"public_api_token"`
	PublicJoinToken string `yaml:"public_join_token"`

	// Internal HMAC
	InternalID           string `yaml:"internal_id"`
	InternalSecret       string `yaml:"internal_secret"`
	InternalClockSkewSec int    `yaml:"internal_clock_skew_sec"`
}

// LoadConfig memuat konfigurasi dari file YAML.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("gagal membaca config file %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("gagal parse config yaml: %w", err)
	}
	if cfg.DataDir == "" {
		cfg.DataDir = fmt.Sprintf("./raft-data-%s", cfg.NodeID)
	}
	return &cfg, nil
}
