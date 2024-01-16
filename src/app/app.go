package app

import (
	"fmt"
	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
	"os"
	"regexp"
	"strings"
)

type Config struct {
	Kafka struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Consumer struct {
			GroupId string `yaml:"group-id"`
		} `json:"consumer"`
	} `yaml:"kafka"`
	Clickhouse struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
	} `yaml:"clickhouse"`
	Topic        []string `yaml:"topic"`
	Fields       string   `yaml:"fields"`
	Sink         string   `yaml:"sink"`
	Pattern      string   `yaml:"pattern"`
	PatternIndex []string `yaml:"pattern-index"`
	Debug        bool     `yaml:"debug"`
	Output       struct {
		Format string `yaml:"format"`
	} `yaml:"output"`

	columns []string
	regexp  *regexp.Regexp
}

func (config *Config) Columns() []string {
	if config.columns == nil {
		var columns []string
		for _, it := range strings.Split(config.Fields, ",") {
			columns = append(columns, strings.TrimSpace(it))
		}
		config.columns = columns
	}
	return config.columns
}

func (config *Config) Regexp() *regexp.Regexp {
	if config.regexp == nil {
		config.regexp = regexp.MustCompile(config.Pattern)
	}
	return config.regexp
}

var app Config

// SetConfig GLOBAL variable
func SetConfig(fileName string) {
	yamlFile, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println("[Config] Error reading YAML file:", err)
		return
	}
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		color.Red("[Config] Error parse YAML file: %v", err)
		return
	}
	app = config
}

// Get GLOBAL variable
func Get() *Config {
	return &app
}
