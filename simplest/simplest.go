package simplest

import (
	"fmt"
	"github.com/grafana/loki/pkg/logentry/stages"
	"github.com/prometheus/common/model"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/mitchellh/mapstructure"
)

var descriptor =  stages.PluginDescriptor{
	Name:    "simplest",
	Version : "0.0.1",
	Stagger:  NewStage,
}

func Descriptor() stages.PluginDescriptor {
	return descriptor
}

type Config struct {
	Source string `mapstructure:"source"`
}

func NewStage(stgCfg *stages.StageConfig) (stages.Stage, error) {
	cfgs := &Config{}
	err := mapstructure.Decode(stgCfg.Config, cfgs)
	if err != nil {
		return nil, err
	}

	ss := &simplestStage{
		logger: stgCfg.Logger,
		source: cfgs.Source,
	}

	return ss, nil
}

type simplestStage struct {
	logger log.Logger
	source string
}

func (m *simplestStage) Process(labels model.LabelSet, source map[string]interface{}, time *time.Time, entry *string) {
	if v, ok := source[m.source]; ok {
		source[m.source] = fmt.Sprintf("simplest->%v", v)
	}
}

func (m *simplestStage) Name() string {
	return descriptor.Name
}
