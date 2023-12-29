package handlers_test

import (
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/integralist/go-findroot/find"
	"github.com/unionj-cloud/chameleon/config"
	"github.com/unionj-cloud/chameleon/internal/handlers"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/yaml"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/zlogger"
	"path/filepath"
	"regexp"
	"testing"
)

var conf *config.Config

func TestMain(m *testing.M) {
	LoadConfigFromLocal()
	conf = config.LoadFromEnv()
	m.Run()
}

func LoadConfigFromLocal() {
	root, err := find.Repo()
	if err != nil {
		panic(err)
	}
	yaml.LoadFile(filepath.Join(root.Path, "app-local.yml"))
}

func TestVastbaseEventHandler_Migrate(t *testing.T) {
	conn, err := client.Connect(conf.Source.Addr, conf.Source.User, conf.Source.Pass, conf.Source.Database)
	if err != nil {
		zlogger.Fatal().Msg(err.Error())
	}
	var regs []*regexp.Regexp
	for _, item := range conf.Source.IncludeTableRegex {
		reg, err := regexp.Compile(item)
		if err != nil {
			zlogger.Fatal().Msg(err.Error())
		}
		regs = append(regs, reg)
	}
	eventHandler := handlers.NewVastbaseEventHandler(conf, nil, conn, regs, &handlers.Hooks{})
	if err := eventHandler.Migrate(); err != nil {
		panic(err)
	}
}
