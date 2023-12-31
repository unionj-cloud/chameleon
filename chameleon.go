package chameleon

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/unionj-cloud/chameleon/config"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/zlogger"
)

var (
	handlerRegistry = make(map[string]IEventHandler)
)

func RegisterHandler(handler IEventHandler) {
	handlerRegistry[handler.String()] = handler
}

func GetHandlerRegistry() map[string]IEventHandler {
	return handlerRegistry
}

var c *canal.Canal

func GetCanal() *canal.Canal {
	return c
}

func Close() {
	if c != nil {
		c.Close()
		c = nil
	}
	for _, v := range handlerRegistry {
		if v != nil {
			v.Close()
		}
	}
}

type IEventHandler interface {
	canal.EventHandler
	Migrate() error
	Close()
}

func init() {
	conf := config.G_Config
	cfg := canal.NewDefaultConfig()
	cfg.Addr = conf.Source.Addr
	cfg.User = conf.Source.User
	cfg.Password = conf.Source.Pass
	cfg.Dump.Databases = []string{conf.Source.Database}
	cfg.IncludeTableRegex = conf.Source.IncludeTableRegex
	cfg.ExcludeTableRegex = conf.Source.ExcludeTableRegex

	var err error
	c, err = canal.NewCanal(cfg)
	if err != nil {
		zlogger.Panic().Msg(err.Error())
	}
}
