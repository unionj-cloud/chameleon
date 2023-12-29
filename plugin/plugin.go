/**
* Generated by go-doudou v2.2.1.
* You can edit it as your need.
 */
package plugin

import (
	"context"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/unionj-cloud/chameleon/config"
	"github.com/unionj-cloud/chameleon/internal/handlers"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/zlogger"
	"os"
	"regexp"

	"github.com/unionj-cloud/go-doudou/v2/framework/grpcx"
	"github.com/unionj-cloud/go-doudou/v2/framework/plugin"
	"github.com/unionj-cloud/go-doudou/v2/framework/rest"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/pipeconn"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/stringutils"
)

var _ plugin.ServicePlugin = (*CanalPlugin)(nil)

type CanalPlugin struct {
}

func (receiver *CanalPlugin) Close() {
}

func (receiver *CanalPlugin) GoDoudouServicePlugin() {

}

func (receiver *CanalPlugin) GetName() string {
	name := os.Getenv("GDD_SERVICE_NAME")
	if stringutils.IsEmpty(name) {
		name = "com.hundsun.ecod.canal"
	}
	return name
}

func (receiver *CanalPlugin) Initialize(_ *rest.RestServer, _ *grpcx.GrpcServer, _ pipeconn.DialContextFunc) {
	conf := config.LoadFromEnv()
	cfg := canal.NewDefaultConfig()
	cfg.Addr = conf.Source.Addr
	cfg.User = conf.Source.User
	cfg.Password = conf.Source.Pass
	cfg.Dump.Databases = []string{conf.Source.Database}
	cfg.IncludeTableRegex = conf.Source.IncludeTableRegex
	cfg.ExcludeTableRegex = conf.Source.ExcludeTableRegex

	c, err := canal.NewCanal(cfg)
	if err != nil {
		zlogger.Fatal().Msg(err.Error())
	}

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

	hooks := handlers.Hooks{}
	eventHandler := handlers.NewVastbaseEventHandler(conf, c, conn, regs, &hooks)
	if conf.Source.Migrate {
		if err = eventHandler.Migrate(); err != nil {
			zlogger.Fatal().Msg(err.Error())
		}
	}
	// Register a handler to handle RowsEvent
	c.SetEventHandler(eventHandler)

	if conf.Source.Dump {
		if err = c.Dump(); err != nil {
			zlogger.Fatal().Msg(err.Error())
		}
		if hooks.AfterDumpHook != nil {
			if err = hooks.AfterDumpHook(context.Background()); err != nil {
				zlogger.Fatal().Msg(err.Error())
			}
		}
	}

	if conf.Source.Sync {
		go func() {
			if conf.Source.Dump {
				if err := c.Run(); err != nil {
					panic(err)
				}
			} else {
				pos, err := c.GetMasterPos()
				if err != nil {
					zlogger.Fatal().Msg(err.Error())
				}
				if err := c.RunFrom(pos); err != nil {
					panic(err)
				}
			}
		}()
	}
}

func init() {
	plugin.RegisterServicePlugin(&CanalPlugin{})
}
