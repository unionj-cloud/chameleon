/**
* Generated by go-doudou v2.2.1.
* You can edit it as your need.
 */
package plugin

import (
	rice "github.com/GeertJohan/go.rice"
	"github.com/unionj-cloud/chameleon"
	"github.com/unionj-cloud/chameleon/config"
	_ "github.com/unionj-cloud/chameleon/internal/handlers"
	gddconf "github.com/unionj-cloud/go-doudou/v2/framework/config"
	"github.com/unionj-cloud/go-doudou/v2/framework/grpcx"
	"github.com/unionj-cloud/go-doudou/v2/framework/plugin"
	"github.com/unionj-cloud/go-doudou/v2/framework/rest"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/pipeconn"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/stringutils"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/zlogger"
	"net/http"
	"os"
)

var _ plugin.ServicePlugin = (*CanalPlugin)(nil)

type CanalPlugin struct {
}

func (receiver *CanalPlugin) Close() {
	chameleon.Close()
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

func (receiver *CanalPlugin) Initialize(restServer *rest.RestServer, _ *grpcx.GrpcServer, _ pipeconn.DialContextFunc) {
	defer func() {
		if r := recover(); r != nil {
			zlogger.Info().Msgf("Recovered. Error:\n", r)
			receiver.Close()
		}
	}()
	assetHandler := http.FileServer(rice.MustFindBox("../web/dist").HTTPBox())
	restServer.AddRoute(rest.Route{
		Name:        "Home",
		Method:      http.MethodGet,
		Pattern:     "",
		HandlerFunc: http.StripPrefix(gddconf.GddConfig.RouteRootPath, assetHandler).ServeHTTP,
	})
	restServer.AddRoute(rest.Route{
		Name:        "Css",
		Method:      http.MethodGet,
		Pattern:     "/css/*",
		HandlerFunc: http.StripPrefix(gddconf.GddConfig.RouteRootPath, assetHandler).ServeHTTP,
	})
	restServer.AddRoute(rest.Route{
		Name:        "Js",
		Method:      http.MethodGet,
		Pattern:     "/js/*",
		HandlerFunc: http.StripPrefix(gddconf.GddConfig.RouteRootPath, assetHandler).ServeHTTP,
	})
	restServer.AddRoute(rest.Route{
		Name:        "Scripts",
		Method:      http.MethodGet,
		Pattern:     "/scripts/*",
		HandlerFunc: http.StripPrefix(gddconf.GddConfig.RouteRootPath, assetHandler).ServeHTTP,
	})
	restServer.AddRoute(rest.Route{
		Name:        "Img",
		Method:      http.MethodGet,
		Pattern:     "/img/*",
		HandlerFunc: http.StripPrefix(gddconf.GddConfig.RouteRootPath, assetHandler).ServeHTTP,
	})
	restServer.AddRoute(rest.Route{
		Name:        "Fonts",
		Method:      http.MethodGet,
		Pattern:     "/fonts/*",
		HandlerFunc: http.StripPrefix(gddconf.GddConfig.RouteRootPath, assetHandler).ServeHTTP,
	})
	conf := config.G_Config
	eventHandler, ok := chameleon.GetHandlerRegistry()[conf.Source.Handler]
	if !ok {
		zlogger.Panic().Msgf("Handler %s not found", conf.Source.Handler)
	}
	if conf.Source.Migrate {
		if err := eventHandler.Migrate(); err != nil {
			zlogger.Panic().Msg(err.Error())
		}
	}

	// Register a handler to handle RowsEvent
	chameleon.GetCanal().SetEventHandler(eventHandler)

	if conf.Source.Dump {
		if err := chameleon.GetCanal().Dump(); err != nil {
			zlogger.Panic().Msg(err.Error())
		}
	}

	if conf.Source.Sync {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					zlogger.Info().Msgf("Recovered. Error:\n", r)
					receiver.Close()
				}
			}()
			if conf.Source.Dump {
				if err := chameleon.GetCanal().Run(); err != nil {
					panic(err)
				}
			} else {
				pos, err := chameleon.GetCanal().GetMasterPos()
				if err != nil {
					zlogger.Panic().Msg(err.Error())
				}
				if err := chameleon.GetCanal().RunFrom(pos); err != nil {
					zlogger.Panic().Msg(err.Error())
				}
			}
		}()
	}
}

func init() {
	plugin.RegisterServicePlugin(&CanalPlugin{})
}
