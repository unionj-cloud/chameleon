/**
* Generated by go-doudou v2.2.1.
* You can edit it as your need.
 */
package main

import (
	_ "github.com/unionj-cloud/chameleon/plugin"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/zlogger"

	"github.com/unionj-cloud/go-doudou/v2/framework/grpcx"
	"github.com/unionj-cloud/go-doudou/v2/framework/plugin"
	"github.com/unionj-cloud/go-doudou/v2/framework/rest"
)

func main() {
	srv := rest.NewRestServer()
	grpcServer := grpcx.NewEmptyGrpcServer()
	for _, v := range plugin.GetServicePlugins() {
		v.Initialize(srv, grpcServer, nil)
	}
	defer func() {
		if r := recover(); r != nil {
			zlogger.Info().Msgf("Recovered. Error:\n", r)
		}
		for _, v := range plugin.GetServicePlugins() {
			v.Close()
		}
	}()
	go func() {
		grpcServer.Run()
	}()
	srv.Run()
}
