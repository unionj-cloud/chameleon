package handlers

import (
	"context"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/dbvendor"
	"gorm.io/gorm"
)

type Hooks struct {
	AfterCreateTableHook func(ctx context.Context, db *gorm.DB, table dbvendor.Table) error
	AfterDumpHook        func(ctx context.Context) error
}
