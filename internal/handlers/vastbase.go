package handlers

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pkg/errors"
	"github.com/siddontang/go-log/log"
	"github.com/unionj-cloud/chameleon/config"
	"github.com/unionj-cloud/chameleon/internal/schema"
	"github.com/unionj-cloud/go-doudou/v2/framework/database"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/dbvendor"
	_ "github.com/unionj-cloud/go-doudou/v2/toolkit/dbvendor/mysql"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/dbvendor/mysql/parser/parser"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/dbvendor/postgres"
	_ "github.com/unionj-cloud/go-doudou/v2/toolkit/dbvendor/postgres"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/sliceutils"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/stringutils"
	"github.com/unionj-cloud/go-doudou/v2/toolkit/zlogger"
	"gorm.io/gorm"
	"regexp"
	"strings"
)

var _ canal.EventHandler = (*VastbaseEventHandler)(nil)

type VastbaseEventHandler struct {
	conf     *config.Config
	vendor   dbvendor.IVendor
	targetDB *gorm.DB
	canal    *canal.Canal
	sourceDB *client.Conn
	regs     []*regexp.Regexp
}

func NewVastbaseEventHandler(conf *config.Config, canal *canal.Canal, sourceDB *client.Conn, regs []*regexp.Regexp) *VastbaseEventHandler {
	return &VastbaseEventHandler{
		vendor:   dbvendor.Registry.GetVendor(conf.Db.Driver),
		targetDB: database.NewDb(conf.Config),
		conf:     conf,
		canal:    canal,
		sourceDB: sourceDB,
		regs:     regs,
	}
}

func (h *VastbaseEventHandler) fetchExistTable(conn *client.Conn) ([]string, error) {
	r, err := conn.Execute("show tables")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer r.Close()

	var existTables []string

	// Direct access to fields
	for _, row := range r.Values {
		for _, val := range row {
			if val.Type == mysql.FieldValueTypeString {
				existTables = append(existTables, string(val.AsString()))
			}
		}
	}

	return existTables, nil
}

func (h *VastbaseEventHandler) Migrate() error {
	if !h.conf.Source.Migrate {
		return nil
	}
	existTables, err := h.fetchExistTable(h.sourceDB)
	if err != nil {
		return errors.WithStack(err)
	}
	database := h.conf.Source.Database
	for _, table := range existTables {
		match := false
		for _, reg := range h.regs {
			match = reg.MatchString(fmt.Sprintf("%s.%s", database, table))
			if match {
				break
			}
		}
		if !match {
			continue
		}
		sourceTable, err := schema.NewTable(h.sourceDB, database, table)
		if err != nil {
			return errors.WithStack(err)
		}
		var targetTable dbvendor.Table
		if err = h.ConvertSourceTable2TargetTable(sourceTable, &targetTable); err != nil {
			return errors.WithStack(err)
		}
		if err = h.vendor.CreateTable(context.Background(), h.targetDB, targetTable); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (h *VastbaseEventHandler) ConvertSourceTable2TargetTable(source *schema.Table, target *dbvendor.Table) error {
	target.Name = strings.ToLower(source.Name)
	target.TablePrefix = h.conf.Db.Table.Prefix
	target.IsCopy = h.conf.Source.Copy
	if len(source.PKColumns) == 0 {
		return errors.WithStack(errors.Errorf("Source table %s should have at least one primary key", source.Name))
	}
	// only support single primary key
	target.Pk = strings.ToLower(source.Columns[source.PKColumns[0]].Name)
	for _, item := range source.Columns {
		isPk := item.Name == target.Pk
		col := dbvendor.Column{
			TablePrefix: target.TablePrefix,
			Table:       target.Name,
			Name:        strings.ToLower(item.Name),
			Default:     item.Default,
			Pk:          isPk,
			Nullable:    item.Nullable,
			Unsigned:    item.IsUnsigned,
			Comment:     item.Comment,
		}
		var err error
		col.Type, err = h.ConvertColType2ColumnType(item.Type)
		if err != nil {
			return errors.WithStack(err)
		}
		if col.Pk && item.Type == schema.TYPE_NUMBER && item.IsAuto {
			col.Type = postgres.SerialType
		}
		target.Columns = append(target.Columns, col)
	}
	return nil
}

func (h *VastbaseEventHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	return nil
}

func (h *VastbaseEventHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	log.Infof("===================== OnTableChanged called =====================")
	h.canal.ClearTableCache([]byte(schema), []byte(table))
	h.canal.ClearTableCache([]byte(schema), []byte(strings.ToLower(table)))
	return nil
}

func (h *VastbaseEventHandler) ConvertDataType2ColumnType(dataType parser.DataType) (vbType string, err error) {
	normalDataType, ok := dataType.(*parser.NormalDataType)
	if !ok {
		return "", errors.WithStack(errors.Errorf("Not support %s yet", dataType.String()))
	}
	typeStr := normalDataType.String()
	switch typeStr {
	case `CHAR`:
		vbType = postgres.VarcharType
	case `CHARACTER`:
		vbType = postgres.VarcharType
	case `VARCHAR`:
		vbType = postgres.VarcharType
	case `TINYTEXT`:
		vbType = postgres.TextType
	case `TEXT`:
		vbType = postgres.TextType
	case `MEDIUMTEXT`:
		vbType = postgres.TextType
	case `LONGTEXT`:
		vbType = postgres.TextType
	case `LONG`:
		vbType = postgres.BigintType
	case `TIME`:
		vbType = postgres.DatetimeType
	case `TIMESTAMP`:
		vbType = postgres.DatetimeType
	case `DATETIME`:
		vbType = postgres.DatetimeType
	case `BINARY`:
		vbType = postgres.BlobType
	case `NCHAR`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `NVARCHAR`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `BIT`:
		vbType = postgres.BitType
	case `VARBINARY`:
		vbType = postgres.BlobType
	case `BLOB`:
		vbType = postgres.BlobType
	case `YEAR`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `DECIMAL`:
		vbType = postgres.DecimalType
	case `DEC`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `FIXED`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `NUMERIC`:
		vbType = postgres.DoubleType
	case `FLOAT`:
		vbType = postgres.DoubleType
	case `FLOAT4`:
		vbType = postgres.FloatType
	case `FLOAT8`:
		vbType = postgres.DoubleType
	case `DOUBLE`:
		vbType = postgres.DoubleType
	case `REAL`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `TINYINT`:
		vbType = postgres.TinyintType
	case `SMALLINT`:
		vbType = postgres.SmallintType
	case `MEDIUMINT`:
		vbType = postgres.MediumintType
	case `INT`:
		vbType = postgres.IntType
	case `INTEGER`:
		vbType = postgres.IntType
	case `BIGINT`:
		vbType = postgres.BigintType
	case `MIDDLEINT`:
		vbType = postgres.MediumintType
	case `INT1`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `INT2`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `INT3`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `INT4`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `INT8`:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	case `DATE`:
		vbType = postgres.DateType
	case `TINYBLOB`:
		vbType = postgres.BlobType
	case `MEDIUMBLOB`:
		vbType = postgres.MediumblobType
	case `LONGBLOB`:
		vbType = postgres.LongblobType
	case `BOOL`:
		vbType = postgres.BoolType
	case `BOOLEAN`:
		vbType = postgres.BoolType
	case `SERIAL`:
		vbType = postgres.SerialType
	default:
		return "", errors.WithStack(errors.Errorf("Not support %s yet", typeStr))
	}
	return
}

func (h *VastbaseEventHandler) ConvertColType2ColumnType(colType int) (vbType string, err error) {
	switch colType {
	case schema.TYPE_STRING:
		vbType = postgres.TextType
	case schema.TYPE_BINARY:
		vbType = postgres.BlobType
	case schema.TYPE_DATETIME:
		vbType = postgres.DatetimeType
	case schema.TYPE_TIME:
		vbType = postgres.DatetimeType
	case schema.TYPE_TIMESTAMP:
		vbType = postgres.DatetimeType
	case schema.TYPE_BIT:
		vbType = postgres.BitType
	case schema.TYPE_DECIMAL:
		vbType = postgres.DecimalType
	case schema.TYPE_FLOAT:
		vbType = postgres.DoubleType
	case schema.TYPE_MEDIUM_INT:
		vbType = postgres.MediumintType
	case schema.TYPE_NUMBER:
		vbType = postgres.BigintType
	case schema.TYPE_DATE:
		vbType = postgres.DateType
	case schema.TYPE_BOOL:
		vbType = postgres.BoolType
	default:
		return "", errors.WithStack(errors.Errorf("Not support %d yet", colType))
	}
	return
}

func (h *VastbaseEventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	log.Infof("===================== OnDDL called =====================")
	p := parser.NewParser(parser.WithDebugMode(true))
	ret, err := p.ParseDDL(string(queryEvent.Query))
	if err != nil {
		zlogger.Err(errors.WithStack(err)).Msg(err.Error())
		return nil
	}
	tables, err := sliceutils.ConvertAny2Interface(ret)
	if err != nil {
		zlogger.Err(errors.WithStack(err)).Msg(err.Error())
		return nil
	}
	for _, item := range tables {
		switch table := item.(type) {
		case *parser.CreateTable:
			err = errors.New("Not support create table ddl statement yet")
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		case *parser.AlterTable:
			tableName := strings.ToLower(table.Name)
			tablePrefix := h.conf.Db.Table.Prefix
			if stringutils.IsEmpty(tablePrefix) {
				tablePrefix = h.conf.Db.Name
			}
			tableColumns := table.Columns
			for _, column := range tableColumns {
				name := strings.ToLower(column.Name)
				def := column.ColumnDefinition
				if def == nil {
					continue
				}
				switch def.Type {
				case parser.AddColumn:
					col := dbvendor.Column{
						TablePrefix: tablePrefix,
						Table:       tableName,
						Name:        name,
						Comment:     def.ColumnConstraint.Comment,
						Nullable:    false,
					}
					vbType, err := h.ConvertDataType2ColumnType(def.DataType)
					if err != nil {
						zlogger.Err(errors.WithStack(err)).Msg(err.Error())
						return nil
					}
					col.Type = vbType
					if def.ColumnConstraint.HasDefaultValue {
						col.Default = &def.ColumnConstraint.DefaultValue
					}
					if !def.ColumnConstraint.NotNull {
						col.Nullable = true
					}
					err = h.vendor.AddColumn(context.Background(), h.targetDB, col)
					if err != nil {
						zlogger.Err(errors.WithStack(err)).Msg(err.Error())
						return nil
					}
				case parser.ModifyColumn:
					col := dbvendor.Column{
						TablePrefix: tablePrefix,
						Table:       tableName,
						Name:        name,
						Comment:     def.ColumnConstraint.Comment,
						Nullable:    false,
					}
					vbType, err := h.ConvertDataType2ColumnType(def.DataType)
					if err != nil {
						zlogger.Err(errors.WithStack(err)).Msg(err.Error())
						return nil
					}
					col.Type = vbType
					if def.ColumnConstraint.HasDefaultValue {
						col.Default = &def.ColumnConstraint.DefaultValue
					}
					if !def.ColumnConstraint.NotNull {
						col.Nullable = true
					}
					err = h.vendor.ChangeColumn(context.Background(), h.targetDB, col)
					if err != nil {
						zlogger.Err(errors.WithStack(err)).Msg(err.Error())
						return nil
					}
				case parser.DropColumn:
					err = errors.New("Not support drop column ddl statement yet")
					zlogger.Err(errors.WithStack(err)).Msg(err.Error())
					return nil
				}
			}
		}
	}
	return nil
}

func (h *VastbaseEventHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	return nil
}

func (h *VastbaseEventHandler) OnGTID(header *replication.EventHeader, gtid mysql.GTIDSet) error {
	return nil
}

func (h *VastbaseEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *VastbaseEventHandler) OnRow(e *canal.RowsEvent) error {
	log.Infof("%s %v\n", e.Action, e.Rows)
	tableName := e.Table.Name
	tablePrefix := h.conf.Db.Table.Prefix
	if stringutils.IsEmpty(tablePrefix) {
		tablePrefix = h.conf.Db.Name
	}
	dml := dbvendor.DMLSchema{
		Schema:      h.conf.Db.Name,
		TablePrefix: tablePrefix,
		TableName:   tableName,
		Pk: dbvendor.Column{
			Name: "id",
		},
	}
	switch e.Action {
	case canal.InsertAction:
		tableColumns := e.Table.Columns
		for _, v := range tableColumns {
			dml.InsertColumns = append(dml.InsertColumns, dbvendor.Column{
				Table: tableName,
				Name:  strings.ToLower(v.Name),
			})
		}
		statement, err := h.vendor.GetInsertStatement(dml)
		if err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
		sqlDB, err := h.targetDB.DB()
		if err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
		for _, row := range e.Rows {
			if _, err = sqlDB.ExecContext(context.Background(), statement, row...); err != nil {
				zlogger.Err(errors.WithStack(err)).Msg(err.Error())
				return nil
			}
		}
	case canal.UpdateAction:
		tableColumns := e.Table.Columns
		for _, v := range tableColumns {
			dml.UpdateColumns = append(dml.UpdateColumns, dbvendor.Column{
				Table: tableName,
				Name:  strings.ToLower(v.Name),
			})
		}
		statement, err := h.vendor.GetUpdateStatement(dml)
		if err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
		sqlDB, err := h.targetDB.DB()
		if err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
		if len(e.Rows) < 2 {
			return nil
		}
		row := e.Rows[1]
		pkVals, err := e.Table.GetPKValues(row)
		if err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
		if len(pkVals) == 0 {
			return nil
		}
		row = append(row, pkVals[0])
		if _, err = sqlDB.ExecContext(context.Background(), statement, row...); err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
	case canal.DeleteAction:
		pkVals, err := e.Table.GetPKValues(e.Rows[0])
		if err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
		if len(pkVals) == 0 {
			return nil
		}
		if err = h.vendor.Delete(context.Background(), h.targetDB, dml, pkVals[0]); err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
	}
	return nil
}

func (h *VastbaseEventHandler) String() string {
	return "VastbaseEventHandler"
}
