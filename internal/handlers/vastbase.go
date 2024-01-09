package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/siddontang/go-log/log"
	"github.com/unionj-cloud/chameleon"
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
	"math"
	"regexp"
	"strings"
	"sync"
)

var _ chameleon.IEventHandler = (*VastbaseEventHandler)(nil)

type DumpTask struct {
	Dml  dbvendor.DMLSchema
	Args []interface{}
}

type VastbaseEventHandler struct {
	conf            *config.Config
	vendor          dbvendor.IVendor
	targetDB        *gorm.DB
	sourceDB        map[string]*client.Conn
	includeRegs     []*regexp.Regexp
	excludeRegs     []*regexp.Regexp
	dumping         bool
	dumpTasks       []*DumpTask
	taskPool        sync.Pool
	targetSchemas   []string
	sourceTargetMap map[string]string
}

func (h *VastbaseEventHandler) Close() {
	if h.targetDB != nil {
		sqlDB, _ := h.targetDB.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}
	if h.sourceDB != nil && len(h.sourceDB) > 0 {
		for _, v := range h.sourceDB {
			v.Close()
		}
	}
}

func init() {
	conf := config.G_Config
	sourceDB := make(map[string]*client.Conn)
	var targetSchemas []string
	sourceTargetMap := make(map[string]string)
	targetDB := database.NewDb(conf.Config)
	for _, item := range conf.Source.Databases {
		conn, err := client.Connect(conf.Source.Addr, conf.Source.User, conf.Source.Pass, item)
		if err != nil {
			zlogger.Panic().Msg(err.Error())
		}
		sourceDB[item] = conn
		target := fmt.Sprintf("%s%s", item, conf.Source.TargetDbSuffix)
		var count int
		if err = targetDB.WithContext(context.Background()).Raw(`SELECT count(1) FROM information_schema.schemata WHERE schema_name = $1;`, target).Scan(&count).Error; err != nil {
			zlogger.Panic().Msg(err.Error())
		}
		if count == 0 {
			if err = targetDB.WithContext(context.Background()).Exec(fmt.Sprintf(`CREATE SCHEMA "%s";`, target)).Error; err != nil {
				zlogger.Panic().Msg(err.Error())
			}
		}
		targetSchemas = append(targetSchemas, target)
		sourceTargetMap[item] = target
	}
	var includeRegs []*regexp.Regexp
	for _, item := range conf.Source.IncludeTableRegex {
		reg, err := regexp.Compile(item)
		if err != nil {
			zlogger.Panic().Msg(err.Error())
		}
		includeRegs = append(includeRegs, reg)
	}
	var excludeRegs []*regexp.Regexp
	for _, item := range conf.Source.ExcludeTableRegex {
		reg, err := regexp.Compile(item)
		if err != nil {
			zlogger.Panic().Msg(err.Error())
		}
		excludeRegs = append(excludeRegs, reg)
	}
	eventHandler := &VastbaseEventHandler{
		vendor:      dbvendor.Registry.GetVendor(conf.Db.Driver),
		targetDB:    targetDB,
		conf:        conf,
		sourceDB:    sourceDB,
		includeRegs: includeRegs,
		excludeRegs: excludeRegs,
		dumpTasks:   make([]*DumpTask, 0, conf.Source.DumpBatchSize),
		taskPool: sync.Pool{
			New: func() any {
				return new(DumpTask)
			},
		},
		targetSchemas:   targetSchemas,
		sourceTargetMap: sourceTargetMap,
	}
	if conf.Source.Dump {
		eventHandler.dumping = true
		go func() {
			<-chameleon.GetCanal().WaitDumpDone()
			eventHandler.batchInsert()
			eventHandler.dumping = false

			if !conf.Source.Sync {
				for _, item := range targetSchemas {
					if err := eventHandler.targetDB.WithContext(context.Background()).Exec(fmt.Sprintf(`set search_path = "%s";
CREATE OR REPLACE FUNCTION "reset_sequence" (tablename text, columnname text, sequence_name text) 
    RETURNS INTEGER AS 
    
    $body$  
      DECLARE 
      retval  INTEGER;
      BEGIN 
    
      EXECUTE 'SELECT setval( ''' || sequence_name  || ''', ' || '(SELECT MAX(' || columnname || 
          ') FROM "' || tablename || '")' || '+1)' INTO retval;
      RETURN retval;
      END;  
    
    $body$  LANGUAGE 'plpgsql';`, item)).Error; err != nil {
						zlogger.Panic().Msg(err.Error())
					}
					if err := eventHandler.targetDB.WithContext(context.Background()).Exec(`SELECT table_name || '_' || column_name || '_seq', 
    reset_sequence(table_name, column_name, table_name || '_' || column_name || '_seq') 
FROM information_schema.columns where columns.table_schema = $1 and columns.column_default like 'nextval%';
`, item).Error; err != nil {
						zlogger.Panic().Msg(err.Error())
					}
				}
			}
		}()
	}
	chameleon.RegisterHandler(eventHandler)
}

const (
	maxParameterSize = 65535
)

type DMLKey struct {
	Schema      string
	TablePrefix string
	TableName   string
}

func (h *VastbaseEventHandler) doBatchInsert(db *sql.DB, dml dbvendor.DMLSchema, rows []interface{}, args []interface{}) {
	statement, err := h.vendor.GetBatchInsertStatement(dml, rows)
	if err != nil {
		zlogger.Panic().Err(errors.WithStack(err)).Msg(err.Error())
	}
	lo.ForEach(args, func(item interface{}, index int) {
		if value, ok := item.(string); ok {
			if value == "0000-00-00 00:00:00" || value == "0000-00-00" {
				args[index] = nil
			}
		}
	})
	if _, err = db.ExecContext(context.Background(), statement, args...); err != nil {
		zlogger.Panic().Err(errors.WithStack(err)).Msg(err.Error())
	}
}

func (h *VastbaseEventHandler) batchInsert() {
	if len(h.dumpTasks) == 0 {
		return
	}
	defer func() {
		zlogger.Info().Msgf("%d rows dumped", len(h.dumpTasks))
		for _, item := range h.dumpTasks {
			h.taskPool.Put(item)
		}
		h.dumpTasks = h.dumpTasks[:0]
	}()

	dmlMap := make(map[DMLKey][]*DumpTask)
	for _, item := range h.dumpTasks {
		key := DMLKey{
			Schema:      item.Dml.Schema,
			TablePrefix: item.Dml.TablePrefix,
			TableName:   item.Dml.TableName,
		}
		dmlMap[key] = append(dmlMap[key], item)
	}

	sqlDB, err := h.targetDB.DB()
	if err != nil {
		zlogger.Panic().Msg(err.Error())
	}

	var args []interface{}
	var rows []interface{}
	for _, v := range dmlMap {
		parameterSizePerRow := float64(len(v[0].Args))
		maxRowSize := math.Floor(maxParameterSize / parameterSizePerRow)
		for _, item := range v {
			args = append(args, item.Args...)
			rows = append(rows, item)
			if len(rows) == int(maxRowSize) {
				h.doBatchInsert(sqlDB, v[0].Dml, rows, args)
				args = args[:0]
				rows = rows[:0]
			}
		}
		h.doBatchInsert(sqlDB, v[0].Dml, rows, args)
		args = args[:0]
		rows = rows[:0]
	}
	args = nil
	rows = nil
}

func (h *VastbaseEventHandler) fetchExistTable(conn *client.Conn) ([]string, error) {
	r, err := conn.Execute("show full tables where Table_Type = 'BASE TABLE';")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer r.Close()

	var existTables []string

	// Direct access to fields
	for _, row := range r.Values {
		val := row[0]
		if val.Type == mysql.FieldValueTypeString {
			existTables = append(existTables, string(val.AsString()))
		}
	}

	return existTables, nil
}

func (h *VastbaseEventHandler) Migrate() error {
	for k, v := range h.sourceDB {
		existTables, err := h.fetchExistTable(v)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, table := range existTables {
			if len(h.includeRegs) > 0 {
				match := false
				for _, reg := range h.includeRegs {
					match = reg.MatchString(fmt.Sprintf("%s.%s", k, table))
					if match {
						break
					}
				}
				if !match {
					continue
				}
			}
			if len(h.excludeRegs) > 0 {
				match := false
				for _, reg := range h.excludeRegs {
					match = reg.MatchString(fmt.Sprintf("%s.%s", k, table))
					if match {
						break
					}
				}
				if match {
					continue
				}
			}
			sourceTable, err := schema.NewTable(v, k, table)
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
	}
	return nil
}

func (h *VastbaseEventHandler) ConvertSourceTable2TargetTable(source *schema.Table, target *dbvendor.Table) error {
	target.Name = strings.ToLower(source.Name)
	target.TablePrefix = h.sourceTargetMap[source.Schema]
	if len(source.PKColumns) > 0 {
		for _, col := range source.PKColumns {
			target.Pk = append(target.Pk, strings.ToLower(source.Columns[col].Name))
		}
	}
	for _, item := range source.Columns {
		isPk := false
		if sliceutils.StringContains(target.Pk, item.Name) {
			isPk = true
		}
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
	log.Debugf("===================== OnTableChanged called =====================")
	chameleon.GetCanal().ClearTableCache([]byte(schema), []byte(table))
	chameleon.GetCanal().ClearTableCache([]byte(schema), []byte(strings.ToLower(table)))
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
	log.Debugf("===================== OnDDL called =====================")
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
			tablePrefix := h.sourceTargetMap[string(queryEvent.Schema)]
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
	log.Debugf("%s %v\n", e.Action, e.Rows)
	tableName := e.Table.Name
	tablePrefix := h.sourceTargetMap[e.Table.Schema]
	if stringutils.IsEmpty(tablePrefix) {
		tablePrefix = h.conf.Db.Name
	}
	var pk []dbvendor.Column
	for i := range e.Table.PKColumns {
		pkCol := e.Table.GetPKColumn(i)
		pk = append(pk, dbvendor.Column{
			Name: pkCol.Name,
		})
	}
	dml := dbvendor.DMLSchema{
		Schema:      h.conf.Db.Name,
		TablePrefix: tablePrefix,
		TableName:   tableName,
		Pk:          pk,
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
		sqlDB, err := h.targetDB.DB()
		if err != nil {
			zlogger.Panic().Msg(err.Error())
		}
		if h.dumping {
			for _, row := range e.Rows {
				task := h.taskPool.Get().(*DumpTask)
				task.Dml = dml
				task.Args = row
				h.dumpTasks = append(h.dumpTasks, task)
				if len(h.dumpTasks) == h.conf.Source.DumpBatchSize {
					h.batchInsert()
				}
			}
		} else {
			statement, err := h.vendor.GetInsertStatement(dml)
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
			zlogger.Panic().Msg(err.Error())
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
		row = append(row, pkVals...)
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
		if err = h.vendor.Delete(context.Background(), h.targetDB, dml, pkVals...); err != nil {
			zlogger.Err(errors.WithStack(err)).Msg(err.Error())
			return nil
		}
	}
	return nil
}

func (h *VastbaseEventHandler) String() string {
	return "vastbase"
}
