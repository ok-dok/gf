// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

// Package pgsql implements gdb.Driver, which supports operations for database PostgreSQL.
//
// Note:
// 1. It does not support Save/Replace features.
// 2. It does not support Insert Ignore features.
package pgsql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/container/gset"
	"github.com/gogf/gf/v2/frame/g"
	"strings"

	_ "github.com/lib/pq"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/text/gregex"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gutil"
)

// Driver is the driver for postgresql database.
type Driver struct {
	*gdb.Core
}

const (
	internalPrimaryKeyInCtx gctx.StrKey = "primary_key"
	defaultSchema           string      = "public"
	quoteChar               string      = `"`
)

func init() {
	if err := gdb.Register(`pgsql`, New()); err != nil {
		panic(err)
	}
}

// New create and returns a driver that implements gdb.Driver, which supports operations for PostgreSql.
func New() gdb.Driver {
	return &Driver{}
}

// New creates and returns a database object for postgresql.
// It implements the interface of gdb.Driver for extra database driver installation.
func (d *Driver) New(core *gdb.Core, node *gdb.ConfigNode) (gdb.DB, error) {
	return &Driver{
		Core: core,
	}, nil
}

// Open creates and returns an underlying sql.DB object for pgsql.
// https://pkg.go.dev/github.com/lib/pq
func (d *Driver) Open(config *gdb.ConfigNode) (db *sql.DB, err error) {
	var (
		source               string
		underlyingDriverName = "postgres"
	)
	if config.Link != "" {
		// ============================================================================
		// Deprecated from v2.2.0.
		// ============================================================================
		source = config.Link
		// Custom changing the schema in runtime.
		if config.Name != "" {
			source, _ = gregex.ReplaceString(`dbname=([\w\.\-]+)+`, "dbname="+config.Name, source)
		}
	} else {
		if config.Name != "" {
			source = fmt.Sprintf(
				"user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
				config.User, config.Pass, config.Host, config.Port, config.Name,
			)
		} else {
			source = fmt.Sprintf(
				"user=%s password=%s host=%s port=%s sslmode=disable",
				config.User, config.Pass, config.Host, config.Port,
			)
		}

		if config.Schema != "" {
			source = fmt.Sprintf("%s search_path=%s", source, config.Schema)
		}

		if config.Timezone != "" {
			source = fmt.Sprintf("%s timezone=%s", source, config.Timezone)
		}

		if config.Extra != "" {
			var extraMap map[string]interface{}
			if extraMap, err = gstr.Parse(config.Extra); err != nil {
				return nil, err
			}
			for k, v := range extraMap {
				source += fmt.Sprintf(` %s=%s`, k, v)
			}
		}
	}

	if db, err = sql.Open(underlyingDriverName, source); err != nil {
		err = gerror.WrapCodef(
			gcode.CodeDbOperationError, err,
			`sql.Open failed for driver "%s" by source "%s"`, underlyingDriverName, source,
		)
		return nil, err
	}
	return
}

// GetQuoteChars returns the security char for this type of database.
func (d *Driver) GetQuoteChars() (charLeft string, charRight string) {
	return quoteChar, quoteChar
}

// CheckLocalTypeForField checks and returns corresponding local golang type for given db type.
func (d *Driver) CheckLocalTypeForField(ctx context.Context, fieldType string, fieldValue interface{}) (gdb.LocalType, error) {
	var typeName string
	match, _ := gregex.MatchString(`(.+?)\((.+)\)`, fieldType)
	if len(match) == 3 {
		typeName = gstr.Trim(match[1])
	} else {
		typeName = fieldType
	}
	typeName = strings.ToLower(typeName)
	switch typeName {
	case
		// For pgsql, int2 = smallint.
		"int2",
		// For pgsql, int4 = integer
		"int4":
		return gdb.LocalTypeInt, nil

	case
		// For pgsql, int8 = bigint
		"int8":
		return gdb.LocalTypeInt64, nil

	case
		"_int2",
		"_int4":
		return gdb.LocalTypeIntSlice, nil

	case
		"_int8":
		return gdb.LocalTypeInt64Slice, nil

	default:
		return d.Core.CheckLocalTypeForField(ctx, fieldType, fieldValue)
	}
}

// ConvertValueForLocal converts value to local Golang type of value according field type name from database.
// The parameter `fieldType` is in lower case, like:
// `float(5,2)`, `unsigned double(5,2)`, `decimal(10,2)`, `char(45)`, `varchar(100)`, etc.
func (d *Driver) ConvertValueForLocal(ctx context.Context, fieldType string, fieldValue interface{}) (interface{}, error) {
	typeName, _ := gregex.ReplaceString(`\(.+\)`, "", fieldType)
	typeName = strings.ToLower(typeName)
	switch typeName {
	// For pgsql, int2 = smallint and int4 = integer.
	case "int2", "int4":
		return gconv.Int(gconv.String(fieldValue)), nil

	// For pgsql, int8 = bigint.
	case "int8":
		return gconv.Int64(gconv.String(fieldValue)), nil

	// Int32 slice.
	case
		"_int2", "_int4":
		return gconv.Ints(
			gstr.ReplaceByMap(gconv.String(fieldValue),
				map[string]string{
					"{": "[",
					"}": "]",
				},
			),
		), nil

	// Int64 slice.
	case
		"_int8":
		return gconv.Int64s(
			gstr.ReplaceByMap(gconv.String(fieldValue),
				map[string]string{
					"{": "[",
					"}": "]",
				},
			),
		), nil

	default:
		return d.Core.ConvertValueForLocal(ctx, fieldType, fieldValue)
	}
}

// DoFilter deals with the sql string before commits it to underlying sql driver.
func (d *Driver) DoFilter(ctx context.Context, link gdb.Link, sql string, args []interface{}) (newSql string, newArgs []interface{}, err error) {
	var index int
	// Convert placeholder char '?' to string "$x".
	newSql, _ = gregex.ReplaceStringFunc(`\?`, sql, func(s string) string {
		index++
		return fmt.Sprintf(`$%d`, index)
	})
	// Handle pgsql jsonb feature support, which contains place-holder char '?'.
	// Refer:
	// https://github.com/gogf/gf/issues/1537
	// https://www.postgresql.org/docs/12/functions-json.html
	newSql, _ = gregex.ReplaceStringFuncMatch(`(::jsonb([^\w\d]*)\$\d)`, newSql, func(match []string) string {
		return fmt.Sprintf(`::jsonb%s?`, match[2])
	})
	newSql, _ = gregex.ReplaceString(` LIMIT (\d+),\s*(\d+)`, ` LIMIT $2 OFFSET $1`, newSql)
	return d.Core.DoFilter(ctx, link, newSql, args)
}

// Tables retrieves and returns the tables of current database's default or public schema(if the db support schema).
// It's mainly used in cli tool chain for automatically generating the models.
func (d *Driver) Tables(ctx context.Context, database ...string) (tables []string, err error) {
	return d.TablesInSchema(ctx, d.GetSchema(), database...)
}

// TablesInSchema retrieves and returns the tables of given database's schema.
// It's mainly used in cli tool chain for automatically generating the models.
func (d *Driver) TablesInSchema(ctx context.Context, schema string, database ...string) (tables []string, err error) {
	var (
		result gdb.Result
		//usedDatabase = gutil.GetOrDefaultStr(d.GetConfig().Name, database...)
		usedSchema = gutil.GetOrDefaultStr(d.GetSchema(), schema)
	)
	if usedSchema == "" {
		usedSchema = defaultSchema
	}
	// DO NOT use `usedDatabase` as parameter for function `SlaveLink`.
	link, err := d.SlaveLink(database...)
	if err != nil {
		return nil, err
	}
	var query = fmt.Sprintf(`
SELECT t.tablename
FROM pg_tables t
WHERE t.schemaname = '%s'`,
		usedSchema,
	)

	query, _ = gregex.ReplaceString(`[\n\r\s]+`, " ", gstr.Trim(query))
	result, err = d.DoSelect(ctx, link, query)
	if err != nil {
		return
	}
	for _, m := range result {
		for _, v := range m {
			tables = append(tables, v.String())
		}
	}
	return
}

// TableFields retrieves and returns the fields' information of specified table of given database's schema.
func (d *Driver) TableFields(ctx context.Context, table string, database ...string) (fields map[string]*gdb.TableField, err error) {
	return d.TableFieldsInSchema(ctx, table, d.GetSchema(), database...)
}

// TableFieldsInSchema retrieves and returns the fields' information of specified table of given database's schema.
func (d *Driver) TableFieldsInSchema(ctx context.Context, table string, schema string, database ...string) (fields map[string]*gdb.TableField, err error) {
	var (
		result gdb.Result
		link   gdb.Link
		//usedDatabase = gutil.GetOrDefaultStr(d.GetConfig().Name, database...)
		usedSchema = gutil.GetOrDefaultStr(d.GetSchema(), schema)
	)
	if usedSchema == "" {
		usedSchema = defaultSchema
	}
	structureSql := fmt.Sprintf(`
SELECT a.attname AS field, t.typname AS type,a.attnotnull as null,
    (case
		when d.contype = 'p' then 'pri'
		when d.contype = 'f' then 'for'
		else '' end)  as key
      ,ic.column_default as default_value,b.description as comment
      ,coalesce(character_maximum_length, numeric_precision, -1) as length
      ,numeric_scale as scale
FROM pg_attribute a
         left join pg_class c on a.attrelid = c.oid
         inner join pg_namespace n ON c.relnamespace = n.oid
         left join pg_constraint d on d.conrelid = c.oid and a.attnum = d.conkey[1]
         left join pg_description b ON a.attrelid=b.objoid AND a.attnum = b.objsubid
         left join pg_type t ON a.atttypid = t.oid
         left join information_schema.columns ic on ic.column_name = a.attname and ic.table_name = c.relname and ic.table_schema = n.nspname
WHERE n.nspname = '%s' and c.relname = '%s' and a.attisdropped is false and a.attnum > 0
ORDER BY a.attnum`,
		usedSchema,
		gstr.Trim(table, quoteChar),
	)
	// DO NOT use `usedDatabase` as parameter for function `SlaveLink`.
	if link, err = d.SlaveLinkWithSchema(usedSchema, database...); err != nil {
		return nil, err
	}
	structureSql, _ = gregex.ReplaceString(`[\n\r\s]+`, " ", gstr.Trim(structureSql))
	result, err = d.DoSelect(ctx, link, structureSql)
	if err != nil {
		return nil, err
	}
	fields = make(map[string]*gdb.TableField)
	var (
		index = 0
		name  string
		ok    bool
	)
	for _, m := range result {
		name = m["field"].String()
		// Filter duplicated fields.
		if _, ok = fields[name]; ok {
			continue
		}
		fields[name] = &gdb.TableField{
			Index:   index,
			Name:    name,
			Type:    m["type"].String(),
			Null:    !m["null"].Bool(),
			Key:     m["key"].String(),
			Default: m["default_value"].Val(),
			Comment: m["comment"].String(),
		}
		index++
	}
	return fields, nil
}

// DoInsert inserts or updates data forF given table.
func (d *Driver) DoInsert(ctx context.Context, link gdb.Link, table string, list gdb.List, option gdb.DoInsertOption) (result sql.Result, err error) {
	switch option.InsertOption {
	case gdb.InsertOptionSave:
		return d.insertOrUpdate(ctx, link, table, list, option)

	case gdb.InsertOptionReplace:
		return nil, gerror.NewCode(
			gcode.CodeNotSupported,
			`Replace operation is not supported by pgsql driver`,
		)

	case gdb.InsertOptionIgnore:
		return nil, gerror.NewCode(
			gcode.CodeNotSupported,
			`Insert ignore operation is not supported by pgsql driver`,
		)

	case gdb.InsertOptionDefault:
		if tableFields, err := d.TableFields(ctx, table); err == nil {
			for _, field := range tableFields {
				if field.Key == "pri" {
					ctx = context.WithValue(ctx, internalPrimaryKeyInCtx, field)
					break
				}
			}
		}
	}
	return d.Core.DoInsert(ctx, link, table, list, option)
}

// DoExec commits the sql string and its arguments to underlying driver
// through given link object and returns the execution result.
func (d *Driver) DoExec(ctx context.Context, link gdb.Link, sql string, args ...interface{}) (result sql.Result, err error) {
	var (
		isUseCoreDoExec bool   = false // Check whether the default method needs to be used
		primaryKey      string = ""
		pkField         *gdb.TableField
	)

	// Transaction checks.
	if link == nil {
		if tx := gdb.TXFromCtx(ctx, d.GetGroup()); tx != nil {
			// Firstly, check and retrieve transaction link from context.
			link = tx
		} else if link, err = d.MasterLink(); err != nil {
			// Or else it creates one from master node.
			return nil, err
		}
	} else if !link.IsTransaction() {
		// If current link is not transaction link, it checks and retrieves transaction from context.
		if tx := gdb.TXFromCtx(ctx, d.GetGroup()); tx != nil {
			link = tx
		}
	}

	// Check if it is an insert operation with primary key.
	if value := ctx.Value(internalPrimaryKeyInCtx); value != nil {
		var ok bool
		// TODO 检查所有从ctx中存、取internalPrimaryKeyInCtx键数据的类型，这里取值类型必须和存入时的类型（要么是指针，要么是值）保持一致，
		pkField, ok = value.(*gdb.TableField)
		// check if it is an insert operation.
		// when it exists pkField and is an insert operation, `sql` should append " RETURNING pkName"
		// regardless of whether the `link` is transaction
		if ok && pkField.Name != "" && strings.Contains(sql, "INSERT INTO") {
			primaryKey = pkField.Name
			sql += " RETURNING " + primaryKey
			isUseCoreDoExec = false
		} else {
			isUseCoreDoExec = true
		}
	} else {
		isUseCoreDoExec = true
	}

	if isUseCoreDoExec {
		// use default DoExec
		return d.Core.DoExec(ctx, link, sql, args...)
	}

	// Only the insert operation with primary key can execute the following code
	if d.GetConfig().ExecTimeout > 0 {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, d.GetConfig().ExecTimeout)
		defer cancelFunc()
	}

	// Sql filtering.
	sql, args = d.FormatSqlBeforeExecuting(sql, args)
	sql, args, err = d.DoFilter(ctx, link, sql, args)
	if err != nil {
		return nil, err
	}

	// Link execution.
	var out gdb.DoCommitOutput
	out, err = d.Core.DoCommit(ctx, gdb.DoCommitInput{
		Link:          link,
		Sql:           sql,
		Args:          args,
		Stmt:          nil,
		Type:          gdb.SqlTypeQueryContext,
		IsTransaction: link.IsTransaction(),
	})

	if err != nil {
		return nil, err
	}
	affected := len(out.Records)
	if affected > 0 {
		if !strings.Contains(pkField.Type, "int") {
			return Result{
				affected:     int64(affected),
				lastInsertId: 0,
				lastInsertIdError: gerror.NewCodef(
					gcode.CodeNotSupported,
					"LastInsertId is not supported by primary key type: %s", pkField.Type),
			}, nil
		}

		if out.Records[affected-1][primaryKey] != nil {
			lastInsertId := out.Records[affected-1][primaryKey].Int64()
			return Result{
				affected:     int64(affected),
				lastInsertId: lastInsertId,
			}, nil
		}
	}

	return Result{}, nil
}

func (d *Driver) insertOrUpdate(ctx context.Context, link gdb.Link, table string, list gdb.List, option gdb.DoInsertOption) (res sql.Result, err error) {
	var (
		keys           []string      // Field names.
		values         []string      // Value holder string array, like: (?,?,?)
		params         []interface{} // Values that will be committed to underlying database driver.
		onDuplicateStr string        // onDuplicateStr is used in "ON CONFLICT (col_name) DO UPDATE SET" statement.
	)
	// Group the list by fields. Different fields to different list.
	// It here uses ListMap to keep sequence for data inserting.
	var keyListMap = gmap.NewListMap()
	for _, item := range list {
		var (
			tmpKeys              = make([]string, 0)
			tmpKeysInSequenceStr string
		)
		for k := range item {
			tmpKeys = append(tmpKeys, k)
		}
		var tableFields map[string]*gdb.TableField
		if tableFields, err = d.TableFields(ctx, table); err == nil {
			for _, field := range tableFields {
				if field.Key == "pri" {
					ctx = context.WithValue(ctx, internalPrimaryKeyInCtx, field)
					break
				}
			}
			keys, err = d.fieldsToSequence(tableFields, tmpKeys)
		}
		if err != nil {
			return nil, err
		}

		tmpKeysInSequenceStr = gstr.Join(keys, ",")

		if !keyListMap.Contains(tmpKeysInSequenceStr) {
			keyListMap.Set(tmpKeysInSequenceStr, make(g.List, 0))
		}
		tmpKeysInSequenceList := keyListMap.Get(tmpKeysInSequenceStr).(g.List)
		tmpKeysInSequenceList = append(tmpKeysInSequenceList, item)
		keyListMap.Set(tmpKeysInSequenceStr, tmpKeysInSequenceList)
	}
	if keyListMap.Size() > 1 {
		var (
			tmpResult    sql.Result
			sqlResult    gdb.SqlResult
			rowsAffected int64
		)
		keyListMap.Iterator(func(key, value interface{}) bool {
			tmpResult, err = d.Core.DoInsert(ctx, link, table, value.(g.List), option)
			if err != nil {
				return false
			}
			rowsAffected, err = tmpResult.RowsAffected()
			if err != nil {
				return false
			}
			sqlResult.Result = tmpResult
			sqlResult.Affected += rowsAffected
			return true
		})
		return &sqlResult, nil
	}
	// Prepare the batch result pointer.
	var (
		charL, charR = d.GetQuoteChars()
		batchResult  = new(gdb.SqlResult)
		keysStr      = charL + strings.Join(keys, charR+","+charL) + charR
		operation    = gdb.GetInsertOperationByOption(option.InsertOption)
	)
	if option.InsertOption == gdb.InsertOptionSave {
		var pkField = ctx.Value(internalPrimaryKeyInCtx).(*gdb.TableField)
		if pkField != nil {
			pk := pkField.Name
			onDuplicateStr = d.formatOnDuplicate(keys, d.QuoteWord(pk), option)
		} else {
			return nil, gerror.Newf("cannot read key %s from ctx", d.QuoteWord(string(internalPrimaryKeyInCtx)))
		}
	}
	var (
		listLength  = len(list)
		valueHolder = make([]string, 0)
	)
	for i := 0; i < listLength; i++ {
		values = values[:0]
		// Note that the map type is unordered,
		// so it should use slice+key to retrieve the value.
		for _, k := range keys {
			if s, ok := list[i][k].(gdb.Raw); ok {
				values = append(values, gconv.String(s))
			} else {
				values = append(values, "?")
				params = append(params, list[i][k])
			}
		}
		valueHolder = append(valueHolder, "("+gstr.Join(values, ",")+")")
		// Batch package checks: It meets the batch number, or it is the last element.
		if len(valueHolder) == option.BatchCount || (i == listLength-1 && len(valueHolder) > 0) {
			var (
				stdSqlResult sql.Result
				affectedRows int64
			)
			stdSqlResult, err = d.DoExec(ctx, link, fmt.Sprintf(
				"%s INTO %s(%s) VALUES%s %s",
				operation, d.QuotePrefixTableName(table), keysStr,
				gstr.Join(valueHolder, ","),
				onDuplicateStr,
			), params...)
			if err != nil {
				return stdSqlResult, err
			}
			if affectedRows, err = stdSqlResult.RowsAffected(); err != nil {
				err = gerror.WrapCode(gcode.CodeDbOperationError, err, `sql.Result.RowsAffected failed`)
				return stdSqlResult, err
			} else {
				batchResult.Result = stdSqlResult
				batchResult.Affected += affectedRows
			}
			params = params[:0]
			valueHolder = valueHolder[:0]
		}
	}
	return batchResult, nil
}

func (d *Driver) fieldsToSequence(tableFields map[string]*gdb.TableField, fields []string) ([]string, error) {
	var (
		fieldSet               = gset.NewStrSetFrom(fields)
		fieldsResultInSequence = make([]string, 0)
	)
	// Sort the fields in order.
	var fieldsOfTableInSequence = make([]string, len(tableFields))
	for _, field := range tableFields {
		fieldsOfTableInSequence[field.Index] = field.Name
	}
	// Sort the input fields.
	for _, fieldName := range fieldsOfTableInSequence {
		if fieldSet.Contains(fieldName) {
			fieldsResultInSequence = append(fieldsResultInSequence, fieldName)
		}
	}
	return fieldsResultInSequence, nil
}

func (d *Driver) formatOnDuplicate(columns []string, pk string, option gdb.DoInsertOption) string {
	var onDuplicateStr string
	if option.OnDuplicateStr != "" {
		onDuplicateStr = option.OnDuplicateStr
	} else if len(option.OnDuplicateMap) > 0 {
		for k, v := range option.OnDuplicateMap {
			if len(onDuplicateStr) > 0 {
				onDuplicateStr += ","
			}
			switch v.(type) {
			case gdb.Raw, *gdb.Raw:
				onDuplicateStr += fmt.Sprintf(
					"%s=%s",
					d.QuoteWord(k),
					v,
				)
			default:
				onDuplicateStr += fmt.Sprintf(
					"%s=%s",
					d.QuoteWord(k),
					d.QuoteWord(gconv.String(v)),
				)
			}
		}
	} else {
		for _, column := range columns {
			// If it's SAVE operation, do not automatically update the creating time.
			if d.GetCore().IsSoftCreatedFieldName(column) {
				continue
			}
			if len(onDuplicateStr) > 0 {
				onDuplicateStr += ","
			}
			onDuplicateStr += fmt.Sprintf(
				"%s=EXCLUDED.%s",
				d.QuoteWord(column),
				d.QuoteWord(column),
			)
		}
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s", d.QuoteWord(pk), onDuplicateStr)
}
