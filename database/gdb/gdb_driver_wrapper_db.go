// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/internal/intlog"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gutil"
)

// DriverWrapperDB is a DB wrapper for extending features with embedded DB.
type DriverWrapperDB struct {
	DB
}

// Open creates and returns an underlying sql.DB object for pgsql.
// https://pkg.go.dev/github.com/lib/pq
func (d *DriverWrapperDB) Open(node *ConfigNode) (db *sql.DB, err error) {
	var ctx = d.GetCtx()
	intlog.PrintFunc(ctx, func() string {
		return fmt.Sprintf(`open new connection:%s`, gjson.MustEncode(node))
	})
	return d.DB.Open(node)
}

// Tables retrieves and returns the tables of current database's public or default schema(if the db support schema).
//
// The parameter `database` is optional, if given nil it automatically retrieves a raw sql connection
// as its link to proceed necessary sql query.
//
// It's mainly used in cli tool chain for automatically generating the models.
func (d *DriverWrapperDB) Tables(ctx context.Context, database ...string) (tables []string, err error) {
	ctx = context.WithValue(ctx, ctxKeyInternalProducedSQL, struct{}{})
	return d.DB.Tables(ctx, database...)
}

// TablesInSchema retrieves and returns the tables of given database's schema(if the db support schema).
//
// The parameter `schema` is required. It set the search_path to `schema` in pgsql, set current_schema `schema` in oracle or DB2, set default_schema to `schema` in mssql.
//
// The parameter `database` is optional, if given nil it automatically retrieves a raw sql connection
// as its link to proceed necessary sql query.
//
// It's mainly used in cli tool chain for automatically generating the models.
func (d *DriverWrapperDB) TablesInSchema(ctx context.Context, schema string, database ...string) (tables []string, err error) {
	ctx = context.WithValue(ctx, ctxKeyInternalProducedSQL, struct{}{})
	return d.DB.TablesInSchema(ctx, schema, database...)
}

// TableFields retrieves and returns the fields' information of specified table of current
// database public or default schema(if the db support schema).
//
// The parameter `database` is optional, if given nil it automatically retrieves a raw sql connection
// as its link to proceed necessary sql query.
//
// Note that it returns a map containing the field name and its corresponding fields.
// As a map is unsorted, the TableField struct has an "Index" field marks its sequence in
// the fields.
//
// It's using cache feature to enhance the performance, which is never expired util the
// process restarts.
func (d *DriverWrapperDB) TableFields(
	ctx context.Context, table string, database ...string,
) (fields map[string]*TableField, err error) {
	if table == "" {
		return nil, nil
	}
	charL, charR := d.GetQuoteChars()
	table = gstr.Trim(table, charL+charR)
	if gstr.Contains(table, " ") {
		return nil, gerror.NewCode(
			gcode.CodeInvalidParameter,
			"function TableFields supports only single table operations",
		)
	}
	var (
		cacheKey = fmt.Sprintf(
			`%s%s@%s#%s`,
			cachePrefixTableFields,
			d.GetGroup(),
			gutil.GetOrDefaultStr(d.GetDatabase(), database...),
			table,
		)
		value = tableFieldsMap.GetOrSetFuncLock(cacheKey, func() interface{} {
			ctx = context.WithValue(ctx, ctxKeyInternalProducedSQL, struct{}{})
			fields, err = d.DB.TableFields(ctx, table, database...)
			if err != nil {
				return nil
			}
			return fields
		})
	)
	if value != nil {
		fields = value.(map[string]*TableField)
	}
	return
}

// TableFieldsInSchema retrieves and returns the fields' information of specified table of given
// database's schema.
//
// The parameter `schema` is required. It set the search_path to `schema` in pgsql, set current_schema `schema` in oracle or DB2, set default_schema to `schema` in mssql.
//
// The parameter `database` is optional, if given nil it automatically retrieves a raw sql connection
// as its link to proceed necessary sql query.
// Note that it returns a map containing the field name and its corresponding fields.
// As a map is unsorted, the TableField struct has an "Index" field marks its sequence in
// the fields.
//
// It's using cache feature to enhance the performance, which is never expired util the
// process restarts.
func (d *DriverWrapperDB) TableFieldsInSchema(ctx context.Context, table string, schema string, database ...string) (fields map[string]*TableField, err error) {
	if table == "" {
		return nil, nil
	}
	charL, charR := d.GetQuoteChars()
	table = gstr.Trim(table, charL+charR)
	if gstr.Contains(table, " ") {
		return nil, gerror.NewCode(
			gcode.CodeInvalidParameter,
			"function TableFieldsInSchema supports only single table operations",
		)
	}
	var (
		cacheKey = fmt.Sprintf(
			`%s%s@%s?%s#%s`,
			cachePrefixTableFieldsInSchema,
			d.GetGroup(),
			gutil.GetOrDefaultStr(d.GetDatabase(), database...),
			gutil.GetOrDefaultStr(d.GetSchema(), schema),
			table,
		)
		value = tableFieldsMap.GetOrSetFuncLock(cacheKey, func() interface{} {
			ctx = context.WithValue(ctx, ctxKeyInternalProducedSQL, struct{}{})
			fields, err = d.DB.TableFieldsInSchema(ctx, table, schema, database...)
			if err != nil {
				return nil
			}
			return fields
		})
	)
	if value != nil {
		fields = value.(map[string]*TableField)
	}
	return
}
