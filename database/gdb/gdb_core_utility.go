// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.
//

package gdb

import (
	"context"
	"fmt"

	"github.com/gogf/gf/v2/crypto/gmd5"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/text/gregex"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gutil"
)

// GetDB returns the underlying DB.
func (c *Core) GetDB() DB {
	return c.db
}

// GetLink creates and returns the underlying database link object with transaction checks.
// The parameter `master` specifies whether using the master node if master-slave configured.
func (c *Core) GetLink(ctx context.Context, master bool, schema string) (Link, error) {
	tx := TXFromCtx(ctx, c.db.GetGroup())
	if tx != nil {
		return &txLink{tx.GetSqlTX()}, nil
	}
	if master {
		link, err := c.db.GetCore().MasterLink(schema)
		if err != nil {
			return nil, err
		}
		return link, nil
	}
	link, err := c.db.GetCore().SlaveLink(schema)
	if err != nil {
		return nil, err
	}
	return link, nil
}

// MasterLink acts like function Master but with additional `database` parameter specifying
// the database for the connection. It is defined for internal usage.
// Also see Master.
func (c *Core) MasterLink(database ...string) (Link, error) {
	return c.MasterLinkWithSchema(c.GetSchema(), database...)
}

// MasterLinkWithSchema acts like function MasterWithSchema but with additional `schema` and `database` parameter specifying
// the database for the connection and the schema namespace for searching. It is defined for internal usage.
// Also see Master.
func (c *Core) MasterLinkWithSchema(schema string, database ...string) (Link, error) {
	db, err := c.db.MasterWithSchema(schema, database...)
	if err != nil {
		return nil, err
	}
	return &dbLink{
		DB:         db,
		isOnMaster: true,
	}, nil
}

// SlaveLink acts like function Slave but with additional `database` parameter specifying
// the database for the connection. It is defined for internal usage.
// Also see Slave.
func (c *Core) SlaveLink(database ...string) (Link, error) {
	return c.SlaveLinkWithSchema(c.GetSchema(), database...)
}

// SlaveLinkWithSchema acts like function SlaveWithSchema but with additional `schema` and `database` parameter specifying
// the database for the connection and the schema namespace for searching. It is defined for internal usage.
// Also see Slave.
func (c *Core) SlaveLinkWithSchema(schema string, database ...string) (Link, error) {
	db, err := c.db.SlaveWithSchema(schema, database...)
	if err != nil {
		return nil, err
	}
	return &dbLink{
		DB:         db,
		isOnMaster: false,
	}, nil
}

// QuoteWord checks given string `s` a word,
// if true it quotes `s` with security chars of the database
// and returns the quoted string; or else it returns `s` without any change.
//
// The meaning of a `word` can be considered as a column name.
func (c *Core) QuoteWord(s string) string {
	s = gstr.Trim(s)
	if s == "" {
		return s
	}
	charLeft, charRight := c.db.GetQuoteChars()
	return doQuoteWord(s, charLeft, charRight)
}

// QuoteString quotes string with quote chars. Strings like:
// "user", "user u", "user,user_detail", "user u, user_detail ut", "u.id asc".
//
// The meaning of a `string` can be considered as part of a statement string including columns.
func (c *Core) QuoteString(s string) string {
	charLeft, charRight := c.db.GetQuoteChars()
	return doQuoteString(s, charLeft, charRight)
}

// QuotePrefixTableName adds prefix string and quotes chars for the table.
// It handles table string like:
// "user", "user u",
// "user,user_detail",
// "user u, user_detail ut",
// "user as u, user_detail as ut".
//
// Note that, this will automatically checks the table prefix whether already added,
// if true it does nothing to the table name, or else adds the prefix to the table name.
func (c *Core) QuotePrefixTableName(table string) string {
	charLeft, charRight := c.db.GetQuoteChars()
	return doQuoteTableName(table, c.db.GetPrefix(), charLeft, charRight)
}

// GetQuoteChars returns the security char for current database.
// It does nothing in default.
func (c *Core) GetQuoteChars() (charLeft string, charRight string) {
	return "", ""
}

// Tables retrieves and returns the tables of current database's public or default schema(if the db support schema).
//
// The parameter `database` is optional, if given nil it automatically retrieves a raw sql connection
// as its link to proceed necessary sql query.
//
// It's mainly used in cli tool chain for automatically generating the models.
func (c *Core) Tables(ctx context.Context, database ...string) (tables []string, err error) {
	return
}

// TablesInSchema retrieves and returns the tables of given database's schema(if the db support schema).
//
// The parameter `schema` is required. It set the search_path to `schema` in pgsql, set current_schema `schema` in oracle or DB2, set default_schema to `schema` in mssql.
//
// The parameter `database` is optional, if given nil it automatically retrieves a raw sql connection
// as its link to proceed necessary sql query.
//
// It's mainly used in cli tool chain for automatically generating the models.
func (c *Core) TablesInSchema(ctx context.Context, schema string, database ...string) (tables []string, err error) {
	return
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
func (c *Core) TableFields(ctx context.Context, table string, database ...string) (fields map[string]*TableField, err error) {
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
func (c *Core) TableFieldsInSchema(ctx context.Context, table string, schema string, database ...string) (fields map[string]*TableField, err error) {
	return
}

// ClearTableFields removes certain cached table fields of current configuration group.
func (c *Core) ClearTableFields(ctx context.Context, table string, database ...string) (err error) {
	tableFieldsMap.Remove(fmt.Sprintf(
		`%s%s@%s#%s`,
		cachePrefixTableFields,
		c.db.GetGroup(),
		gutil.GetOrDefaultStr(c.db.GetDatabase(), database...),
		table,
	))
	return
}

// ClearTableFieldsAll removes all cached table fields of current configuration group.
func (c *Core) ClearTableFieldsAll(ctx context.Context) (err error) {
	var (
		keys        = tableFieldsMap.Keys()
		cachePrefix = fmt.Sprintf(`%s@%s`, cachePrefixTableFields, c.db.GetGroup())
		removedKeys = make([]string, 0)
	)
	for _, key := range keys {
		if gstr.HasPrefix(key, cachePrefix) {
			removedKeys = append(removedKeys, key)
		}
	}
	if len(removedKeys) > 0 {
		tableFieldsMap.Removes(removedKeys)
	}
	return
}

// ClearCache removes cached sql result of certain table.
func (c *Core) ClearCache(ctx context.Context, table string) (err error) {
	return c.db.GetCache().Clear(ctx)
}

// ClearCacheAll removes all cached sql result from cache
func (c *Core) ClearCacheAll(ctx context.Context) (err error) {
	return c.db.GetCache().Clear(ctx)
}

func (c *Core) makeSelectCacheKey(name, database, schema, table, sql string, args ...interface{}) string {
	if name == "" {
		name = fmt.Sprintf(
			`%s@%s?%s#%s:%s`,
			c.db.GetGroup(),
			database,
			schema,
			table,
			gmd5.MustEncryptString(sql+", @PARAMS:"+gconv.String(args)),
		)
	}
	return fmt.Sprintf(`%s%s`, cachePrefixSelectCache, name)
}

// HasField determine whether the field exists in the table.
func (c *Core) HasField(ctx context.Context, table, field string, schema ...string) (bool, error) {
	table = c.guessPrimaryTableName(table)
	tableFields, err := c.db.TableFields(ctx, table, schema...)
	if err != nil {
		return false, err
	}
	if len(tableFields) == 0 {
		return false, gerror.NewCodef(
			gcode.CodeNotFound,
			`empty table fields for table "%s"`, table,
		)
	}
	fieldsArray := make([]string, len(tableFields))
	for k, v := range tableFields {
		fieldsArray[v.Index] = k
	}
	charLeft, charRight := c.db.GetQuoteChars()
	field = gstr.Trim(field, charLeft+charRight)
	for _, f := range fieldsArray {
		if f == field {
			return true, nil
		}
	}
	return false, nil
}

// guessPrimaryTableName parses and returns the primary table name.
func (c *Core) guessPrimaryTableName(tableStr string) string {
	if tableStr == "" {
		return ""
	}
	var (
		guessedTableName string
		array1           = gstr.SplitAndTrim(tableStr, ",")
		array2           = gstr.SplitAndTrim(array1[0], " ")
		array3           = gstr.SplitAndTrim(array2[0], ".")
	)
	if len(array3) >= 2 {
		guessedTableName = array3[1]
	} else {
		guessedTableName = array3[0]
	}
	charL, charR := c.db.GetQuoteChars()
	if charL != "" || charR != "" {
		guessedTableName = gstr.Trim(guessedTableName, charL+charR)
	}
	if !gregex.IsMatchString(regularFieldNameRegPattern, guessedTableName) {
		return ""
	}
	return guessedTableName
}
