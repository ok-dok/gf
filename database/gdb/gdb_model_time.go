// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"fmt"

	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/text/gregex"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gutil"
)

// Unscoped disables the auto-update time feature for insert, update and delete options.
func (m *Model) Unscoped() *Model {
	model := m.getModel()
	model.unscoped = true
	return model
}

// getSoftFieldNameCreate checks and returns the field name for record creating time.
// If there's no field name for storing creating time, it returns an empty string.
// It checks the key with or without cases or chars '-'/'_'/'.'/' '.
func (m *Model) getSoftFieldNameCreated(database, schema, table string) string {
	// It checks whether this feature disabled.
	if m.db.GetConfig().TimeMaintainDisabled {
		return ""
	}
	tableName := ""
	if table != "" {
		tableName = table
	} else {
		tableName = m.tablesInit
	}
	config := m.db.GetConfig()
	if config.CreatedAt != "" {
		return m.getSoftFieldName(database, schema, tableName, []string{config.CreatedAt})
	}
	return m.getSoftFieldName(database, schema, tableName, m.db.GetCore().GetCreatedFiledNames())
}

// getSoftFieldNameUpdate checks and returns the field name for record updating time.
// If there's no field name for storing updating time, it returns an empty string.
// It checks the key with or without cases or chars '-'/'_'/'.'/' '.
func (m *Model) getSoftFieldNameUpdated(database, schema, table string) (field string) {
	// It checks whether this feature disabled.
	if m.db.GetConfig().TimeMaintainDisabled {
		return ""
	}
	tableName := ""
	if table != "" {
		tableName = table
	} else {
		tableName = m.tablesInit
	}
	config := m.db.GetConfig()
	if config.UpdatedAt != "" {
		return m.getSoftFieldName(database, schema, tableName, []string{config.UpdatedAt})
	}
	return m.getSoftFieldName(database, schema, tableName, m.db.GetCore().GetUpdatedFiledNames())
}

// getSoftFieldNameDelete checks and returns the field name for record deleting time.
// If there's no field name for storing deleting time, it returns an empty string.
// It checks the key with or without cases or chars '-'/'_'/'.'/' '.
func (m *Model) getSoftFieldNameDeleted(database, schema, table string) (field string) {
	// It checks whether this feature disabled.
	if m.db.GetConfig().TimeMaintainDisabled {
		return ""
	}
	tableName := ""
	if table != "" {
		tableName = table
	} else {
		tableName = m.tablesInit
	}
	config := m.db.GetConfig()
	if config.DeletedAt != "" {
		return m.getSoftFieldName(database, schema, tableName, []string{config.DeletedAt})
	}
	return m.getSoftFieldName(database, schema, tableName, m.db.GetCore().GetDeletedFiledNames())
}

// getSoftFieldName retrieves and returns the field name of the table for possible key.
func (m *Model) getSoftFieldName(database, schema, table string, keys []string) (field string) {
	// Ignore the error from TableFields.
	fieldsMap, _ := m.TableFieldsInSchema(table, schema, database)
	if len(fieldsMap) > 0 {
		for _, key := range keys {
			field, _ = gutil.MapPossibleItemByKey(
				gconv.Map(fieldsMap), key,
			)
			if field != "" {
				return
			}
		}
	}
	return
}

// getConditionForSoftDeleting retrieves and returns the condition string for soft deleting.
// It supports multiple tables string like:
// "user u, user_detail ud"
// "user u LEFT JOIN user_detail ud ON(ud.uid=u.uid)"
// "user LEFT JOIN user_detail ON(user_detail.uid=user.uid)"
// "user u LEFT JOIN user_detail ud ON(ud.uid=u.uid) LEFT JOIN user_stats us ON(us.uid=u.uid)".
func (m *Model) getConditionForSoftDeleting() string {
	if m.unscoped {
		return ""
	}
	conditionArray := garray.NewStrArray()
	if gstr.Contains(m.tables, " JOIN ") {
		// Base table.
		match, _ := gregex.MatchString(`(.+?) [A-Z]+ JOIN`, m.tables)
		conditionArray.Append(m.getConditionOfTableStringForSoftDeleting(match[1]))
		// Multiple joined tables, exclude the sub query sql which contains char '(' and ')'.
		matches, _ := gregex.MatchAllString(`JOIN ([^()]+?) ON`, m.tables)
		for _, match := range matches {
			conditionArray.Append(m.getConditionOfTableStringForSoftDeleting(match[1]))
		}
	}
	if conditionArray.Len() == 0 && gstr.Contains(m.tables, ",") {
		// Multiple base tables.
		for _, s := range gstr.SplitAndTrim(m.tables, ",") {
			conditionArray.Append(m.getConditionOfTableStringForSoftDeleting(s))
		}
	}
	conditionArray.FilterEmpty()
	if conditionArray.Len() > 0 {
		return conditionArray.Join(" AND ")
	}
	// Only one table.
	if fieldName := m.getSoftFieldNameDeleted(m.database, m.schema, m.tablesInit); fieldName != "" {
		return fmt.Sprintf(`%s IS NULL`, m.db.GetCore().QuoteWord(fieldName))
	}
	return ""
}

// getConditionOfTableStringForSoftDeleting does something as its name describes.
// Examples for `s`:
// - `test`.`demo` as b
// - `test`.`demo` b
// - `demo`
// - demo
func (m *Model) getConditionOfTableStringForSoftDeleting(s string) string {
	var (
		field    string
		table    string
		schema   string
		database string
		array1   = gstr.SplitAndTrim(s, " ")
		array2   = gstr.SplitAndTrim(array1[0], ".")
	)
	if len(array2) >= 3 {
		table = array2[2]
		schema = array2[1]
		database = array2[0]
	} else if len(array2) == 2 {
		table = array2[1]
		schema = array2[0]
	} else {
		table = array2[0]
	}
	field = m.getSoftFieldNameDeleted(database, schema, table)
	if field == "" {
		return ""
	}
	if len(array1) >= 3 {
		return fmt.Sprintf(`%s.%s IS NULL`, m.db.GetCore().QuoteWord(array1[2]), m.db.GetCore().QuoteWord(field))
	}
	if len(array1) >= 2 {
		return fmt.Sprintf(`%s.%s IS NULL`, m.db.GetCore().QuoteWord(array1[1]), m.db.GetCore().QuoteWord(field))
	}
	return fmt.Sprintf(`%s.%s IS NULL`, m.db.GetCore().QuoteWord(table), m.db.GetCore().QuoteWord(field))
}
