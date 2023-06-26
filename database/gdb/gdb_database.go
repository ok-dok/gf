// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

// Database is a database object from which it can then create a Model.
type Database struct {
	DB
}

// Database creates and returns a database.
func (c *Core) Database(database string) *Database {
	// Do not change the database of the original db,
	// it here creates a new db and changes its database.
	db, err := NewByGroup(c.GetGroup())
	if err != nil {
		panic(err)
	}
	core := db.GetCore()
	// Different database share some same objects.
	core.logger = c.logger
	core.cache = c.cache
	core.database = database
	return &Database{
		DB: db,
	}
}
