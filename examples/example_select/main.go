package main

import (
	"database/sql"
	"github.com/ilknarf/sharded-sqlite"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

func main() {
	path := "test/t1"

	defer os.RemoveAll(path)

	query := "CREATE TABLE t1 (c1 TEXT UNIQUE NOT NULL, c2 INT, c3 INT);"

	// column 0 is the hashed value
	err := sharded.NewCluster(path, "t1", 5, 3, query, 0)

	if err != nil {
		return
	}

	c, err := sharded.LoadCluster(path)

	if err != nil {
		return
	}

	err = c.InsertFullValue("hello1@example.com", 3, 5)

	// use custom INSERT statement and indicate hash value index
	err = c.InsertValue("INSERT INTO t1 (c1, c2) VALUES (?, ?)", 0, "hello1@example.com", 3)

	rows, err := c.ParallelQuery("SELECT * FROM t1;")
	if err != nil {
		return
	}

	c1 := sql.NullString{}
	c2 := sql.NullInt32{}
	c3 := sql.NullInt32{}

	rows.Next()
	err = rows.Scan(&c1, &c2, &c3)
}