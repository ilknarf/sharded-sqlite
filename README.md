[![Build Status](https://cloud.drone.io/api/badges/ilknarf/sharded-sqlite/status.svg)](https://cloud.drone.io/ilknarf/sharded-sqlite)

# Sharded SQLite

This is a demo library of database sharding through a hash
function, supporting parallel reads. SQLite is used for convenience.

Example Usage:

```go
func ExampleShard() error {
    path := "test/t1"
	defer os.RemoveAll(path)

	query := "CREATE TABLE t1 (c1 TEXT UNIQUE NOT NULL, c2 INT, c3 INT);"

    // column 0 is the hashed value
	err := NewCluster(path, "t1", 5, 3, query, 0)

	if err != nil {
		return err
	}

	c, err := LoadCluster(path)

	if err != nil {
		return err
	}

    // default full insertion into shard
    err = c.InsertFullValue("hello1@example.com", 3, 5)
	
    // use custom INSERT statement and indicate hash value index
	err = c.InsertValue("INSERT INTO t1 (c1, c2) VALUES (?, ?)", 0, "hello1@example.com", 3)

    // queries all shards
	rows, err := c.ParallelQuery("SELECT * FROM t1;")
	if err != nil {
		return err
	}

    // use of go-sqlite3 structs
	c1 := sql.NullString{}
	c2 := sql.NullInt32{}
	c3 := sql.NullInt32{}

	rows.Next()
	err = rows.Scan(&c1, &c2, &c3)
}
```

For simplification, this implementation requires manual specification of index value coordinates.
A real-world implementation might utilize query parsing for optimizations and hashing.