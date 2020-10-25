package sharded

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCluster_InsertFullValue(t *testing.T) {
	path := filepath.Join(testDir, "t1")
	defer os.RemoveAll(path)

	query := "CREATE TABLE t1 (c1 TEXT UNIQUE NOT NULL, c2 INT, c3 INT);"

	err := NewCluster(path, "t1", 5, 3, query, 0)

	if err != nil {
		t.Log("unable to create cluster")
		t.Fatal(err)
	}

	c, err := LoadCluster(path)

	if err != nil {
		t.Log("unable to load cluster")
		t.Fatal(err)
	}

	key := "hello1@example.com"
	err = c.InsertFullValue(key, 3, 5)
	shardIndex, _ := c.Hash(key)

	if err != nil {
		t.Log("unable to insert value")
		t.Fatal(err)
	}

	shard := c.shardConnections[shardIndex]

	res, err := shard.Query(`Select * FROM t1 WHERE c1 = '` + key + `';`)

	if err != nil {
		t.Log("unable to query shard")
		t.Fatal(err)
	}

	if !res.Next() {
		t.Log("unable to find insertion in shard")
		t.Fail()
	}
}

func TestCluster_InsertValue(t *testing.T) {
	path := filepath.Join(testDir, "t1")
	defer os.RemoveAll(path)

	query := "CREATE TABLE t1 (c1 TEXT UNIQUE NOT NULL, c2 INT, c3 INT);"

	err := NewCluster(path, "t1", 5, 3, query, 0)

	if err != nil {
		t.Log("unable to create cluster")
		t.Fatal(err)
	}

	c, err := LoadCluster(path)

	if err != nil {
		t.Log("unable to load cluster")
		t.Fatal(err)
	}

	key := "hello1@example.com"
	err = c.InsertValue("INSERT INTO t1 (c1, c2) VALUES (?, ?)",0, key, 3)
	shardIndex, _ := c.Hash(key)

	if err != nil {
		t.Log("unable to insert value")
		t.Fatal(err)
	}

	shard := c.shardConnections[shardIndex]

	res, err := shard.Query(`SELECT * from t1 WHERE c1 = '` + key + `';`)

	if err != nil {
		t.Log("unable to query shard")
		t.Fatal(err)
	}

	if !res.Next() {
		t.Log("unable to find insertion in shard")
		t.Fail()
	}
}