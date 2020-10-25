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

	err = c.InsertFullValue("hello1@example.com", 3, 5)

	if err != nil {
		t.Log("unable to insert value")
		t.Fatal(err)
	}
}