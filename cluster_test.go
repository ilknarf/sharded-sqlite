package sharded

import (
	"os"
	"path/filepath"
	"testing"
)

const testDir = "target/test"

func TestNewCluster(t *testing.T) {
	path := filepath.Join(testDir,"NewCluster")
	defer os.RemoveAll(path)

	query := "CREATE TABLE t1 (c1 INT, c2 INT, c3 VARCHAR(255), c4 INT);"
	err := NewCluster(path, "testtable", 4, 4, query, 0)

	if err != nil {
		t.Error(err)
	}
}

func TestLoadCluster(t *testing.T) {
	path := filepath.Join(testDir,"LoadCluster")
	defer os.RemoveAll(path)

	query := "CREATE TABLE t1 (c1 INT, c2 INT, c3 VARCHAR(255), c4 INT);"
	err := NewCluster(path, "testtable", 4, 4, query, 0)

	if err != nil {
		t.Error(err)
	}

	_, err = LoadCluster(path)

	if err != nil {
		t.Error(err)
	}
}
