package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

type Cluster struct {
	name        string
	directory   string
	numShards   int
	shards      []*sql.DB
	columns     int
}

func CreateNewCluster(path string) {

}