package sharded

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

const metadataCreateTable = "CREATE TABLE metadata (name VARCHAR(255), shard_number INT);"
const metadataInsertInto = "INSERT INTO metadata VALUES (?, ?);"

// ClusterMetadata is used to
type ClusterMetadata struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	NumShards   int    `json:"numShards"`
	Shards      []string `json:"shards"`
	Columns     int `json:"columns"`
}

type Cluster struct {
	metadata ClusterMetadata
	shardConnections []*sql.DB
}

// NewCluster creates a number of SQLite shards with a given createTable statement
func NewCluster(path string, name string, numShards int, columns int, createTable string) error {
	if err := os.Mkdir(path, 0777); err != nil {
		return fmt.Errorf("Directory already exists")
	}

	// convert path to absolute
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	c := &ClusterMetadata{
		Name: name,
		Path: path,
		NumShards: numShards,
		Shards: make([]string, numShards),
		Columns: columns,
	}

	// create each shard
	for i := 0; i < numShards; i++ {
		dbName := "shard" + strconv.Itoa(i) + ".db"
		dbPath := filepath.Join(path, dbName)

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return err
		}

		db.Exec(createTable)
		db.Exec(metadataCreateTable)
		stmt, err := db.Prepare(metadataInsertInto)
		stmt.Exec(c.Name, i)

		c.Shards[i] = dbName
		db.Close()
	}

	// write config to JSON
	shardfilePath := filepath.Join(path, ".shard")

	f, err := json.Marshal(c)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(shardfilePath, f, 0644)
	if err != nil {
		return err
	}

	return nil
}

// loadCluster loads a sharded database from a directory
func LoadCluster(path string) (*Cluster, error) {
	shardedPath := filepath.Join(path, ".shard")

	b, err := ioutil.ReadFile(shardedPath)

	if err != nil {
		return nil, fmt.Errorf("unable to open .shard file in directory")
	}

	metadata := ClusterMetadata{}

	err = json.Unmarshal(b, &metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid schema")
	}

	numShards := metadata.NumShards

	cluster := &Cluster{
		metadata: metadata,
		shardConnections: make([]*sql.DB, numShards),
	}

	for i := 0; i < numShards; i++ {
		dbName := metadata.Shards[i]
		dbPath := filepath.Join(path, dbName)

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return nil, err
		}

		cluster.shardConnections[i] = db
	}

	return cluster, nil
}