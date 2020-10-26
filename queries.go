package sharded

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

// InsertFullValue inserts full values into the cluster
func (c *Cluster) InsertFullValue(args ...interface{}) error {
	if len(args) != c.metadata.NumColumns {
		return fmt.Errorf("invalid number of arguments")
	}

	query := "INSERT INTO " + c.metadata.TableName + " VALUES (?"
	// concatenate sql query string
	for i := 1; i < c.metadata.NumColumns; i++ {
		query += ",?"
	}
	query += ");"

	shardIndex, err := c.Hash(args[c.metadata.IdIndex])
	if err != nil {
		// unable to hash index
		return err
	}

	shard := c.shardConnections[shardIndex]
	_, err = shard.Exec(query, args...)

	if err != nil {
		return err
	}

	return nil
}

func (c *Cluster) InsertValue(query string, idIndex int, args ...interface{}) error {
	shardIndex, err := c.Hash(args[idIndex])
	if err != nil {
		return err
	}

	shard := c.shardConnections[shardIndex]
	_, err = shard.Exec(query, args...)

	return err
}

// IdQuery executes a single query based on the provided id value
func (c *Cluster) IndexQuery(query string, idPos int, args ...interface{}) (*sql.Rows, error) {
	shardIndex, err := c.Hash(args[idPos])
	if err != nil {
		// unable to hash index
		return nil, err
	}

	shard := c.shardConnections[shardIndex]

	rows, err := shard.Query(query, args...)

	if err != nil {
		// failed query
		return nil, err
	}

	return rows, nil
}

// ParallelQuery executes a parallel query by creating a thread for each DB connection
func (c *Cluster) ParallelQuery(query string) (*ShardedRows, error) {
	wg := &sync.WaitGroup{}

	errorChan := make(chan error, c.metadata.NumShards)
	resultChan := make(chan *sql.Rows, c.metadata.NumShards)

	// aggregate rows
	res := &ShardedRows{
		rows: make([]*sql.Rows, 0),
		curr: 0,
	}

	// parallel query execution
	for i, shard := range c.shardConnections {
		wg.Add(1)
		go func(i int, shard *sql.DB) {
			defer wg.Done()

			if len(errorChan) != 0 {
				return
			}

			res, err := shard.Query(query)

			if err != nil {
				errorChan <- err
				return
			}

			// send rows to result channel
			resultChan <- res
		}(i, shard)
	}

	wg.Wait()

	if len(errorChan) != 0 {
		return nil, <-errorChan
	}

	for len(resultChan) != 0 {
		n := <-resultChan
		res.rows = append(res.rows, n)
	}

	return res, nil
}

// ShardedRows abstracts away the multiple *sql.Rows structs that result from parallel queries
type ShardedRows struct {
	rows []*sql.Rows
	curr int
	prepared bool
}

// Scan behaves like sql.Rows.Scan with the array of sql.Rows
func (sr *ShardedRows) Scan(args ...interface{}) error {
	if !sr.prepared {
		return fmt.Errorf("Scan called before Next")
	}

	if sr.curr >= len(sr.rows) {
		return fmt.Errorf("No more rows left in ShardedRows")
	}
	// call original Scan function
	return sr.rows[sr.curr].Scan(args...)
}

// Next checks the slice for rows until either there are either no more values or
func (sr *ShardedRows) Next() bool {
	l := len(sr.rows)

	for sr.curr < l && !sr.rows[sr.curr].Next() {
		sr.curr++
	}

	if sr.curr < l {
		sr.prepared = true
		return true
	}

	return false
}