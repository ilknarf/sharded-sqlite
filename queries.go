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

func (c *Cluster) InsertValue(query string, idIndex int, args ...interface{}) (sql.Result, error) {
	shardIndex, err := c.Hash(args[idIndex])
	if err != nil {
		return nil, err
	}

	shard := c.shardConnections[shardIndex]
	res, err := shard.Exec(query, args)

	return res, err
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
	wg := sync.WaitGroup{}

	errorChan := make(chan error)
	resultChan := make(chan *sql.Rows)

	// aggregate rows
	res := &ShardedRows{
		rows: make([]*sql.Rows, 0),
		curr: 0,
	}

	addRows := func() {
		// make sure that this function completes before returning
		wg.Add(1)
		defer wg.Done()
		// terminate if error exists within top-level
		if len(errorChan) != 0 {
			return
		}

		// adds rows to result
		chanLen := len(resultChan)
		for i := 0; i < chanLen; i++ {
			rows := <-resultChan

			res.rows = append(res.rows, rows)
		}
	}

	go func() {
		for {
			addRows()
		}
	}()

	// parallel query execution
	for _, shard := range c.shardConnections {
		go func() {
			if len(errorChan) != 0 {
				return
			}
			wg.Add(1)
			defer wg.Done()

			res, err := shard.Query(query)
			if err != nil {
				errorChan <- err
				return
			}

			// send rows to result channel
			resultChan <- res
		}()
	}

	wg.Wait()

	if len(errorChan) != 0 {
		return nil, <-errorChan
	}

	for i := 0; i < len(resultChan); i++ {

	}

	return res, nil
}

// ShardedRows abstracts away the multiple *sql.Rows structs that result from parallel queries
type ShardedRows struct {
	rows []*sql.Rows
	curr int
}

// Scan behaves like sql.Rows.Scan with the array of sql.Rows
func (sr *ShardedRows) Scan(args ...interface{}) error {
	if sr.curr < len(sr.rows) {
		return fmt.Errorf("ShardedRows is empty")
	}
	c := sr.rows[sr.curr]

	// call original Scan function
	err := c.Scan(args...)

	return err
}

// Next checks the slice for rows until either there are either no more values or
func (sr *ShardedRows) Next() bool {
	l := len(sr.rows)

	for sr.curr < l && sr.rows[sr.curr].Next() {
		sr.curr++
	}

	return sr.curr < l
}