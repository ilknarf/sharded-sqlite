package sharded

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

// Parallel query by creating a thread for each DB connection
func (c *Cluster) Query(query string) (*ShardedRows, error) {
	wg := sync.WaitGroup{}

	errorChan := make(chan error)
	resultChan := make(chan *sql.Rows)

	// aggregate rows
	res := &ShardedRows{
		rows: make([]*sql.Rows, 0),
	}

	addRows := func() {
		// make sure that this function completes before returning
		wg.Add(1)
		defer wg.Done()
		// terminate if error exists within top-level
		if len(errorChan) != 0 {
			return
		}

		// adds all rows to result
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
}