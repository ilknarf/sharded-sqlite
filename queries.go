package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

// Parallel query by creating a thread for each DB connection
func (c *Cluster) Select(pre string, post string) (*Rows, error) {
	query := pre + " from " + c.name + " " + post

	wg := sync.WaitGroup{}

	errorChan := make(chan error)
	resultChan := make(chan *sql.Rows)

	hasSchema := false

	aggRows := make([]*Row, 0)
	res := Rows {
		rows: aggRows,
	}

	go func() {
		for {
			// terminate if error exists within top-level
			if len(errorChan) != 0 {
				return
			}

			// adds all rows to result
			chanLen := len(resultChan)
			for i := 0; i < chanLen; i++ {
				rows := <-resultChan

				if !hasSchema {
					res.columnTypes, _ = rows.ColumnTypes()
					res.columns, _ = rows.Columns()
				}

				for rows.Next() {
					row := Row{}

					err := rows.Scan(&row.id, &row.date, &row.memo, &row.topic)
					if err != nil {
						errorChan <- err
						return
					}

					aggRows = append(aggRows, &row)
				}
			}
		}
	}()

	// parallel query execution
	for _, shard := range c.shards {
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

	return &res, nil
}

// Row represents a row of a specific table schema whose parameters we can scan into with sql.Rows.Scan
type Row struct {
	id    int
	date  string
	memo  string
	topic string
}

type Rows struct {
	rows        []*Row
	columnTypes []*sql.ColumnType
	columns     []string
}