package examples

func queryData() {
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

	go func() {
		for {
			addRows()
		}
	}()
}