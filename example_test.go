// Example of minimal multiple SQL insert
package sqlbatchinsert_test

import (
	sbi "github.com/krolaw/sqlbatchinsert"

	"context"
	"database/sql"
	"fmt"
)

func ExampleMultiInsert() {
	var ctx context.Context

	db, err := sql.Open("mysql", "root:@/DBNAME") // Or however you open your mysql
	if err != nil {
		panic(err) // Or some graceful handling
	}

	defer db.Close()

	tx, err := db.BeginTx(ctx)
	if err != nil {
		panic(err) // Or some graceful handling
	}

	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	bc := sbi.Config{Tx: tx, ReturnIds: true}

	s := bc.NewBatchInsert("PayPalDonations", []string{"Email", "Amount"})

	for i := 1; i <= 100; i++ {
		if err = s.Insert(ctx, "richard@warburton.it", i); err != nil {
			return
		}
	}

	var ids []int64
	if ids, err = s.Flush(ctx); err != nil {
		return
	}

	fmt.Println("Inserted IDs: ", ids)
}
