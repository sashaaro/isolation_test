package isolation

import (
	"context"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
)

type Sale struct {
	ProductId int
	Quantity  int
	Price     int
}

func querySales(tx *pgx.Tx) []*Sale {
	var sales []*Sale
	err := pgxscan.Select(context.Background(), *tx, &sales, "select product_id, quantity, price from sale")
	if err != nil {
		panic(err)
	}
	return sales
}

func sumSalesWithTx(tx *pgx.Tx) int {
	var sum int
	err := (*tx).QueryRow(context.Background(), "select sum(quantity * price) from sale").Scan(&sum)
	if err != nil {
		panic(err)
	}
	return sum
}

func sumSales() int {
	conn, bob := createAliceBob(createPool())
	bob.Release()

	tx, err := conn.Begin(context.Background())
	if err != nil {
		panic(err)
	}

	sum := sumSalesWithTx(&tx)

	//conn.Release()
	err = tx.Rollback(context.Background())
	if err != nil {
		panic(err)
	}

	return sum
}

func calc(sale Sale) int {
	return sale.Quantity * sale.Price
}

func assertInitSales(t *testing.T, sales []*Sale) {
	if len(sales) != 2 {
		t.Errorf("Sales count not equal 2")
	}

	if calc(*sales[0]) != 50 || calc(*sales[1]) != 80 {
		t.Errorf("Sales fixtures wrong")
	}
}

func createPool() *pgxpool.Pool {
	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		databaseUrl = "postgresql://root:root@localhost:5432/acid"
	}
	pool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		panic(err)
	}
	return pool
}
func createAliceBob(pool *pgxpool.Pool) (*pgxpool.Conn, *pgxpool.Conn) {
	alice, err := pool.Acquire(context.Background()) //createPool().Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	bob, err := pool.Acquire(context.Background()) //createPool().Acquire(context.Background())
	if err != nil {
		panic(err)
	}

	return alice, bob
}

const defaultIsolationLevel = pgx.ReadCommitted

func createTx(conn *pgxpool.Conn, isolationLevel pgx.TxIsoLevel) pgx.Tx {
	tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{
		IsoLevel:       isolationLevel,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	})
	if err != nil {
		panic(err)
	}

	return tx
}
