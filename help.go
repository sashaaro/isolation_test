package isolation

import (
	"context"
	"os"
	"testing"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Account struct {
	Name    string
	Balance int
}

func queryAccounts(tx *pgx.Tx) []*Account {
	var l []*Account
	err := pgxscan.Select(context.Background(), *tx, &l, "Select name, balance FROM account")
	if err != nil {
		panic(err)
	}
	return l
}

func sumWithTx(tx *pgx.Tx) int {
	var sum int
	err := (*tx).QueryRow(context.Background(), "SELECT sum(balance) FROM account").Scan(&sum)
	if err != nil {
		panic(err)
	}
	return sum
}

func sumBalance() int {
	conn, bob := createAliceBob(createPool())
	bob.Release()

	tx, err := conn.Begin(context.Background())
	if err != nil {
		panic(err)
	}

	sum := sumWithTx(&tx)

	//conn.Release()
	err = tx.Rollback(context.Background())
	if err != nil {
		panic(err)
	}

	return sum
}

func assertInitAccounts(t *testing.T, list []*Account) {
	if len(list) != 2 {
		t.Errorf("acc count not equal 2")
	}

	if list[0].Balance != 10 || list[1].Balance != 20 {
		t.Errorf("acc fixtures wrong")
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
	_, _ = alice.Exec(context.Background(), `set application_name to "Alice"`)
	bob, err := pool.Acquire(context.Background()) //createPool().Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	_, _ = bob.Exec(context.Background(), `set application_name to "Bob"`)

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
