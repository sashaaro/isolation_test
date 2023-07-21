package isolation

import (
	"context"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
)

var isPgError = func(err error, pgError string) bool {
	pgErr, ok := err.(*pgconn.PgError)
	return ok && pgErr.Code == pgError
}

type Account struct {
	Name    string
	Balance int
}

func queryAccounts(tx *pgx.Tx) []*Account {
	var l []*Account
	err := pgxscan.Select(context.Background(), *tx, &l,
		"SELECT name, balance FROM account",
	)
	if err != nil {
		panic(err)
	}
	return l
}

func aBalance() int {
	var balance int
	err := p.QueryRow(context.Background(), "SELECT balance FROM account WHERE name = 'A'").Scan(&balance)
	if err != nil {
		panic(err)
	}
	return balance
}

func sumBalance() int {
	var sum int
	err := p.QueryRow(context.Background(), "SELECT SUM(balance) FROM account").Scan(&sum)
	if err != nil {
		panic(err)
	}
	return sum
}

func checkInitialData(t *testing.T, list []*Account) {
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

const defaultIsolationLevel = pgx.ReadCommitted
