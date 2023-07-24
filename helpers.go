package isolation

import (
	"context"
	"github.com/jackc/pgx/v5/pgconn"
)

var isPgError = func(err error, pgError string) bool {
	pgErr, ok := err.(*pgconn.PgError)
	return ok && pgErr.Code == pgError
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
