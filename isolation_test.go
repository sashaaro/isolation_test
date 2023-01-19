package isolation

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// https://www.postgresql.org/docs/current/transaction-iso.html#:~:text=Read%20Committed%20is%20the%20default%20isolation%20level%20in%20PostgreSQL.
// https://www.youtube.com/watch?v=pomxJOFVcQs
// https://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads

const bobTxLevel = pgx.ReadCommitted // any tx level is allowed, test should pass, no effect for Alice's transaction

func setupTest(t *testing.T) func(t *testing.T) {
	pool, err := createPool().Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	conn := pool.Conn()

	row, err := conn.Query(context.Background(), "TRUNCATE sale")
	if err != nil {
		panic(err)
	}
	row.Close()

	sql, err := os.ReadFile("./fixtures.sql")
	if err != nil {
		panic(err)
	}

	_, err = conn.Exec(context.Background(), string(sql))
	if err != nil {
		panic(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		panic(err)
	}

	//fmt.Println("load fixtures...")

	return func(t *testing.T) {
		// t.Log("teardown test case")
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestUncommittedDirtyRead(t *testing.T) {
	setupTest(t)

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, pgx.ReadCommitted)
	bobTx := createTx(bob, bobTxLevel)

	sales := querySales(&aliceTx)
	assertInitSales(t, sales)

	row, err := bobTx.Query(context.Background(), "update sale set quantity = quantity + 5 where id = 1")
	if err != nil {
		panic(err)
	}
	row.Close()

	sum := sumSalesWithTx(&aliceTx)
	if sum == 155 {
		t.Errorf("fail. produced dirty read sum equals %v", sum)
	}
	// not allow in pg
}

func TestNonRepeatableRead(t *testing.T) {
	testNonRepeatableRead := func(aliceTxLevel pgx.TxIsoLevel) int {
		setupTest(t)

		alice, bob := createAliceBob(createPool())
		defer alice.Release()
		defer bob.Release()

		aliceTx := createTx(alice, aliceTxLevel)
		bobTx := createTx(bob, bobTxLevel)

		sales := querySales(&aliceTx)
		assertInitSales(t, sales)

		row, err := bobTx.Query(context.Background(), "update sale set quantity = quantity + 5 where id = 1")
		if err != nil {
			panic(err)
		}
		row.Close()
		err = bobTx.Commit(context.Background())
		if err != nil {
			panic(err)
		}
		return sumSalesWithTx(&aliceTx)
	}

	sum := testNonRepeatableRead(pgx.ReadCommitted) // expect 155 if no repeatable read
	if sum == 130 {
		t.Errorf("fail. no produced non repeatable read sum equals %v", sum)
	}

	sum = testNonRepeatableRead(pgx.RepeatableRead)
	if sum == 155 {
		t.Errorf("fail. no produced non repeatable read sum equals %v", sum)
	}
}

func TestPhantomRead(t *testing.T) {
	testPhantomRead := func(aliceTxLevel pgx.TxIsoLevel) int {
		setupTest(t)
		alice, bob := createAliceBob(createPool())
		defer alice.Release()
		defer bob.Release()

		aliceTx := createTx(alice, aliceTxLevel)
		bobTx := createTx(bob, bobTxLevel)

		sales := querySales(&aliceTx)
		assertInitSales(t, sales)

		row, err := bobTx.Query(context.Background(), "insert into sale values (3, 3, 1, 10)")
		if err != nil {
			panic(err)
		}
		row.Close()

		err = bobTx.Commit(context.Background())
		if err != nil {
			panic(err)
		}

		return sumSalesWithTx(&aliceTx)
	}

	sum := testPhantomRead(pgx.ReadCommitted)
	if sum != 140 { // prev select 130, next plus + 10 inserted in committed bob tx
		t.Errorf("fail. no produced phantom read %v", sum)
	}

	sum = testPhantomRead(pgx.RepeatableRead)
	if sum != 130 {
		t.Errorf("fail. produced phantom read %v", sum)
	}
}

var aliceErrUpdate40001 = errors.New("alice tx update 40001")
var aliceErrTxCommitRollback = errors.New("alice tx rollback")

// https://www.youtube.com/watch?v=Qcpsx2INYdU
func TestLostUpdate(t *testing.T) {
	testLostUpdate := func(aliceTxLevel pgx.TxIsoLevel) error {
		setupTest(t)
		alice, bob := createAliceBob(createPool())
		defer alice.Release()
		defer bob.Release()

		aliceTx := createTx(alice, aliceTxLevel)
		bobTx := createTx(bob, pgx.ReadCommitted)

		sales := querySales(&aliceTx)
		assertInitSales(t, sales)

		var bobQuantity int
		var aliceQuantity int
		err := bobTx.QueryRow(context.Background(), "select quantity from sale where id = 1").Scan(&bobQuantity)
		if err != nil {
			return err
		}
		if bobQuantity != 10 {
			t.Errorf("bob quantity is wrong. should is 10, actual is %v, fixture broken", bobQuantity)
		}

		err = aliceTx.QueryRow(context.Background(), "select quantity from sale where id = 1").Scan(&aliceQuantity)
		if err != nil {
			return err
		}
		if aliceQuantity != 10 {
			t.Errorf("alice quantity is wrong. should is 10, actual is %v. read committed not work", aliceQuantity)
		}

		row, err := bobTx.Query(context.Background(), "update sale set quantity = $1 where id = 1", bobQuantity+5) // (10 + 5) * 5 = 75, add 25
		if err != nil {
			return err
		}
		row.Close()

		err = bobTx.Commit(context.Background())
		if err != nil {
			return err
		}

		tag, err := aliceTx.Exec(context.Background(), "update sale set quantity = $1 where id = 1", aliceQuantity+2) // (12 + 5) * 5 = 60, add 10
		if err != nil {
			pgErr, ok := err.(*pgconn.PgError)
			if aliceTxLevel == pgx.RepeatableRead && ok && pgErr.Code == "40001" {
				return aliceErrUpdate40001 // could not serialize access due to concurrent update
			}
			return err
		}
		if tag.Update() && tag.RowsAffected() != 1 {
			return errors.New("no update one sale row")
		}

		err = aliceTx.Commit(context.Background())

		if err != nil {
			if err == pgx.ErrTxCommitRollback && aliceTxLevel == pgx.RepeatableRead {
				return aliceErrTxCommitRollback
			}
			return err
		}

		return nil
	}

	err := testLostUpdate(pgx.ReadCommitted)
	assert.NoError(t, err)                                                 // lost update 75 + 80 = 135
	assert.Equal(t, 140, sumSales(), "fail. no produced lost update read") // 60 + 80 = 140
	// 50 + 80 + 25 (first update) + 10 (second) = 165 (no lost update)

	err = testLostUpdate(pgx.RepeatableRead)
	assert.Equal(t, aliceErrUpdate40001, err)
	assert.Equal(t, 155, sumSales(), "fail. produced lost update read")
}
