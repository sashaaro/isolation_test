package isolation

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

// https://www.postgresql.org/docs/current/transaction-iso.html#:~:text=Read%20Committed%20is%20the%20default%20isolation%20level%20in%20PostgreSQL.
// https://www.youtube.com/watch?v=pomxJOFVcQs
// https://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads
// https://www.youtube.com/watch?v=e9a4ESSHQ74

// any tx level is allowed, test should pass, no effect for Alice's transaction
const defaultBobTxLevel = pgx.ReadUncommitted

// const defaultBobTxLevel = pgx.RepeatableRead
// const defaultBobTxLevel = pgx.Serializable

func setupTest(t *testing.T) func(t *testing.T) {
	pool, err := createPool().Acquire(context.Background())
	assert.NoError(t, err)
	conn := pool.Conn()

	row, err := conn.Query(context.Background(), "TRUNCATE sale")
	assert.NoError(t, err)
	row.Close()

	sql, err := os.ReadFile("./fixtures.sql")
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), string(sql))
	assert.NoError(t, err)

	err = conn.Close(context.Background())
	assert.NoError(t, err)

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

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, pgx.ReadCommitted)

	sales := querySales(&aliceTx)
	assertInitSales(t, sales)

	row, err := aliceTx.Query(context.Background(), "update sale set quantity = quantity + 1 where id = 1")
	assert.NoError(t, err)
	row.Close()

	sum := sumSalesWithTx(&aliceTx)
	if sum != 120 {
		t.Errorf("alice should see own uncommitted changes %v", sum)
	}

	sum = sumSalesWithTx(&bobTx)
	if sum != 110 {
		t.Errorf("bob should not see uncommitted alice changes %v", sum)
	}
}

func TestNonRepeatableRead(t *testing.T) {
	testNonRepeatableRead := func(aliceTxLevel pgx.TxIsoLevel) int {
		setupTest(t)

		alice, bob := createAliceBob(createPool())
		defer alice.Release()
		defer bob.Release()

		aliceTx := createTx(bob, defaultBobTxLevel)
		bobTx := createTx(alice, aliceTxLevel)

		// bob read all sales
		sales := querySales(&bobTx)
		assertInitSales(t, sales)

		row, err := aliceTx.Query(context.Background(), "update sale set quantity = quantity + 1 where id = 1")
		assert.NoError(t, err)
		row.Close()
		err = aliceTx.Commit(context.Background())
		assert.NoError(t, err)
		return sumSalesWithTx(&bobTx)
	}

	sum := testNonRepeatableRead(pgx.ReadCommitted)
	if sum != 120 {
		t.Errorf("bob should not use repeatable read & sum should be update. sum:  %v", sum)
	}

	sum = testNonRepeatableRead(pgx.RepeatableRead)
	if sum != 110 {
		t.Errorf("bob should return no update sum. sum: %v", sum)
	}
	sum = testNonRepeatableRead(pgx.Serializable)
	if sum != 110 {
		t.Errorf("bob should return no update sum. sum: %v", sum)
	}
}

func TestPhantomRead(t *testing.T) {
	testPhantomRead := func(aliceTxLevel pgx.TxIsoLevel) int {
		setupTest(t)
		alice, bob := createAliceBob(createPool())
		defer alice.Release()
		defer bob.Release()

		aliceTx := createTx(alice, defaultBobTxLevel)
		bobTx := createTx(bob, aliceTxLevel)

		// bob read all sales
		sales := querySales(&bobTx)
		assertInitSales(t, sales)

		row, err := aliceTx.Query(context.Background(), "insert into sale values (3, 3, 1, 10)")
		assert.NoError(t, err)
		row.Close()

		err = aliceTx.Commit(context.Background())
		assert.NoError(t, err)

		return sumSalesWithTx(&bobTx)
	}

	sum := testPhantomRead(pgx.ReadCommitted)
	if sum != 120 {
		t.Errorf("bob should return updated sum. sum: %v", sum)
	}

	// no include inserted record
	sum = testPhantomRead(pgx.RepeatableRead)
	if sum != 110 {
		t.Errorf("bob should see no updated sum. sum: %v", sum)
	}
	sum = testPhantomRead(pgx.Serializable)
	if sum != 110 {
		t.Errorf("bob should see no updated sum. sum: %v", sum)
	}
}

var aliceErrUpdate40001 = errors.New("alice tx update 40001")

// https://www.youtube.com/watch?v=Qcpsx2INYdU
func TestLostUpdate(t *testing.T) {
	ctx := context.Background()
	testLostUpdate := func(bobTxLevel pgx.TxIsoLevel) error {
		setupTest(t)
		alice, bob := createAliceBob(createPool())
		defer alice.Release()
		defer bob.Release()

		aliceTx := createTx(alice, defaultBobTxLevel)
		bobTx := createTx(bob, bobTxLevel)

		// bob read first sale record
		readAndCheckFirstSale(t, bobTx)

		row, err := aliceTx.Query(ctx, "UPDATE sale SET quantity = $1 WHERE id = 1", 4)
		assert.NoError(t, err)
		row.Close()

		err = aliceTx.Commit(ctx)
		assert.NoError(t, err)

		tag, err := bobTx.Exec(ctx, "UPDATE sale SET quantity = $1 WHERE id = 1", 5)
		if err != nil {
			pgErr, ok := err.(*pgconn.PgError)
			if bobTxLevel != pgx.ReadCommitted && ok && pgErr.Code == "40001" {
				return aliceErrUpdate40001 // could not serialize access due to concurrent update
			}
			return err
		}
		if tag.Update() && tag.RowsAffected() != 1 {
			return errors.New("no update one sale row")
		}

		err = bobTx.Commit(ctx)

		//if err != nil {
		//	if err == pgx.ErrTxCommitRollback && aliceTxLevel == pgx.RepeatableRead {
		//		return aliceErrTxCommitRollback
		//	}
		//	return err
		//}

		return nil
	}

	err := testLostUpdate(pgx.ReadCommitted)
	assert.NoError(t, err)
	assert.Equal(t, 130, sumSales(), "fail. no produced lost update read") // bob update +2 pass, but lose alice +1

	err = testLostUpdate(pgx.RepeatableRead)
	assert.Equal(t, aliceErrUpdate40001, err)
	assert.Equal(t, 120, sumSales(), "fail. produced lost update read") // alice update +1 pass, bob update rejected

	err = testLostUpdate(pgx.Serializable)
	assert.Equal(t, aliceErrUpdate40001, err)
	assert.Equal(t, 120, sumSales(), "fail. produced lost update read")
}

// https://medium.com/nerd-for-tech/db-dead-lock-complete-case-study-using-golang-15dd754e5cb8
func TestDeadlockWithTimout(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 110, sumSales())

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, defaultBobTxLevel)

	sales := querySales(&aliceTx)
	assertInitSales(t, sales)

	_, err := bobTx.Exec(ctx, "update sale set quantity = quantity + 5 where id = 1")
	if err != nil {
		panic(err)
	}

	// _, err = bobTx.Exec(ctx, "SET LOCAL lock_timeout = '0.5s';")
	// assert.NoError(t, err)

	_, err = aliceTx.Exec(ctx, "update sale set quantity = quantity + 3 where id = 2")
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, _ = bobTx.Exec(ctx, "update sale set quantity = quantity + 5 where id = 2")
	}()

	time.Sleep(1 * time.Second)
	_, err = aliceTx.Exec(ctx, "update sale set quantity = quantity + 3 where id = 1")
	assert.Error(t, err)
	assert.Equal(t, "40P01", err.(*pgconn.PgError).Code)
	assert.Equal(t, "deadlock detected", err.(*pgconn.PgError).Message)
}

func TestForShare(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 110, sumSales())

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, pgx.ReadCommitted) // no matter even serializable

	sales := querySales(&bobTx)
	assertInitSales(t, sales)

	_, err := aliceTx.Exec(ctx, "SELECT * FROM sale WHERE id = 1 FOR SHARE")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SELECT * FROM sale WHERE id = 1 FOR SHARE")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SELECT * FROM sale WHERE id = 1")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SET LOCAL lock_timeout = '0.1s';")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SELECT * FROM sale WHERE id = 1 FOR UPDATE")
	assert.Error(t, err)
	assert.Equal(t, "55P03", err.(*pgconn.PgError).Code)
	assert.Equal(t, "canceling statement due to lock timeout", err.(*pgconn.PgError).Message)
}

func TestForUpdate(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 110, sumSales())

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, pgx.ReadCommitted)

	sales := querySales(&bobTx)
	assertInitSales(t, sales)

	_, err := aliceTx.Exec(ctx, "SELECT * FROM sale WHERE id = 1 FOR UPDATE")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SET LOCAL lock_timeout = '0.1s';")
	assert.NoError(t, err)
	_, err = bobTx.Exec(ctx, "SELECT * FROM sale WHERE id = 1 FOR SHARE")

	assert.Error(t, err)
	assert.Equal(t, "55P03", err.(*pgconn.PgError).Code)
	assert.Equal(t, "canceling statement due to lock timeout", err.(*pgconn.PgError).Message)
}

func TestAdvisoryLock(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 110, sumSales())

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, pgx.ReadCommitted)

	sales := querySales(&bobTx)
	assertInitSales(t, sales)

	_, err := aliceTx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(1)")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SET LOCAL lock_timeout = '0.1s';")
	assert.NoError(t, err)
	_, err = bobTx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(1)")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SELECT pg_advisory_xact_lock(1)")
	assert.Error(t, err)
	assert.Equal(t, "55P03", err.(*pgconn.PgError).Code)
	assert.Equal(t, "canceling statement due to lock timeout", err.(*pgconn.PgError).Message)
}

func TestDeadlock(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 110, sumSales())
	pool := createPool()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
			_, err = tx.Exec(ctx, "UPDATE sale SET price = price + 1 WHERE id = 1")
			assert.NoError(t, err)
			err = tx.Commit(ctx)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	var p int
	err = tx.QueryRow(context.Background(), "SELECT price FROM sale WHERE id = 1").Scan(&p)
	assert.NoError(t, err)
	assert.Equal(t, 1010, p)
}
