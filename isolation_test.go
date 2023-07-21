package isolation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
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
	// fmt.Printf("reinit\n")
	ctx := context.Background()
	pool, err := createPool().Acquire(ctx)
	assert.NoError(t, err)
	conn := pool.Conn()

	_, err = conn.Exec(ctx, "TRUNCATE account")
	assert.NoError(t, err)

	sql, err := os.ReadFile("./fixtures.sql")
	assert.NoError(t, err)

	_, err = conn.Exec(ctx, string(sql))
	assert.NoError(t, err)

	err = conn.Close(ctx)
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

	acc := queryAccounts(&aliceTx)
	assertInitAccounts(t, acc)

	r, err := aliceTx.Exec(context.Background(), "update account set balance = balance + 5 where name = 'A'")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), r.RowsAffected())

	sum := sumWithTx(aliceTx)
	if sum != 35 {
		t.Errorf("alice should see own uncommitted changes %v", sum)
	}

	sum = sumWithTx(bobTx)
	if sum != 30 {
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

		// bob read all
		assertInitAccounts(t, queryAccounts(&bobTx))

		r, err := aliceTx.Exec(context.Background(), "UPDATE account SET balance = balance + 1 where name = 'A'")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), r.RowsAffected())

		err = aliceTx.Commit(context.Background())
		assert.NoError(t, err)
		return sumWithTx(bobTx)
	}

	sum := testNonRepeatableRead(pgx.ReadCommitted)
	if sum != 31 {
		t.Errorf("bob should not use repeatable read & sum should be update. sum:  %v", sum)
	}

	sum = testNonRepeatableRead(pgx.RepeatableRead)
	if sum != 30 {
		t.Errorf("bob should return no update sum. sum: %v", sum)
	}
	sum = testNonRepeatableRead(pgx.Serializable)
	if sum != 30 {
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

		// bob read all
		assertInitAccounts(t, queryAccounts(&bobTx))

		r, err := aliceTx.Exec(context.Background(), "INSERT INTO account values ('C', 30)")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), r.RowsAffected())

		err = aliceTx.Commit(context.Background())
		assert.NoError(t, err)

		return sumWithTx(bobTx)
	}

	sum := testPhantomRead(pgx.ReadCommitted)
	if sum != 60 {
		t.Errorf("bob should return updated sum. sum: %v", sum)
	}

	// no include inserted record
	sum = testPhantomRead(pgx.RepeatableRead)
	if sum != 30 {
		t.Errorf("bob should see no updated sum. sum: %v", sum)
	}
	sum = testPhantomRead(pgx.Serializable)
	if sum != 30 {
		t.Errorf("bob should see no updated sum. sum: %v", sum)
	}
}

var aliceErrUpdate40001 = errors.New("alice tx update 40001")

type Q struct {
	q     string
	alice bool
}

func runScenario(ctx context.Context, t *testing.T, scenario string) error {
	setupTest(t)
	commands := strings.Split(strings.Trim(scenario, "\n"), "\n")

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()
	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, pgx.ReadCommitted) //bobTxLevel)
	// defer aliceTx.Conn().Close(ctx)
	// defer bobTx.Conn().Close(ctx)

	c := make(chan Q, len(commands))

	for _, v := range commands {
		p := strings.Split(v, "|")
		alice := strings.TrimSpace(p[0])
		bob := ""
		if len(p) == 2 {
			bob = p[1]
			bob = strings.TrimSpace(bob)
		}
		var q Q
		if bob != "" {
			q.alice = false
			q.q = bob
		} else if alice != "" {
			q.alice = true
			q.q = alice
		} else {
			panic("no sql")
		}
		c <- q
	}
	close(c)

	aliceQ := make(chan Q)
	bobQ := make(chan Q)

	done := make(chan bool)

	const (
		AliceColor = "\033[1;34m%s\033[0m"
		BobColor   = "                \033[1;33m%s\033[0m"
	)
	run := func(qCh chan Q, tx pgx.Tx) chan error {
		errs := make(chan error)
		go func() {
		L:
			for {
				select {
				case q, ok := <-qCh:
					if !ok {
						errs <- nil
						break L
					}
					if q.alice {
						fmt.Printf(AliceColor+"\n", q.q)
					} else {
						fmt.Printf(BobColor+"\n", q.q)
					}
					if strings.ToUpper(q.q) == "COMMIT" {
						err := tx.Commit(ctx)
						if err != nil {
							errs <- err
							close(errs)
							break L
						}
					} else {
						tt, err := tx.Exec(ctx, q.q)
						if err != nil {
							errs <- err
							close(errs)
							break L
						} else {
							if tt.Update() || tt.Select() {
								assert.Equal(t, int64(1), tt.RowsAffected())
							}
						}
					}
				case _ = <-done:
					errs <- nil
					break L
				}
			}
		}()

		return errs
	}

	aliceErrCh := run(aliceQ, aliceTx)
	bobErrCh := run(bobQ, bobTx)

	go func() {
		i := 0
		for q := range c {
			defer func() {
				i = i + 1
			}()
			c := bobQ
			if q.alice {
				c = aliceQ
			}
			if i > 0 {
				time.Sleep(100 * time.Millisecond)
			}
			c <- q
		}
		close(aliceQ)
		close(bobQ)
	}()

	select {
	case r := <-aliceErrCh:
		close(done)
		return r
	case r := <-bobErrCh:
		close(done)
		return r
	case _ = <-done:
		return nil
		// default:
		// 	return nil
	}
}

var isPgError = func(err error, pgError string) bool {
	pgErr, ok := err.(*pgconn.PgError)
	return ok && pgErr.Code == pgError
}

// https://www.youtube.com/watch?v=Qcpsx2INYdU
func TestLostUpdate(t *testing.T) {
	var testLostUpdate = func(t *testing.T, bobTxLevel pgx.TxIsoLevel) error {
		ctx := context.Background()
		scenario := `
                                                 | SET TRANSACTION ISOLATION LEVEL ` + strings.ToUpper(string(bobTxLevel)) + ` 
                                                 | SELECT balance FROM account WHERE name = 'A'
UPDATE account SET balance = 12 WHERE name = 'A' | 
COMMIT                                           | 
                                                 | UPDATE account SET balance = 15 WHERE name = 'A'
                                                 | COMMIT`

		err := runScenario(ctx, t, scenario)
		if err != nil {
			return err
		}
		return nil
	}

	t.Run("lost update", func(t *testing.T) {
		err := testLostUpdate(t, pgx.ReadCommitted)
		assert.NoError(t, err)
		assert.Equal(t, 35, sumBalance(), "failed produce lost update")
	})

	t.Run("avoid lost update", func(t *testing.T) {
		err := testLostUpdate(t, pgx.RepeatableRead)
		assert.True(t, isPgError(err, pgerrcode.SerializationFailure))
		assert.Equal(t, 32, sumBalance(), "fail. produced lost update read")
	})

	// t.Run("avoid lost update", func(t *testing.T) {
	// 	err := testLostUpdate(t, pgx.Serializable)
	// 	assert.True(t, isPgError(err, pgerrcode.SerializationFailure))
	// 	assert.Equal(t, 32, sumBalance(), "fail. produced lost update read")
	// })
}

// https://medium.com/nerd-for-tech/db-dead-lock-complete-case-study-using-golang-15dd754e5cb8
func TestDeadlockWithTimout(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 30, sumBalance())

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, defaultBobTxLevel)

	assertInitAccounts(t, queryAccounts(&aliceTx))

	_, err := bobTx.Exec(ctx, "UPDATE account SET balance = balance + 5 WHERE name = 'A'")
	if err != nil {
		panic(err)
	}

	// _, err = bobTx.Exec(ctx, "SET LOCAL lock_timeout = '0.5s';")
	// assert.NoError(t, err)

	_, err = aliceTx.Exec(ctx, "UPDATE account SET balance = balance + 4 WHERE name = 'B'")
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, _ = bobTx.Exec(ctx, "UPDATE account SET balance = balance + 3 WHERE name = 'B'")
	}()

	time.Sleep(1 * time.Second)
	_, err = aliceTx.Exec(ctx, "UPDATE account SET balance = balance + 2 WHERE name = 'A'")
	assert.Error(t, err)
	assert.Equal(t, "40P01", err.(*pgconn.PgError).Code)
	assert.Equal(t, "deadlock detected", err.(*pgconn.PgError).Message)
}

func TestForShare(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 30, sumBalance())

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, pgx.ReadCommitted) // no matter even serializable

	accs := queryAccounts(&bobTx)
	assertInitAccounts(t, accs)

	_, err := aliceTx.Exec(ctx, "SELECT * FROM account WHERE name = 'A' FOR SHARE")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SELECT * FROM account WHERE name = 'A' FOR SHARE")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SELECT * FROM account WHERE name = 'A'")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SET LOCAL lock_timeout = '0.1s';")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SELECT * FROM account WHERE name = 'A' FOR UPDATE")
	assert.Error(t, err)
	assert.Equal(t, "55P03", err.(*pgconn.PgError).Code)
	assert.Equal(t, "canceling statement due to lock timeout", err.(*pgconn.PgError).Message)
}

func TestForUpdate(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 30, sumBalance())

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, pgx.ReadCommitted)

	accs := queryAccounts(&bobTx)
	assertInitAccounts(t, accs)

	_, err := aliceTx.Exec(ctx, "SELECT * FROM account WHERE name = 'A' FOR UPDATE")
	assert.NoError(t, err)

	_, err = bobTx.Exec(ctx, "SET LOCAL lock_timeout = '0.1s';")
	assert.NoError(t, err)
	_, err = bobTx.Exec(ctx, "SELECT * FROM account WHERE name = 'A' FOR SHARE")

	assert.Error(t, err)
	assert.Equal(t, "55P03", err.(*pgconn.PgError).Code)
	assert.Equal(t, "canceling statement due to lock timeout", err.(*pgconn.PgError).Message)
}

func TestAdvisoryLock(t *testing.T) {
	setupTest(t)
	ctx := context.Background()

	assert.Equal(t, 30, sumBalance())

	alice, bob := createAliceBob(createPool())
	defer alice.Release()
	defer bob.Release()

	aliceTx := createTx(alice, defaultBobTxLevel)
	bobTx := createTx(bob, pgx.ReadCommitted)

	assertInitAccounts(t, queryAccounts(&bobTx))

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

	assert.Equal(t, 30, sumBalance())
	pool := createPool()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
			_, err = tx.Exec(ctx, "UPDATE account SET balance = balance + 1 WHERE name = 'A'")
			assert.NoError(t, err)
			err = tx.Commit(ctx)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	var p int
	err = tx.QueryRow(context.Background(), "SELECT balance FROM account WHERE name = 'A'").Scan(&p)
	assert.NoError(t, err)
	assert.Equal(t, 1010, p)
}
