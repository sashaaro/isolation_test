package isolation

import (
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
)

func (s *MySuite) TestUpdateLock() {
	test := func(timeout string) (int, error) {
		scenario := `
	                                                        | SET LOCAL lock_timeout = '` + timeout + `'
UPDATE account SET balance = balance + 5 WHERE name = 'A' | 
	                                                        | UPDATE account SET balance = balance + 3 WHERE name = 'A'
COMMIT -- run after 15 milliseconds                       |
                                                          | SELECT balance FROM account WHERE name = 'A'
                                                          | COMMIT
`

		sum, err := s.runScenario(scenario)
		return sum, err
	}

	s.Run("lock update with timeout", func() {
		_, err := test("0.01s")
		s.Require().Error(err)
		s.Require().True(isPgError(err, pgerrcode.LockNotAvailable))
	})
	s.Run("lock update", func() {
		sum, err := test("0.2s")

		s.Require().NoError(err)
		s.Require().Equal(18, sum)
	})
}

func (s *MySuite) TestForShare() {
	test := func(aliceFor, bobFor string) error {
		scenario := `
SELECT * FROM account WHERE name = 'A' FOR ` + aliceFor + ` | 
	                                                          | SET LOCAL lock_timeout = '0.1s'
	                                                          | SELECT * FROM account WHERE name = 'A' FOR ` + bobFor + `
`

		_, err := s.runScenario(scenario)
		return err
	}

	s.Run("select for share && for share", func() {
		err := test("SHARE", "SHARE")
		s.Require().NoError(err)
	})
	s.Run("select for share && for update", func() {
		err := test("SHARE", "UPDATE")
		s.Require().Error(err)
		s.Require().True(isPgError(err, pgerrcode.LockNotAvailable))
	})
	// t.Run("select for update && for share", func(t *testing.T) {
	// 	err := test("UPDATE", "SHARE")
	// 	assert.Error(t, err)
	// 	assert.Equal(t, pgerrcode.LockNotAvailable, err.(*pgconn.PgError).Code)
	// })
	// t.Run("select for update && for update", func(t *testing.T) {
	// 	err := test("UPDATE", "UPDATE")
	// 	assert.Error(t, err)
	// 	assert.Equal(t, pgerrcode.LockNotAvailable, err.(*pgconn.PgError).Code)
	// })
}

func (s *MySuite) TestAdvisoryLock() {
	test := func(bobSharedLock bool, aliceSleep string, bobTimeout string) error {
		bobLock := "pg_advisory_xact_lock(1)"
		if bobSharedLock {
			bobLock = "pg_advisory_xact_lock_shared(1)"
		}
		scenario := `
SELECT pg_advisory_xact_lock_shared(1) |
	                                     | SET LOCAL lock_timeout = '` + bobTimeout + `'
	                                     | SELECT ` + bobLock + ` 
SELECT pg_sleep(` + aliceSleep + `)    |
COMMIT                                 |
		                                   | SELECT 123
`

		res, err := s.runScenario(scenario)
		if err == nil && res != 123 {
			panic("last select not execute") //
		}
		return err
	}
	//
	s.Run("lock_shared && shared", func() {
		err := test(true, "0.3", "0.1s")
		s.Require().NoError(err)
	})
	s.Run("lock_shared && lock", func() {
		err := test(false, "0.3", "0.1s")
		s.Require().Error(err)
		s.Require().True(isPgError(err, pgerrcode.LockNotAvailable))
	})

	s.Run("lock_shared && lock", func() {
		err := test(false, "0.2", "1s")
		s.Require().NoError(err)
	})
}

// https://medium.com/nerd-for-tech/db-dead-lock-complete-case-study-using-golang-15dd754e5cb8
func (s *MySuite) TestDeadlockWithTimout() {
	var testDeadlock = func(bobTxLevel pgx.TxIsoLevel) error {
		scenario := `
                                                          | UPDATE account SET balance = balance + 5 WHERE name = 'A'
UPDATE account SET balance = balance + 4 WHERE name = 'B' |
 		                                                      | UPDATE account SET balance = balance + 3 WHERE name = 'B'
UPDATE account SET balance = balance + 2 WHERE name = 'A' |
`

		_, err := s.runScenario(scenario)
		return err
	}

	err := testDeadlock(pgx.ReadCommitted)

	s.Require().Error(err)
	s.Require().True(isPgError(err, pgerrcode.DeadlockDetected))
}

func (s *MySuite) TestNowait() {
	var testNowait = func(bobTxLevel pgx.TxIsoLevel) error {
		scenario := `
                                                          | UPDATE account SET balance = balance + 5 WHERE name = 'A'
UPDATE account SET balance = balance + 4 WHERE name = 'B' |
 		                                                      | SELECT balance FROM account WHERE name = 'B' FOR UPDATE NOWAIT
`

		_, err := s.runScenario(scenario)
		return err
	}

	err := testNowait(pgx.ReadCommitted)

	s.Require().Error(err)
	s.Require().True(isPgError(err, pgerrcode.LockNotAvailable))
}

func (s *MySuite) TestSkipLocked() {
	var testSkipLocked = func(bobTxLevel pgx.TxIsoLevel) (int, error) {
		scenario := `
SELECT * FROM account WHERE name = 'A' FOR UPDATE |
-- UPDATE account SET balance = 1 WHERE name = 'A'|
                                                  | SELECT balance FROM account ORDER BY name ASC FOR SHARE SKIP LOCKED LIMIT 1
`

		return s.runScenario(scenario)
	}

	r, err := testSkipLocked(pgx.ReadCommitted)

	s.Require().NoError(err)
	s.Require().Equal(20, r, "should skip A and return B")
}
