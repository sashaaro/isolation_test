package isolation

import (
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
)

// https://www.postgresql.org/docs/current/transaction-iso.html#:~:text=Read%20Committed%20is%20the%20default%20isolation%20level%20in%20PostgreSQL.
// https://www.youtube.com/watch?v=pomxJOFVcQs
// https://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads
// https://www.youtube.com/watch?v=e9a4ESSHQ74

// any tx level is allowed, test should pass, no effect for Alice's transaction
const defaultBobTxLevel = pgx.ReadUncommitted

// const defaultBobTxLevel = pgx.RepeatableRead
// const defaultBobTxLevel = pgx.Serializable

var selectAllSQL = "SELECT name, balance FROM account"
var selectSumSQL = "SELECT SUM(balance) FROM account"

func (s *MySuite) TestUncommittedDirtyRead() {
	var test = func(bobTxLevel pgx.TxIsoLevel) (int, error) {
		scenario := `
                                                          | SET TRANSACTION ISOLATION LEVEL ` + strings.ToUpper(string(bobTxLevel)) + ` 
UPDATE account SET balance = balance + 5 WHERE name = 'A' | 
		                                                      | SELECT balance FROM account WHERE name = 'A'
`
		sum, err := s.runScenario(scenario)
		if err != nil {
			return 0, err
		}
		return sum, nil
	}

	sum, err := test(pgx.ReadCommitted)
	s.Require().NoError(err)
	s.Require().Equal(10, sum, "bob should not see alice uncommitted changes")
}

func (s *MySuite) TestNonRepeatableRead() {
	var test = func(bobTxLevel pgx.TxIsoLevel) (int, error) {
		scenario := `
                                                          | SET TRANSACTION ISOLATION LEVEL ` + strings.ToUpper(string(bobTxLevel)) + `
                                                          | ` + selectAllSQL + `
UPDATE account SET balance = balance + 5 WHERE name = 'A' | 
COMMIT                                                    | 
		                                                      | SELECT balance FROM account WHERE name = 'A'
`
		sum, err := s.runScenario(scenario)
		if err != nil {
			return 0, err
		}
		return sum, nil
	}

	s.Run("non-repeatable read", func() {
		sum, err := test(pgx.ReadCommitted)
		s.Require().NoError(err)
		s.Require().Equal(15, sum, "bob should see alice changes, non-repeatable read produced")
	})
	s.Run("repeatable read", func() {
		sum, err := test(pgx.RepeatableRead)
		s.Require().NoError(err)
		s.Require().Equal(10, sum, "bob should not see alice changes, repeatable read produced")
	})
}

func (s *MySuite) TestPhantomRead() {
	var testPhantomRead = func(bobTxLevel pgx.TxIsoLevel) (int, error) {
		scenario := `
                                       | SET TRANSACTION ISOLATION LEVEL ` + strings.ToUpper(string(bobTxLevel)) + ` 
                                       | ` + selectAllSQL + ` 
INSERT INTO account values ('C', 5, 3) | 
DELETE FROM account WHERE name = 'A'   | 
COMMIT                                 | 
                                       | ` + selectSumSQL + ` 
                                       | COMMIT`

		sum, err := s.runScenario(scenario)
		if err != nil {
			return 0, err
		}
		return sum, nil
	}

	s.Run("phantom read", func() {
		sum, err := testPhantomRead(pgx.ReadCommitted)
		s.Require().NoError(err)
		s.Require().Equal(25, sum, "bob can see alice insert, phantom read produced")
	})

	s.Run("avoid phantom read", func() {
		sum, err := testPhantomRead(pgx.RepeatableRead)
		s.Require().NoError(err)
		s.Require().Equal(30, sum, "bob should not see alice insert, phantom read not work")
	})
}

func (s *MySuite) TestSerializable() {
	var test = func(bobTxLevel pgx.TxIsoLevel) (int, error) {
		bobLevel := strings.ToUpper(string(bobTxLevel))
		scenario := `
                                                                | SET TRANSACTION ISOLATION LEVEL ` + bobLevel + ` 
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE                    |
` + selectSumSQL + `                                            |
INSERT INTO account(name, balance, user_id) values ('C', 30, 3) | 
                                                                | ` + selectSumSQL + `
                                                                | INSERT INTO account(name, balance, user_id) values ('D', 33, 3)       
                                                                | ` + selectSumSQL + `
COMMIT                                                          | 
                                                                | COMMIT`

		sum, err := s.runScenario(scenario)
		if err != nil {
			return 0, err
		}
		return sum, nil
	}

	s.Run("serializable", func() {
		_, err := test(pgx.Serializable)
		s.Require().Error(err)
		s.Require().True(isPgError(err, pgerrcode.SerializationFailure))
	})

	s.Run("serializable not work with repeatable read", func() {
		sum, err := test(pgx.RepeatableRead)
		s.Require().NoError(err)
		s.Require().Equal(63, sum)
	})
}
