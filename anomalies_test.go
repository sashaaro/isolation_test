package isolation

import (
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// https://www.youtube.com/watch?v=Qcpsx2INYdU
func (s *MySuite) TestLostUpdate() {
	var testLostUpdate = func(bobTxLevel pgx.TxIsoLevel) error {
		scenario := `
                                                          | SET TRANSACTION ISOLATION LEVEL ` + strings.ToUpper(string(bobTxLevel)) + ` 
                                                          | SELECT balance FROM account WHERE name = 'A'       -- return 10
UPDATE account SET balance = balance + 2 WHERE name = 'A' | 
COMMIT       -- A balance = 10 + 2                        | 
                                                          | UPDATE account SET balance = 10 + 5 WHERE name = 'A'
                                                          | COMMIT`

		_, err := s.runScenario(scenario)
		return err
	}

	s.Run("lost update", func() {
		err := testLostUpdate(pgx.ReadCommitted)
		s.Require().NoError(err)
		s.Require().Equal(15, aBalance(), "should lose alice update +2")
	})

	s.Run("avoid lost update", func() {
		err := testLostUpdate(pgx.RepeatableRead)
		s.Require().IsType(&pgconn.PgError{}, err)
		s.Require().True(isPgError(err, pgerrcode.SerializationFailure))
		s.Require().Equal(12, aBalance(), "should failure and left alice changes only")
	})
}

// Несогласованная запись
func (s *MySuite) TestDfs() {
	var testUnconsistWrite = func(lv pgx.TxIsoLevel, commentBobReadSum bool, forUpdate bool) error {
		l := strings.ToUpper(string(lv))

		comment := ""
		if commentBobReadSum {
			comment = "--"
		}

		sForUpdate := ""
		if forUpdate {
			sForUpdate = " FOR UPDATE"
		}
		scenario := `
                                                            | SET TRANSACTION ISOLATION LEVEL ` + l + ` 
SET TRANSACTION ISOLATION LEVEL ` + l + `                   |
SELECT * FROM account` + sForUpdate + `                     |
 -- sum = 30 allow increase to 38, not over 40		          | ` + comment + ` SELECT * FROM account` + sForUpdate + ` -- return 30 if will not wait (read commited + for update)
UPDATE account SET balance = balance + 8 WHERE name = 'A'   | -- allow increase to 36, not over 40
COMMIT                                                      |
                                                            | UPDATE account SET balance = balance + 6 WHERE name = 'B'
                                                            | COMMIT
`

		_, err := s.runScenario(scenario)
		return err
	}

	s.Run("repeatable read allow violate", func() {
		err := testUnconsistWrite(pgx.RepeatableRead, false, false)
		s.Require().NoError(err)
		s.Require().Equal(44, sumBalance(), "violate requirement - balance sum should not greater 40")
	})

	s.Run("serializable", func() {
		err := testUnconsistWrite(pgx.Serializable, false, false)
		s.Require().True(isPgError(err, pgerrcode.SerializationFailure))
		s.Require().Equal(38, sumBalance(), "not allow increase and violate requirement, alice changes applied only")
	})

	s.Run("serializable + bob not select accounts before", func() {
		err := testUnconsistWrite(pgx.Serializable, true, false)
		s.Require().NoError(err)
		s.Require().Equal(44, sumBalance(), "violate requirement even serializable but bob not read sum before update")
	})

	s.Run("repeatable read for update should failure", func() {
		err := testUnconsistWrite(pgx.RepeatableRead, false, true)
		s.Require().True(isPgError(err, pgerrcode.SerializationFailure))
		s.Require().Equal(38, sumBalance(), "not allow increase and violate requirement, alice changes applied only")
	})

	s.Run("read committed for update should wait", func() {
		err := testUnconsistWrite(pgx.ReadCommitted, false, true)
		s.Require().NoError(err)
		s.Require().Equal(44, sumBalance(), "will not violate requirement, cause bob select would be wait alice update and not pass condition")
	})
}
