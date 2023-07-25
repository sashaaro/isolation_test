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

// Несогласованное чтение
func (s *MySuite) TestInconsistentRead() {
	var test = func(lv pgx.TxIsoLevel, selectForShare bool) ([]int, error) {
		l := strings.ToUpper(string(lv))

		forShare := ""
		if selectForShare {
			forShare = " FOR SHARE"
		}

		scenario := `
SET TRANSACTION ISOLATION LEVEL ` + l + `                   |  
                                                            | UPDATE account SET balance = balance - 15 WHERE name = 'B'
SELECT balance FROM account WHERE name = 'B'` + forShare + `|
                                                            | UPDATE account SET balance = balance + 15 WHERE name = 'A'
                                                            | COMMIT
SELECT balance FROM account WHERE name = 'A'` + forShare + `|
COMMIT                                                      |
`

		sum, err := s.runScenario(scenario)
		return sum, err
	}

	s.Run("read commited and inconsistent read", func() {
		v, err := test(pgx.ReadCommitted, false)
		s.Require().NoError(err)
		s.Require().Equal([]int{20, 25}, v, "wrong sum - 45, expected 30") // need use sum(balance) for consistancy
	})

	s.Run("read commited + for share for consistent read", func() {
		v, err := test(pgx.ReadCommitted, true)
		s.Require().NoError(err)
		s.Require().Equal([]int{5, 25}, v, "wrong sum - 45, expected 30")
	})

	s.Run("read commited and wrong write data", func() {
		v, err := test(pgx.RepeatableRead, false)
		s.Require().NoError(err)
		s.Require().Equal([]int{20, 10}, v, "read consistancy data but without last bob changes")
	})
}

// Несогласованная запись
func (s *MySuite) TestInconsistentWrite() {
	var test = func(lv pgx.TxIsoLevel, notBobSelectAccounts bool, forUpdate bool) ([]int, error) {
		l := strings.ToUpper(string(lv))

		comment := ""
		if notBobSelectAccounts {
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
--` + sumBalanceSql + `                                       |
-- sum = 30 allow increase to 38, not over 40		          | ` + comment + ` SELECT * FROM account` + sForUpdate + ` -- return 30 if will not wait (read commited + for update)
                                                            | --` + sumBalanceSql + ` -- allow increase to 36, not over 40
UPDATE account SET balance = balance + 8 WHERE name = 'A'   | 
COMMIT                                                      |
                                                            | UPDATE account SET balance = balance + 6 WHERE name = 'B'
                                                            | COMMIT
`

		res, err := s.runScenario(scenario)
		return res, err
	}

	s.Run("repeatable read allow violate", func() {
		_, err := test(pgx.RepeatableRead, false, false)
		s.Require().NoError(err)
		// s.Require().Equal(30, res[0], "alice sum correctly")
		s.Require().Equal(44, sumBalance(), "violate requirement - balance sum should not greater 40")
	})

	s.Run("serializable", func() {
		_, err := test(pgx.Serializable, false, false)
		s.Require().True(isPgError(err, pgerrcode.SerializationFailure))
		s.Require().Equal(38, sumBalance(), "not allow increase and violate requirement, alice changes applied only")
	})

	s.Run("read committed + for update should wait", func() {
		_, err := test(pgx.ReadCommitted, false, true)
		s.Require().NoError(err)
		s.Require().Equal(44, sumBalance(), "will not violate requirement, cause bob select would be wait alice update and not pass condition")
	})

	s.Run("repeatable read + for update should failure", func() {
		_, err := test(pgx.RepeatableRead, false, true)
		s.Require().True(isPgError(err, pgerrcode.SerializationFailure))
		s.Require().Equal(38, sumBalance(), "not allow increase and violate requirement, alice changes applied only")
	})

	// s.Run("serializable not work if bob not select accounts before", func() {
	// 	_, err := test(pgx.Serializable, true, false)
	// 	s.Require().NoError(err)
	// 	s.Require().Equal(44, sumBalance(), "serializable should work only if bob select accounts before update")
	// })
}
