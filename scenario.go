package isolation

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"strings"
	"time"
)

type Q struct {
	q     string
	alice bool
}

var bobPadding = strings.Repeat(" ", 22)

const (
	aliceColor = "\033[1;36m%s\033[0m"
	bobColor   = "\033[1;33m%s\033[0m"
	errorColor = "\033[1;35m%s\033[0m"
)

// func createTx(isolationLevel pgx.TxIsoLevel) pgx.Tx {
// 	tx, err := p.BeginTx(context.Background(), pgx.TxOptions{
// 		IsoLevel:       isolationLevel,
// 		AccessMode:     pgx.ReadWrite,
// 		DeferrableMode: pgx.NotDeferrable,
// 	})
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	return tx
// }

func (s *MySuite) runScenario(scenario string) (int, error) {
	ctx := context.Background()
	commands := strings.Split(strings.Trim(scenario, "\n"), "\n")

	aliceTx, err := p.BeginTx(ctx, pgx.TxOptions{})
	s.Require().NoError(err)
	bobTx, err := p.BeginTx(ctx, pgx.TxOptions{})
	s.Require().NoError(err)

	defer aliceTx.Rollback(ctx)
	defer bobTx.Rollback(ctx)

	c := make(chan *Q, len(commands))

	var lastSelect *Q
	for _, v := range commands {
		p := strings.Split(v, "|")
		alice := strings.TrimSpace(p[0])
		bob := ""
		if len(p) == 2 {
			bob = p[1]
			bob = strings.TrimSpace(bob)
		}

		if strings.HasPrefix(alice, "--") {
			alice = ""
		}
		if strings.HasPrefix(bob, "--") {
			bob = ""
		}

		// remove comments
		leftBob, _, f := strings.Cut(bob, "--")
		if f {
			bob = leftBob
		}
		leftAlice, _, f := strings.Cut(alice, "--")
		if f {
			alice = leftAlice
		}

		q := &Q{}
		if bob != "" {
			q.alice = false
			q.q = bob
		} else if alice != "" {
			q.alice = true
			q.q = alice
		} else {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(q.q), "SELECT") {
			lastSelect = q
		}
		c <- q
	}
	close(c)

	aliceQ := make(chan *Q)
	bobQ := make(chan *Q)

	done := make(chan bool)

	var lastVal int
	run := func(qCh chan *Q, tx pgx.Tx) chan error {
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
						fmt.Printf(aliceColor+"\n", q.q)
					} else {
						fmt.Printf(bobPadding+bobColor+"\n", q.q)
					}
					if strings.ToUpper(q.q) == "COMMIT" {
						err := tx.Commit(ctx)
						if err != nil {
							errs <- err
							close(errs)
							break L
						}
					} else {
						var err error
						if lastSelect == q {
							row := tx.QueryRow(ctx, q.q)
							err = row.Scan(&lastVal)
							if err != nil {
								if strings.Contains(err.Error(), "number of field descriptions must equal number of destinations") {
									err = nil
								}
								if _, ok := err.(pgx.ScanArgError); ok {
									err = nil
								}
							}

						} else {
							tt, err := tx.Exec(ctx, q.q)
							if err == nil && (tt.Update() || tt.Select()) {
								s.Require().Greater(tt.RowsAffected(), int64(0))
							} else if err != nil {
								errs <- err
								close(errs)
								break L
							}
						}
						if err != nil {
							errs <- err
							close(errs)
							break L
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
		for cQ := range c {
			c := bobQ
			if cQ.alice {
				c = aliceQ
			}
			if i > 0 {
				time.Sleep(15 * time.Millisecond)
			}
			c <- cQ
			i = i + 1
		}
		close(aliceQ)
		close(bobQ)
	}()

	success := 0
	for {
		if success == 2 {
			close(done)
		}
		select {
		case er := <-aliceErrCh:
			if er != nil {
				close(done)
				_, ok := er.(*pgconn.PgError)
				msg := er.Error()
				if !ok {
					msg = "ERROR: " + msg
				}
				fmt.Printf(errorColor+"\n", msg)
				return lastVal, er
			} else {
				success += 1
			}
		case er := <-bobErrCh:
			if er != nil {
				close(done)
				_, ok := er.(*pgconn.PgError)
				msg := er.Error()
				if !ok {
					msg = "ERROR: " + msg
				}
				fmt.Printf(bobPadding+errorColor+"\n", msg)
				return lastVal, er
			} else {
				success += 1
			}
		case _ = <-done:
			return lastVal, nil
		}
	}
}
