package isolation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	// "time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	// "time"
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

type Result struct {
	res *int
	err error
}

func (s *MySuite) runScenario(scenario string) ([]int, error) {
	ctx := context.Background()
	commands := strings.Split(strings.Trim(scenario, "\n"), "\n")

	aliceTx, err := p.BeginTx(ctx, pgx.TxOptions{})
	s.Require().NoError(err)
	bobTx, err := p.BeginTx(ctx, pgx.TxOptions{})
	s.Require().NoError(err)

	defer aliceTx.Rollback(ctx)
	defer bobTx.Rollback(ctx)

	c := make(chan *Q, len(commands))

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
		c <- q
	}
	close(c)

	aliceQ := make(chan *Q)
	bobQ := make(chan *Q)

	done := make(chan bool)

	var result []int

	exec := func(q *Q, tx pgx.Tx) chan Result {
		ch := make(chan Result)
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			wg.Done()

			if q.alice {
				fmt.Printf(aliceColor+"\n", q.q)
			} else {
				fmt.Printf(bobPadding+bobColor+"\n", q.q)
			}
			r := Result{}
			if strings.ToUpper(q.q) == "COMMIT" {
				err := tx.Commit(ctx)
				r.err = err
			} else if strings.HasPrefix(strings.ToUpper(q.q), "SELECT") {
				row := tx.QueryRow(ctx, q.q)
				var v *int
				err := row.Scan(v)
				if err != nil {
					fmt.Printf("err: %v!!", err)
					if strings.Contains(err.Error(), "number of field descriptions must equal number of destinations") {
						err = nil
					}
					if _, ok := err.(pgx.ScanArgError); ok {
						err = nil
					}
					r.err = err
				} else {
					fmt.Printf("v: %v!!", v)
					r.res = v
				}
			} else {
				tt, err := tx.Exec(ctx, q.q)
				if err == nil && (tt.Update() || tt.Select()) {
					s.Require().Greater(tt.RowsAffected(), int64(0))
				}
				r.err = err
			}

			// time.Sleep(50 * time.Millisecond)
			ch <- r
			close(ch)
		}()
		wg.Wait()
		return ch
	}

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
					// time.Sleep(20 * time.Millisecond)
					res := <-exec(q, tx)
					if res.err != nil {
						errs <- res.err
						break L
					} else if res.res != nil {
						result = append(result, *res.res)
					}
				case _ = <-done:
					errs <- nil
					break L
				}
			}
			close(errs)
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
			// if i > 0 {
			// 	time.Sleep(15 * time.Millisecond)
			// }
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
				return result, er
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
				return result, er
			} else {
				success += 1
			}
		case _ = <-done:
			return result, nil
		}
	}
}
