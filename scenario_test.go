package isolation

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"
)

type QueryLine struct {
	query   string
	isAlice bool // or bob
}

var bobPadding = strings.Repeat(" ", 22)

const (
	aliceColor = "\033[1;36m%s\033[0m"
	bobColor   = "\033[1;33m%s\033[0m"
	errorColor = "\033[1;31m%s\033[0m"
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

func (s *MySuite) parseLines(scenario string) (int, chan *QueryLine) {
	commands := strings.Split(strings.Trim(scenario, "\n"), "\n")
	n := len(commands)
	queryCh := make(chan *QueryLine, n)

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

		q := &QueryLine{}
		if bob != "" {
			q.isAlice = false
			q.query = bob
		} else if alice != "" {
			q.isAlice = true
			q.query = alice
		} else {
			continue
		}
		queryCh <- q
	}
	close(queryCh)

	return n, queryCh
}

func (s *MySuite) runScenario(scenario string) ([]int, error) {
	ctx := context.Background()

	aliceTx, err := p.BeginTx(ctx, pgx.TxOptions{})
	s.Require().NoError(err)
	bobTx, err := p.BeginTx(ctx, pgx.TxOptions{})
	s.Require().NoError(err)

	defer aliceTx.Rollback(ctx)
	defer bobTx.Rollback(ctx)

	n, queryCh := s.parseLines(scenario)

	aliceCh := make(chan *QueryLine)
	bobCh := make(chan *QueryLine)

	ctx, cancel := context.WithCancelCause(ctx)

	result := make(chan int, n)

	g, ctx := errgroup.WithContext(ctx)

	s.run(ctx, result, g, aliceCh, aliceTx)
	s.run(ctx, result, g, bobCh, bobTx)

	go func() {
		i := 0
		for cQ := range queryCh {
			c := bobCh
			if cQ.isAlice {
				c = aliceCh
			}
			if i > 0 {
				time.Sleep(15 * time.Millisecond)
			}
			c <- cQ
			i = i + 1
		}
		close(aliceCh)
		close(bobCh)
	}()

	err = g.Wait()

	close(result)
	r := make([]int, 0)
	for i := range result {
		r = append(r, i)
	}

	if err != nil {
		cancel(fmt.Errorf("partner tx failed: %w", err))

		qErr := err.(QueryError)

		_, ok := qErr.err.(*pgconn.PgError)
		msg := qErr.err.Error()
		if !ok {
			s.Require().NoError(qErr.err, "error is not pg error")
		}
		if qErr.isAlice {
			fmt.Printf(errorColor+"\n", msg)
		} else {
			fmt.Printf(bobPadding+errorColor+"\n", msg)
		}

		return r, qErr.err
	}

	return r, nil
}

type QueryError struct {
	query   string
	err     error
	isAlice bool
}

func (q QueryError) Error() string {
	return q.err.Error()
}

var _ error = &QueryError{}

func (s *MySuite) run(ctx context.Context, result chan int, g *errgroup.Group, qCh chan *QueryLine, tx pgx.Tx) {
	g.Go(func() error {
		for {
			select {
			case _ = <-ctx.Done():
				return nil
			case q, ok := <-qCh:
				if !ok {
					return nil
				}

				if q.isAlice {
					fmt.Printf(aliceColor+"\n", q.query)
				} else {
					fmt.Printf(bobPadding+bobColor+"\n", q.query)
				}
				if strings.ToUpper(q.query) == "COMMIT" {
					err := tx.Commit(ctx)
					if err != nil {
						return QueryError{q.query, err, q.isAlice}
					}
				} else {
					var err error
					if strings.HasPrefix(strings.ToUpper(q.query), "SELECT") {
						row := tx.QueryRow(ctx, q.query)
						var v int
						err = row.Scan(&v)
						if err != nil {
							if strings.Contains(err.Error(), "number of field descriptions must equal number of destinations") {
								err = nil
							}
							if _, ok := err.(pgx.ScanArgError); ok {
								err = nil
							}
						} else {
							result <- v
						}
					} else {
						tt, err := tx.Exec(ctx, q.query)
						if err == nil && (tt.Update() || tt.Select()) {
							s.Require().Greater(tt.RowsAffected(), int64(0))
						} else if err != nil {
							return QueryError{q.query, err, q.isAlice}
						}
					}
					if err != nil {
						return QueryError{q.query, err, q.isAlice}
					}
				}
			}
		}
	})
}
