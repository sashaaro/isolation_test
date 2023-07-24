package isolation

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
)

var p *pgxpool.Pool

type MySuite struct {
	suite.Suite
}

var _ suite.SetupTestSuite = &MySuite{}
var _ suite.TearDownAllSuite = &MySuite{}
var _ suite.SetupSubTest = &MySuite{}

func (s *MySuite) initData() {
	ctx := context.Background()

	_, err := p.Exec(ctx, "TRUNCATE account,users")
	s.Require().NoError(err)

	sql, err := os.ReadFile("./fixtures.sql")
	s.Require().NoError(err)

	_, err = p.Exec(ctx, string(sql))
	s.Require().NoError(err)

	s.Require().Equal(30, sumBalance())
}
func (s *MySuite) SetupTest() {
	s.initData()
}

func (s *MySuite) SetupSubTest() {
	s.initData()
}

func (s *MySuite) TearDownSuite() {
	p.Close()
}

func TestMySuite(t *testing.T) {
	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		databaseUrl = "postgresql://root:root@localhost:5432/acid"
	}
	var err error
	p, err = pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		panic(err)
	}
	suite.Run(t, new(MySuite))
}
