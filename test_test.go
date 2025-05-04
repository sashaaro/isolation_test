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
		databaseUrl = "postgresql://postgres:password@localhost:5432/postgres"
	}
	var err error
	p, err = pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		panic(err)
	}

	_, err = p.Exec(context.Background(), (`
create table if not exists users
(
    id int constraint user_pk primary key,
    name    varchar(255) not null
)`))
	if err != nil {
		panic(err)
	}

	_, err = p.Exec(context.Background(), (`
create table if not exists account
(
    name    varchar(255) not null constraint account_pk primary key,
    balance integer not null,
    user_id int not null constraint fk_user references users(id)
);`))
	if err != nil {
		panic(err)
	}

	suite.Run(t, new(MySuite))
}
