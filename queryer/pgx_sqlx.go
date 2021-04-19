package queryer

import (
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

type PgxSqlx struct {
	db *sqlx.DB
}

func NewPgxSqlx(url string, maxOpen, maxIdle int, maxLifetime time.Duration) PgxSqlx {
	db, err := sqlx.Connect("pgx", url)
	if err != nil {
		logrus.Fatal("Failed to connect to PG: ", err)
	}
	logrus.Debug("Connected to PG")

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLifetime)

	return PgxSqlx{db: db}
}

func (q PgxSqlx) Query(n int) {
	data := []struct {
		Time       time.Time `db:"time"`
		DeviceUUID string    `db:"device_uuid"`
	}{}

	if err := q.db.Select(
		&data,
		`SELECT time, device_uuid FROM machine_status LIMIT $1`,
		n,
	); err != nil {
		logrus.Fatal("Failed to select: ", err)
	}
	logrus.Debugf("Got %v rows of data", len(data))
}
