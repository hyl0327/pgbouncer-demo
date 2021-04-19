package queryer

import (
	"time"

	_ "github.com/lib/pq"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

type PqSqlx struct {
	db *sqlx.DB
}

func NewPqSqlx(url string, maxOpen, maxIdle int, maxLifetime time.Duration) PqSqlx {
	db, err := sqlx.Connect("postgres", url)
	if err != nil {
		logrus.Fatal("Failed to connect to PG: ", err)
	}
	logrus.Debug("Connected to PG")

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLifetime)

	return PqSqlx{db: db}
}

func (q PqSqlx) Query(n int) {
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
