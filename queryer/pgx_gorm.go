package queryer

import (
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type PgxGorm struct {
	db *gorm.DB
}

func NewPgxGorm(url string, maxOpen, maxIdle int, maxLifetime time.Duration) PgxGorm {
	db, err := gorm.Open(
		postgres.Open(url),
		&gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		},
	)
	if err != nil {
		logrus.Fatal("Failed to connect to PG: ", err)
	}
	logrus.Debug("Connected to PG")

	_db, err := db.DB()
	if err != nil {
		logrus.Fatal("Failed to get database/sql DB object: ", err)
	}
	_db.SetMaxOpenConns(maxOpen)
	_db.SetMaxIdleConns(maxIdle)
	_db.SetConnMaxLifetime(maxLifetime)

	return PgxGorm{db: db}
}

func (q PgxGorm) Query(n int) {
	data := []struct {
		Time       time.Time
		DeviceUUID string
	}{}

	if err := q.db.Table("machine_status").Order("time DESC").Limit(n).Find(&data).Error; err != nil {
		logrus.Fatal("Failed to select: ", err)
	}
	logrus.Debugf("Got %v rows of data", len(data))
}
