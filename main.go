package main

import (
	"context"
	"flag"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/sirupsen/logrus"

	"gitlab.com/dotzerotech/pgbouncer-demo/queryer"
)

var totalTx int32 = 0

func main() {
	url := os.Getenv("PG_URL")

	// Parse arguments
	var (
		queryerChoice string
		numThreads    int
		timeout       time.Duration
		maxOpen       int
		maxIdle       int
		maxLifetime   time.Duration
	)
	flag.StringVar(&queryerChoice, "queryer", "pgx+sqlx", "Queryer (pgx+sqlx / pgx+gorm / pq+sqlx)")
	flag.IntVar(&numThreads, "numThreads", 10, "Number of threads")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "Timeout")
	flag.IntVar(&maxOpen, "maxOpen", 10, "Max open connections")
	flag.IntVar(&maxOpen, "maxIdle", 5, "Max idle connections")
	flag.DurationVar(&maxLifetime, "maxLifetime", 30*time.Minute, "Max connection lifetime")

	flag.Parse()

	// Sanity checks
	if maxOpen > 30 && !strings.Contains(url, "6432") {
		logrus.Fatal("maxOpen > 30 is allowed only when connecting to pgBouncer (6432 port)")
	}
	if maxIdle > maxOpen {
		logrus.Fatal("maxIdle cannot be greater than maxOpen")
	}

	// Get some work done
	var q queryer.Queryer

	switch queryerChoice {
	case "pgx+sqlx":
		q = queryer.NewPgxSqlx(url, maxOpen, maxIdle, maxLifetime)
	case "pgx+gorm":
		q = queryer.NewPgxGorm(url, maxOpen, maxIdle, maxLifetime)
	case "pq+sqlx":
		q = queryer.NewPqSqlx(url, maxOpen, maxIdle, maxLifetime)
	default:
		logrus.Fatal("Unsupported queryer")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go queryTillDone(ctx, &wg, q)
	}
	wg.Wait()

	// Do some statistics
	timeoutSeconds := timeout.Seconds()
	avgTx := float64(totalTx) / timeoutSeconds

	logrus.Infof("%v tx done in %.0f seconds (%.2f tps)", totalTx, timeoutSeconds, avgTx)
}

func queryTillDone(ctx context.Context, wg *sync.WaitGroup, q queryer.Queryer) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			n := 20 + rand.Intn(6) // 20 ~ 25

			q.Query(n)
			atomic.AddInt32(&totalTx, 1)

			time.Sleep(10 * time.Millisecond)
		}
	}
}
