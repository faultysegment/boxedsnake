package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/faultysegment/boxedsnake/api/gen/scheduler/v1/schedulerv1connect"
	"github.com/faultysegment/boxedsnake/internal/db"
	"github.com/faultysegment/boxedsnake/internal/kafka"
	"github.com/faultysegment/boxedsnake/internal/scheduler"
)

func main() {
	log.Println("Starting Scheduler...")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://boxedsnake:boxedsnake@localhost:5432/scheduler?sslmode=disable"
	}
	
	kafkaBrokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		kafkaBrokers = []string{envBrokers}
	}

	database, err := db.NewDB(dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	producer, err := kafka.NewProducer(kafkaBrokers, "tasks")
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := scheduler.NewWorker(database, producer)
	go worker.Start(ctx)

	schedulerServer := scheduler.NewSchedulerServer(database)
	mux := http.NewServeMux()
	path, handler := schedulerv1connect.NewSchedulerServiceHandler(schedulerServer)
	mux.Handle(path, handler)

	addr := ":8081"
	srv := &http.Server{
		Addr: addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	go func() {
		log.Printf("Scheduler listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Scheduler...")
	ctxShutDown, cancelShutDown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutDown()

	if err := srv.Shutdown(ctxShutDown); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("Scheduler stopped")
}
