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

	"github.com/faultysegment/boxedsnake/api/gen/history/v1/historyv1connect"
	"github.com/faultysegment/boxedsnake/internal/history"
)

func main() {
	log.Println("Starting History Service...")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://boxedsnake:boxedsnake@localhost:5432/scheduler?sslmode=disable"
	}
	
	kafkaBrokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		kafkaBrokers = []string{envBrokers}
	}

	database, err := history.NewDB(dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	historyConsumer, err := history.NewConsumer(kafkaBrokers, "history-group", "task-results", database)
	if err != nil {
		log.Fatalf("Failed to create HistoryConsumer: %v", err)
	}
	defer historyConsumer.Close()
	go historyConsumer.Start(ctx)

	historyServer := history.NewHistoryServer(database)
	mux := http.NewServeMux()
	path, handler := historyv1connect.NewHistoryServiceHandler(historyServer)
	mux.Handle(path, handler)

	addr := ":8082"
	srv := &http.Server{
		Addr: addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	go func() {
		log.Printf("History Service listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down History Service...")
	ctxShutDown, cancelShutDown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutDown()

	if err := srv.Shutdown(ctxShutDown); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("History Service stopped")
}
