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

	"github.com/faultysegment/boxedsnake/api/gen/tasks/v1/tasksv1connect"
	"github.com/faultysegment/boxedsnake/internal/kafka"
	"github.com/faultysegment/boxedsnake/internal/server"
)

func main() {
	log.Println("Starting Orchestrator...")

	kafkaBrokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		kafkaBrokers = []string{envBrokers}
	}

	tasksTopic := "tasks"
	resultsTopic := "task-results"

	// Initialize Kafka Producer
	producer, err := kafka.NewProducer(kafkaBrokers, tasksTopic)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Initialize Kafka Consumer
	consumer, err := kafka.NewConsumer(kafkaBrokers, "orchestrator-group", resultsTopic)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Run consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Run(ctx)

	schedulerURL := os.Getenv("SCHEDULER_URL")
	if schedulerURL == "" {
		schedulerURL = "http://localhost:8081"
	}

	historyURL := os.Getenv("HISTORY_URL")
	if historyURL == "" {
		historyURL = "http://localhost:8082"
	}

	// Setup TaskServer and Connect API
	taskServer := server.NewTaskServer(producer, consumer, schedulerURL, historyURL)
	mux := http.NewServeMux()
	path, handler := tasksv1connect.NewTaskServiceHandler(taskServer)
	mux.Handle(path, handler)

	addr := ":8080"
	srv := &http.Server{
		Addr: addr,
		// Use h2c for HTTP/2 cleartext (required for gRPC over HTTP)
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	go func() {
		log.Printf("Orchestrator listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Orchestrator...")
	ctxShutDown, cancelShutDown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutDown()

	if err := srv.Shutdown(ctxShutDown); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("Orchestrator stopped")
}
