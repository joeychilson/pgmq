# pgmq

A Go library for the [pgmq](https://github.com/tembo-io/pgmq) message queue extension for PostgreSQL.

# Features

- All pgmq features
- Type-safety
- Transactions

## Requirements

- pgmq extension installed in your PostgreSQL database

## Installation

```bash
go get github.com/joeychilson/pgmq
```

## Example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joeychilson/pgmq"
)

// EmailNotification represents an email to be sent
type EmailNotification struct {
	To      string            `json:"to"`
	Subject string            `json:"subject"`
	Body    string            `json:"body"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// EmailProcessor handles the email queue processing
type EmailProcessor struct {
	queue        *pgmq.Queue[EmailNotification]
	emailService EmailService
}

// EmailService interface defines methods for sending emails
type EmailService interface {
	Send(ctx context.Context, email EmailNotification) error
}

// MockEmailService implements EmailService for demonstration
type MockEmailService struct{}

func (m *MockEmailService) Send(ctx context.Context, email EmailNotification) error {
	log.Printf("Sending email to %s: %s\n", email.To, email.Subject)
	return nil
}

func main() {
	ctx := context.Background()

	// Connect to PostgreSQL
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a new typed queue for email notifications
	emailQueue, err := pgmq.New[EmailNotification](ctx, db, "email_notifications")
	if err != nil {
		log.Fatalf("failed to create queue: %v", err)
	}

	// Create the queue table if it doesn't exist
	if err := emailQueue.Create(ctx); err != nil {
		log.Fatalf("failed to create queue table: %v", err)
	}

	// Initialize the email processor
	processor := &EmailProcessor{
		queue:        emailQueue,
		emailService: &MockEmailService{},
	}

	// Start the producer (simulating email requests)
	go func() {
		if err := processor.produceEmails(ctx); err != nil {
			log.Printf("producer error: %v", err)
		}
	}()

	// Start the consumer (processing emails)
	if err := processor.consumeEmails(ctx); err != nil {
		log.Fatalf("consumer error: %v", err)
	}
}

func (p *EmailProcessor) produceEmails(ctx context.Context) error {
	// Simulate sending different types of emails
	emails := []*EmailNotification{
		{
			To:      "user1@example.com",
			Subject: "Welcome to our platform!",
			Body:    "Thank you for signing up...",
			Meta: map[string]string{
				"type": "welcome",
			},
		},
		{
			To:      "user2@example.com",
			Subject: "Your order has shipped",
			Body:    "Your order #12345 is on its way...",
			Meta: map[string]string{
				"type":    "order_status",
				"orderID": "12345",
			},
		},
		{
			To:      "user3@example.com",
			Subject: "Password reset request",
			Body:    "Click here to reset your password...",
			Meta: map[string]string{
				"type": "security",
			},
		},
	}

	// Send emails to the queue
	msgIDs, err := p.queue.SendBatch(ctx, emails)
	if err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	log.Printf("Successfully queued %d emails with IDs: %v", len(msgIDs), msgIDs)
	return nil
}

func (p *EmailProcessor) consumeEmails(ctx context.Context) error {
	log.Println("Starting email consumer...")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Read a batch of up to 10 messages with a 30-second visibility timeout
			messages, err := p.queue.ReadBatch(ctx, 10, pgmq.VisibilityTimeoutDefault)
			if err != nil {
				log.Printf("error reading messages: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if len(messages) == 0 {
				log.Println("no messages available, waiting...")
				time.Sleep(5 * time.Second)
				continue
			}

			// Process each message
			var successIDs []int64
			for _, msg := range messages {
				err := p.emailService.Send(ctx, msg.Message)
				if err != nil {
					log.Printf("failed to send email ID %d: %v", msg.ID, err)
					continue
				}
				successIDs = append(successIDs, msg.ID)
			}

			// Archive successfully processed messages
			if len(successIDs) > 0 {
				if err := p.queue.ArchiveBatch(ctx, successIDs); err != nil {
					log.Printf("failed to archive messages: %v", err)
				}
			}

			// Print queue metrics periodically
			metrics, err := p.queue.Metrics(ctx)
			if err != nil {
				log.Printf("failed to get metrics: %v", err)
			} else {
				log.Printf("Queue metrics - Length: %d, Total Messages: %d",
					metrics.Length, metrics.TotalMessages)
			}
		}
	}
}
```