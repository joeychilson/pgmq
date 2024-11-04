package pgmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// ReadLimitDefault is the default maximum number of messages to read.
	ReadLimitDefault = 1
	// VisibilityTimeoutDefault is the default message visibility timeout.
	VisibilityTimeoutDefault = 30 * time.Second
	// PollTimeoutDefault is the default maximum time to wait for a message.
	PollTimeoutDefault = 5 * time.Second
	// PollIntervalDefault is the default time to wait between polling attempts.
	PollIntervalDefault = 250 * time.Millisecond
)

// Querier interface defines the required database operations.
type Querier interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Queue represents a typed message queue instance.
type Queue[T any] struct {
	querier Querier
	name    string
}

// Message represents a single message in the queue with metadata.
type Message[T any] struct {
	ID         int64     `json:"msg_id"`
	ReadCount  int       `json:"read_ct"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	VisibleAt  time.Time `json:"vt"`
	Message    T         `json:"message"`
}

// New creates a new Queue instance with the specified name.
// The pgmq extension is automatically created if it does not exist.
func New[T any](ctx context.Context, querier Querier, name string) (*Queue[T], error) {
	if name == "" {
		return nil, fmt.Errorf("queue name cannot be empty")
	}
	_, err := querier.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
	if err != nil {
		return nil, fmt.Errorf("failed to create pgmq extension: %w", err)
	}
	return &Queue[T]{querier: querier, name: name}, nil
}

// WithTx creates a new Queue instance with the specified transaction.
func (q *Queue[T]) WithTx(tx pgx.Tx) *Queue[T] {
	return &Queue[T]{querier: tx, name: q.name}
}

// Create initializes a new queue in the database.
func (q *Queue[T]) Create(ctx context.Context) error {
	_, err := q.querier.Exec(ctx, "SELECT pgmq.create($1)", q.name)
	return err
}

// CreateUnlogged initializes a new unlogged queue in the database.
func (q *Queue[T]) CreateUnlogged(ctx context.Context) error {
	_, err := q.querier.Exec(ctx, "SELECT pgmq.create_unlogged($1)", q.name)
	return err
}

// CreatePartitioned initializes a new partitioned queue in the database.
func (q *Queue[T]) CreatePartitioned(ctx context.Context, partitionInterval, retentionInterval int) error {
	query := "SELECT pgmq.create($1, $2::text, $3::text);"
	_, err := q.querier.Exec(ctx, query, q.name, partitionInterval, retentionInterval)
	if err != nil {
		return fmt.Errorf("failed to create partitioned queue: %w", err)
	}
	return nil
}

// Send adds a new message to the queue.
func (q *Queue[T]) Send(ctx context.Context, message *T) (int64, error) {
	msg, err := json.Marshal(message)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal message: %w", err)
	}

	var msgID int64
	err = q.querier.QueryRow(ctx, "SELECT * FROM pgmq.send($1, $2);", q.name, string(msg)).Scan(&msgID)
	if err != nil {
		return 0, fmt.Errorf("failed to send message: %w", err)
	}
	return msgID, nil
}

// SendDelayed adds a new message with a specified delay.
// delay is the time to wait before the messages become visible in seconds.
func (q *Queue[T]) SendDelayed(ctx context.Context, message *T, delay time.Duration) (int64, error) {
	msg, err := json.Marshal(message)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal message: %w", err)
	}

	var msgID int64
	err = q.querier.QueryRow(ctx, "SELECT * FROM pgmq.send($1, $2, $3);", q.name, string(msg), int(delay.Seconds())).Scan(&msgID)
	if err != nil {
		return 0, fmt.Errorf("failed to send message: %w", err)
	}
	return msgID, nil
}

// SendBatch adds multiple messages to the queue in a single operation.
func (q *Queue[T]) SendBatch(ctx context.Context, messages []*T) ([]int64, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to send")
	}

	jsonMsgs := make([]string, len(messages))
	for i, msg := range messages {
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message: %w", err)
		}
		jsonMsgs[i] = string(jsonMsg)
	}

	rows, err := q.querier.Query(ctx, "SELECT * FROM pgmq.send_batch($1, $2::jsonb[]);", q.name, jsonMsgs)
	if err != nil {
		return nil, fmt.Errorf("failed to send batch: %w", err)
	}
	defer rows.Close()

	var msgIDs []int64
	for rows.Next() {
		var msgID int64
		if err := rows.Scan(&msgID); err != nil {
			return nil, fmt.Errorf("failed to scan message ID: %w", err)
		}
		msgIDs = append(msgIDs, msgID)
	}
	return msgIDs, rows.Err()
}

// SendBatchDelayed adds multiple messages with a specified delay.
// delay is the time to wait before the messages become visible in seconds.
func (q *Queue[T]) SendBatchDelayed(ctx context.Context, messages []*T, delay time.Duration) ([]int64, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to send")
	}

	jsonMsgs := make([]string, len(messages))
	for i, msg := range messages {
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message: %w", err)
		}
		jsonMsgs[i] = string(jsonMsg)
	}

	rows, err := q.querier.Query(ctx, "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3);", q.name, jsonMsgs, int(delay.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("failed to send batch: %w", err)
	}
	defer rows.Close()

	var msgIDs []int64
	for rows.Next() {
		var msgID int64
		if err := rows.Scan(&msgID); err != nil {
			return nil, fmt.Errorf("failed to scan message ID: %w", err)
		}
		msgIDs = append(msgIDs, msgID)
	}
	return msgIDs, rows.Err()
}

// Read retrieves a single message from the queue.
// visibilityTimeout is the time to lock the message in seconds.
func (q *Queue[T]) Read(ctx context.Context, visibilityTimeout time.Duration) (*Message[T], error) {
	query := "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read($1, $2, $3);"

	var msg Message[T]
	err := q.querier.
		QueryRow(ctx, query, q.name, int(visibilityTimeout.Seconds()), 1).
		Scan(&msg.ID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VisibleAt, &msg.Message)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}
	return &msg, nil
}

// ReadWithPoll retrieves a single message with polling support.
// visibilityTimeout is the time to lock the message in seconds.
// pollTimeout is the maximum time to wait for a message in seconds.
// pollInterval is the time to wait between polling attempts in milliseconds.
func (q *Queue[T]) ReadWithPoll(ctx context.Context, visibilityTimeout, pollTimeout, pollInterval time.Duration) (*Message[T], error) {
	var (
		msg    Message[T]
		rawMsg json.RawMessage
	)

	query := "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read_with_poll($1, $2, $3, $4, $5);"

	err := q.querier.
		QueryRow(ctx, query, q.name, int(visibilityTimeout.Seconds()), 1, int(pollTimeout.Seconds()), int(pollInterval.Milliseconds())).
		Scan(
			&msg.ID,
			&msg.ReadCount,
			&msg.EnqueuedAt,
			&msg.VisibleAt,
			&rawMsg,
		)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read message with poll: %w", err)
	}

	if err := json.Unmarshal(rawMsg, &msg.Message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	return &msg, nil
}

// ReadBatch retrieves multiple messages from the queue.
// maxMessages is the maximum number of messages to read.
// visibilityTimeout is the time to lock the messages in seconds.
func (q *Queue[T]) ReadBatch(ctx context.Context, maxMessages int, visibilityTimeout time.Duration) ([]*Message[T], error) {
	query := "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read($1, $2, $3);"

	rows, err := q.querier.Query(ctx, query, q.name, int(visibilityTimeout.Seconds()), maxMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to read messages: %w", err)
	}
	defer rows.Close()

	var messages []*Message[T]
	for rows.Next() {
		var msg Message[T]
		if err := rows.Scan(&msg.ID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VisibleAt, &msg.Message); err != nil {
			return nil, fmt.Errorf("failed to scan message: %w", err)
		}
		messages = append(messages, &msg)
	}
	return messages, rows.Err()
}

// ReadBatchWithPoll retrieves a batch of messages with polling support.
// maxMessages is the maximum number of messages to read.
// visibilityTimeout is the time to lock the messages in seconds.
// pollTimeout is the maximum time to wait for a message in seconds.
// pollInterval is the time to wait between polling attempts in milliseconds.
func (q *Queue[T]) ReadBatchWithPoll(ctx context.Context, maxMessages int, visibilityTimeout, pollTimeout, pollInterval time.Duration) ([]*Message[T], error) {
	query := "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read_with_poll($1, $2, $3, $4, $5);"

	rows, err := q.querier.Query(ctx, query, q.name, int(visibilityTimeout.Seconds()), maxMessages, int(pollTimeout.Seconds()), int(pollInterval.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("failed to read messages: %w", err)
	}
	defer rows.Close()

	var messages []*Message[T]
	for rows.Next() {
		var msg Message[T]
		if err := rows.Scan(&msg.ID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VisibleAt, &msg.Message); err != nil {
			return nil, fmt.Errorf("failed to scan message: %w", err)
		}
		messages = append(messages, &msg)
	}
	return messages, rows.Err()
}

// SetVisibilityTimeout changes the visibility timeout of a message by its ID.
// timeout is the new visibility timeout in seconds.
func (q *Queue[T]) SetVisibilityTimeout(ctx context.Context, msgID int64, timeout time.Duration) error {
	var (
		msg    Message[T]
		rawMsg json.RawMessage
	)

	query := "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.set_vt($1, $2, $3);"

	err := q.querier.
		QueryRow(ctx, query, q.name, msgID, int(timeout.Seconds())).
		Scan(
			&msg.ID,
			&msg.ReadCount,
			&msg.EnqueuedAt,
			&msg.VisibleAt,
			&rawMsg,
		)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("message with ID %d not found", msgID)
	}
	if err != nil {
		return fmt.Errorf("failed to set visibility timeout: %w", err)
	}
	return nil
}

// Pop removes and returns a single message from the queue.
func (q *Queue[T]) Pop(ctx context.Context) (*Message[T], error) {
	var (
		msg    Message[T]
		rawMsg json.RawMessage
	)

	query := "SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.pop($1);"

	err := q.querier.
		QueryRow(ctx, query, q.name).
		Scan(
			&msg.ID,
			&msg.ReadCount,
			&msg.EnqueuedAt,
			&msg.VisibleAt,
			&rawMsg,
		)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("no messages available")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to pop message: %w", err)
	}

	if err := json.Unmarshal(rawMsg, &msg.Message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	return &msg, nil
}

// Archive moves a message to the archive table by its ID.
func (q *Queue[T]) Archive(ctx context.Context, msgID int64) error {
	var success bool

	err := q.querier.QueryRow(ctx, "SELECT pgmq.archive($1, $2::bigint);", q.name, msgID).Scan(&success)
	if err != nil {
		return fmt.Errorf("failed to archive message: %w", err)
	}
	if !success {
		return fmt.Errorf("failed to find message with ID %d", msgID)
	}
	return nil
}

// ArchiveBatch moves multiple messages to the archive table.
func (q *Queue[T]) ArchiveBatch(ctx context.Context, msgIDs []int64) error {
	if len(msgIDs) == 0 {
		return fmt.Errorf("no messages to archive")
	}

	rows, err := q.querier.Query(ctx, "SELECT pgmq.archive($1, $2::bigint[]);", q.name, msgIDs)
	if err != nil {
		return fmt.Errorf("failed to archive messages: %w", err)
	}
	defer rows.Close()

	var archived int
	for rows.Next() {
		var msgID int64
		if err := rows.Scan(&msgID); err != nil {
			return fmt.Errorf("failed to scan message ID: %w", err)
		}
		archived++
	}

	if archived != len(msgIDs) {
		return fmt.Errorf("expected to archive %d messages, archived %d", len(msgIDs), archived)
	}
	return rows.Err()
}

// DetachArchive detaches the archive table from the queue.
func (q *Queue[T]) DetachArchive(ctx context.Context) error {
	_, err := q.querier.Exec(ctx, "SELECT pgmq.detach_archive($1);", q.name)
	if err != nil {
		return fmt.Errorf("failed to detach archive: %w", err)
	}
	return nil
}

// Delete permanently removes a message from the queue by its ID.
func (q *Queue[T]) Delete(ctx context.Context, msgID int64) error {
	var success bool

	err := q.querier.QueryRow(ctx, "SELECT pgmq.delete($1, $2::bigint);", q.name, msgID).Scan(&success)
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	if !success {
		return fmt.Errorf("failed to find message with ID %d", msgID)
	}
	return nil
}

// DeleteBatch permanently removes multiple messages from the queue.
func (q *Queue[T]) DeleteBatch(ctx context.Context, msgIDs []int64) error {
	if len(msgIDs) == 0 {
		return fmt.Errorf("no messages to delete")
	}

	rows, err := q.querier.Query(ctx, "SELECT pgmq.delete($1, $2::bigint[]);", q.name, msgIDs)
	if err != nil {
		return fmt.Errorf("failed to delete messages: %w", err)
	}
	defer rows.Close()

	var deleted int
	for rows.Next() {
		var msgID int64
		if err := rows.Scan(&msgID); err != nil {
			return fmt.Errorf("failed to scan message ID: %w", err)
		}
		deleted++
	}

	if deleted != len(msgIDs) {
		return fmt.Errorf("expected to delete %d messages, deleted %d", len(msgIDs), deleted)
	}
	return rows.Err()
}

// Purge removes all messages from the queue.
func (q *Queue[T]) Purge(ctx context.Context) (int, error) {
	var msgs int

	err := q.querier.QueryRow(ctx, "SELECT pgmq.purge($1);", q.name).Scan(&msgs)
	if err != nil {
		return 0, fmt.Errorf("failed to purge queue: %w", err)
	}
	return msgs, nil
}

// Drop removes the entire queue and all its messages.
func (q *Queue[T]) Drop(ctx context.Context) error {
	var success bool

	err := q.querier.QueryRow(ctx, "SELECT pgmq.drop_queue($1);", q.name).Scan(&success)
	if err != nil {
		return fmt.Errorf("failed to drop queue: %w", err)
	}

	if !success {
		return fmt.Errorf("failed to find queue %s", q.name)
	}
	return nil
}

// QueueMetrics represents the current queue statistics.
type QueueMetrics struct {
	Name             string
	Length           int
	NewestMessageAge *time.Duration
	OldestMessageAge *time.Duration
	TotalMessages    int
	ScrapeTime       time.Time
}

// Metrics retrieves the current queue metrics.
func (q *Queue[T]) Metrics(ctx context.Context) (*QueueMetrics, error) {
	var metrics QueueMetrics

	err := q.querier.
		QueryRow(ctx, "SELECT * FROM pgmq.metrics($1)", q.name).
		Scan(&metrics.Name, &metrics.Length, &metrics.NewestMessageAge, &metrics.OldestMessageAge, &metrics.TotalMessages, &metrics.ScrapeTime)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve metrics: %w", err)
	}
	return &metrics, nil
}

// Metrics retrieves the metrics of all queues in the database.
func Metrics(ctx context.Context, querier Querier) ([]*QueueMetrics, error) {
	rows, err := querier.Query(ctx, "SELECT * FROM pgmq.metrics_all();")
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve metrics: %w", err)
	}
	defer rows.Close()

	var metrics []*QueueMetrics
	for rows.Next() {
		var m QueueMetrics
		if err := rows.Scan(&m.Name, &m.Length, &m.NewestMessageAge, &m.OldestMessageAge, &m.TotalMessages, &m.ScrapeTime); err != nil {
			return nil, fmt.Errorf("failed to scan metrics: %w", err)
		}
		metrics = append(metrics, &m)
	}
	return metrics, rows.Err()
}

// ValidateQueueName checks if the queue name length is within the allowed limits.
func ValidateQueueName(ctx context.Context, querier Querier, name string) error {
	if name == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	_, err := querier.Exec(ctx, "SELECT pgmq.validate_queue_name($1);", name)
	return err
}

// ListQueues retrieves a list of all queues in the database.
func ListQueues(ctx context.Context, querier Querier) ([]string, error) {
	rows, err := querier.Query(ctx, "SELECT queue_name FROM pgmq.list_queues();")
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var queue string
		if err := rows.Scan(&queue); err != nil {
			return nil, fmt.Errorf("failed to scan queue name: %w", err)
		}
		queues = append(queues, queue)
	}
	return queues, rows.Err()
}
