package api

import "context"

// Flusher represents a component that buffers writes and can persist any
// pending data explicitly.
type Flusher interface {
	Flush() error
}

// Table manages the lifecycle and buffered insertion of a single table.
//
// T represents the type of value stored in the table.
type Table[T any] interface {
	Flusher

	Create(ctx context.Context) error
	Insert(ctx context.Context, value T) error
	Drop(ctx context.Context) error
}
