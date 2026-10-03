package streams

import (
	"context"
	"fmt"
	"iter"

	"golang.org/x/sync/errgroup"
)

type WorkerInfo struct {
	ctx   context.Context
	index int
}

func (w WorkerInfo) ContextIndex() (context.Context, int) {
	return w.ctx, w.index
}

func (w WorkerInfo) Context() context.Context {
	return w.ctx
}

func (w WorkerInfo) Index() int {
	return w.index
}

func ParallelForEach[T any](ctx context.Context, n, k int, i iter.Seq[T], f func(winfo WorkerInfo, t T) error) error {
	if n <= 0 {
		return fmt.Errorf("parallel workers must be at least 1")
	}
	if k < 0 {
		return fmt.Errorf("buffer size cannot be negative")
	}

	type envelope struct {
		t T
	}

	g, childCtx := errgroup.WithContext(ctx)
	tChan := make(chan envelope, k)

	g.Go(func() error {
		defer close(tChan)

		for t := range i {
			select {
			case <-childCtx.Done():
				if err := ctx.Err(); err != nil {
					return err
				}
				return nil
			case tChan <- envelope{t: t}:
			}
		}
		return nil
	})
	for l := range n {
		g.Go(func() error {
			winfo := WorkerInfo{
				ctx:   childCtx,
				index: l,
			}
			for env := range tChan {
				if err := f(winfo, env.t); err != nil {
					return fmt.Errorf("error on worker %d: %w", l, err)
				}
			}
			return nil
		})
	}
	return g.Wait()
}

func ParallelForEach2[T, E any](ctx context.Context, n, k int, i iter.Seq2[T, E], f func(winfo WorkerInfo, t T, e E) error) error {
	if n <= 0 {
		return fmt.Errorf("parallel workers must be at least 1")
	}
	if k < 0 {
		return fmt.Errorf("buffer size cannot be negative")
	}

	type envelope struct {
		t T
		e E
	}

	g, childCtx := errgroup.WithContext(ctx)
	tChan := make(chan envelope, k)

	g.Go(func() error {
		defer close(tChan)

		for t, e := range i {
			select {
			case <-childCtx.Done():
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("error on producer: %w", err)
				}
				return nil
			case tChan <- envelope{t: t, e: e}:
			}
		}
		return nil
	})
	for l := range n {
		g.Go(func() error {
			winfo := WorkerInfo{
				ctx:   childCtx,
				index: l,
			}
			for env := range tChan {
				if err := f(winfo, env.t, env.e); err != nil {
					return fmt.Errorf("error on worker %d: %w", l, err)
				}
			}
			return nil
		})
	}
	return g.Wait()
}
