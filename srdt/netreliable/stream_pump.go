package netreliable

import (
	"context"
	"io"
	"time"

	"example.com/srdt/reliable"
)

// streamReadEvent borrows data from the reader pump's single reusable buffer.
// The network owner MUST signal consumed after it has copied/enqueued data.
type streamReadEvent struct {
	data []byte
	err  error
}

type streamReaderPump struct {
	events   <-chan streamReadEvent
	consumed chan<- struct{}
}

func startStreamReader(ctx context.Context, src io.Reader, chunkSize int, wake func()) streamReaderPump {
	events := make(chan streamReadEvent, 1)
	consumed := make(chan struct{}, 1)
	go func() {
		defer close(events)
		buf := make([]byte, chunkSize)
		emptyReads := 0
		for {
			n, err := src.Read(buf)
			if n > 0 {
				emptyReads = 0
				event := streamReadEvent{data: buf[:n], err: err}
				select {
				case events <- event:
					if wake != nil {
						wake()
					}
				case <-ctx.Done():
					return
				}
				select {
				case <-consumed:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
				continue
			}
			if err != nil {
				select {
				case events <- streamReadEvent{err: err}:
					if wake != nil {
						wake()
					}
				case <-ctx.Done():
				}
				return
			}
			emptyReads++
			if emptyReads >= 100 {
				select {
				case events <- streamReadEvent{err: io.ErrNoProgress}:
					if wake != nil {
						wake()
					}
				case <-ctx.Done():
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	return streamReaderPump{events: events, consumed: consumed}
}

type streamWriteJob struct {
	response  *reliable.Received
	requestID uint64
}

type streamWriteResult struct {
	requestID uint64
	bytes     int
	err       error
}

type streamWriterPump struct {
	jobs chan<- streamWriteJob
	done <-chan streamWriteResult
}

func startStreamWriter(ctx context.Context, dst io.Writer, wake func()) streamWriterPump {
	jobs := make(chan streamWriteJob)
	done := make(chan streamWriteResult, 1)
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case job, ok := <-jobs:
				if !ok {
					return
				}
				result := streamWriteResult{requestID: job.requestID}
				if job.response != nil {
					result.bytes = len(job.response.Data)
					result.err = writeFull(dst, job.response.Data)
					job.response.Release()
				}
				select {
				case done <- result:
					if wake != nil {
						wake()
					}
				case <-ctx.Done():
					return
				}
				if result.err != nil {
					return
				}
			}
		}
	}()
	return streamWriterPump{jobs: jobs, done: done}
}
