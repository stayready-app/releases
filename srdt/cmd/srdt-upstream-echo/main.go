// srdt-upstream-echo is a laboratory TCP upstream for independent-stream tests.
// It is intentionally separate from srdt-server so its RSS/CPU are not charged
// to the SRDT server in process-level measurements.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "TCP listen address")
	flag.Parse()
	addr, err := net.ResolveTCPAddr("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	fmt.Printf("LISTEN_ADDR=%s\n", ln.Addr())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); _ = ln.Close() }()
	var wg sync.WaitGroup
	var accepted atomic.Uint64
	for {
		conn, err := ln.AcceptTCP()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			log.Fatal(err)
		}
		accepted.Add(1)
		wg.Add(1)
		go func(c *net.TCPConn) {
			defer wg.Done()
			defer c.Close()
			_ = c.SetNoDelay(true)
			buf := make([]byte, 32<<10)
			_, _ = io.CopyBuffer(c, c, buf)
			_ = c.CloseWrite()
		}(conn)
	}
	wg.Wait()
	fmt.Printf("ACCEPTED=%d\n", accepted.Load())
}
