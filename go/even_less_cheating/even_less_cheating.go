package main

// https://github.com/hashicorp/boundary/blob/07bca9dd6694d0b84e6c2a432548ab5b70fc3e07/internal/daemon/worker/proxy/tcp/tcp.go

// there is no way to limit the number of OS threads that Go runtime creates; it uses thread-per-concurrent-syscall model
// and GOMAXPROCS only limits how many threads can be running at the same time, not how many can be created
// see https://github.com/golang/go/issues/4056
//
// and some (impractical IMO) attempts at workaround https://developpaper.com/how-to-effectively-control-the-number-of-go-threads/

// does _not_ use zero-copy

import (
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
)

const SIZE = 32 * 1024

// https://github.com/tailscale/tailscale/blob/3ea6ddbb5f55eb7d709b170ed8db655facfe5283/control/controlbase/conn.go#L398
type msgBuffer struct {
	buf      [SIZE]byte
	occupied int
}

// bufPool holds the temporary buffers for Conn.Read & Write.
var bufPool = &sync.Pool{
	New: func() interface{} {
		return new(msgBuffer)
	},
}

func getMsgBuffer() *msgBuffer {
	return bufPool.Get().(*msgBuffer)
}

func main() {
	log.Println("gomaxprocs: ", runtime.GOMAXPROCS(0))

	go listenAndServe(5101, 5201)
	go listenAndServe(5102, 5202)
	go listenAndServe(5103, 5203)
	go listenAndServe(5104, 5204)

	// this will block forever
	select {}
}

func listenAndServe(incomingPort int, outgoingPort int) {
	ln, err := net.Listen("tcp", fmt.Sprint(":", incomingPort))
	mustNot(err)
	for {
		conn, err := ln.Accept()
		mustNot(err)

		go handleConnection(conn, fmt.Sprint(":", outgoingPort))
	}
}

func handleConnection(incomingConn net.Conn, outgoingAddr string) {
	outgoingConn, err := net.Dial("tcp", outgoingAddr)
	mustNot(err)

	io := make(chan *msgBuffer)
	oi := make(chan *msgBuffer)

	go readIntoChan(io, incomingConn)
	go readIntoChan(oi, outgoingConn)
	go func() {
		c := 0
		for c != 2 {
			select {
			case buf, more := <-io:
				if !more {
					outgoingConn.Close()
					c++
				} else {
					writeBufIntoConn(buf, outgoingConn)
				}
			case buf, more := <-oi:
				if !more {
					incomingConn.Close()
					c++
				} else {
					writeBufIntoConn(buf, incomingConn)
				}
			}
		}
		_ = outgoingConn.Close()
		_ = incomingConn.Close()
	}()
}

func writeBufIntoConn(buf *msgBuffer, conn net.Conn) bool {
	_, _ = conn.Write(buf.buf[0:buf.occupied])
	bufPool.Put(buf)
	return true
}

func readIntoChan(c chan *msgBuffer, incomingConn net.Conn) {
	for {
		buf := getMsgBuffer()
		nr, er := incomingConn.Read(buf.buf[:])
		if nr > 0 {
			buf.occupied = nr
			c <- buf
		}
		if er != nil {
			if er != io.EOF {
				break
			}
			break
		}
	}
	close(c)
}

func mustNot(err error) {
	if err != nil {
		panic(err)
	}
}
