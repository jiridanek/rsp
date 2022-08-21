package main

// https://github.com/hashicorp/boundary/blob/07bca9dd6694d0b84e6c2a432548ab5b70fc3e07/internal/daemon/worker/proxy/tcp/tcp.go

// there is no way to limit the number of OS threads that Go runtime creates; it uses thread-per-concurrent-syscall model
// and GOMAXPROCS only limits how many threads can be running at the same time, not how many can be created
// see https://github.com/golang/go/issues/4056
//
// and some (impractical IMO) attempts at workaround https://developpaper.com/how-to-effectively-control-the-number-of-go-threads/

// does _not_ use zero-copy

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
)

func main() {
	log.Println("gomaxprocs: ", runtime.GOMAXPROCS(0))

	go listenAndServe(5101, 5201)
	go listenAndServe(5102, 5202)
	go listenAndServe(5103, 5203)
	go listenAndServe(5104, 5204)

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

	connWg := new(sync.WaitGroup)
	connWg.Add(2)
	go func() {
		defer connWg.Done()
		_, _ = Copy(incomingConn, outgoingConn)
		_ = incomingConn.Close()
		_ = outgoingConn.Close()
	}()
	go func() {
		defer connWg.Done()
		_, _ = Copy(outgoingConn, incomingConn)
		_ = outgoingConn.Close()
		_ = incomingConn.Close()
	}()
	connWg.Wait()
}

// https://cs.opensource.google/go/go/+/refs/tags/go1.19:src/io/io.go;drc=8d57f4dcef5d69a0a3f807afaa9625018569010b;l=405
func Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	size := 32 * 1024
	buf := make([]byte, size)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write")
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func mustNot(err error) {
	if err != nil {
		panic(err)
	}
}
