package main

// https://github.com/hashicorp/boundary/blob/07bca9dd6694d0b84e6c2a432548ab5b70fc3e07/internal/daemon/worker/proxy/tcp/tcp.go

// there is no way to limit the number of OS threads that Go runtime creates; it uses thread-per-concurrent-syscall model
// and GOMAXPROCS only limits how many threads can be running at the same time, not how many can be created
// see https://github.com/golang/go/issues/4056
//
// and some (impractical IMO) attempts at workaround https://developpaper.com/how-to-effectively-control-the-number-of-go-threads/

// uses zero-copy (implicitly, by calling io.Copy())

import (
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
		_, _ = io.Copy(incomingConn, outgoingConn)
		_ = incomingConn.Close()
		_ = outgoingConn.Close()
	}()
	go func() {
		defer connWg.Done()
		_, _ = io.Copy(outgoingConn, incomingConn)
		_ = outgoingConn.Close()
		_ = incomingConn.Close()
	}()
	connWg.Wait()
}

func mustNot(err error) {
	if err != nil {
		panic(err)
	}
}
