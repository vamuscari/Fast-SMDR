package main

import (
	"fmt"
	"net"
)

func main() {
	ln, err := net.Listen("tcp", ":514")
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go checkConnection(conn)
		go handleConnection(conn)
	}
}

func checkConnection(conn net.Conn) {
	fmt.Printf("Remote Address: %s\n", conn.RemoteAddr().String())
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Received: %s\n", buf)
}
