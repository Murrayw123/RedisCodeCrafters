package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
)

func handleRequest(conn net.Conn, store map[string]string) {
	fmt.Println("Client connected: ", conn.RemoteAddr().String())

	for {
		fmt.Println("Waiting for data")
		buf := make([]byte, 1024)

		if _, err := conn.Read(buf); err != nil {
			if err == io.EOF {
				// client has closed the connection
				break
			} else {
				fmt.Println("error reading from client: ", err.Error())
				os.Exit(1)
			}
		}

		fmt.Println("RAW Received: ", string(buf))

		command := bytes.Split(buf, []byte("\r\n"))[2]

		fmt.Println("Received: ", string(buf)+"\r\n")

		if string(command) == "ping" {
			conn.Write([]byte("+PONG\r\n"))
		} else if string(command) == "echo" {
			message := bytes.Split(buf, []byte("\r\n"))[4]
			conn.Write([]byte("+" + string(message) + "\r\n"))
		} else if string(command) == "SET" {
			key := bytes.Split(buf, []byte("\r\n"))[4]
			value := bytes.Split(buf, []byte("\r\n"))[6]
			store[string(key)] = string(value)
			conn.Write([]byte("+OK\r\n"))
		} else if string(command) == "GET" {
			key := bytes.Split(buf, []byte("\r\n"))[4]
			value := store[string(key)]
			conn.Write([]byte("+" + value + "\r\n"))
		} else {
			conn.Write([]byte("+OK\r\n"))
		}

	}
	fmt.Println("Client disconnected: ", conn.RemoteAddr().String())
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")

	store := make(map[string]string)

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer func() {
		err := l.Close()
		if err != nil {
			fmt.Println("Error closing connection: ", err.Error())
			os.Exit(1)
		}
	}()

	fmt.Println("Waiting for connection")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn, store)

	}

}
