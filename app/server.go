package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type StoreValue struct {
	value     string
	timeout   int
	timeAdded int
}

func checkAllValuesForExpired(store map[string]StoreValue) {
	for key, value := range store {
		if value.timeout != 0 {
			if value.timeAdded+(value.timeout/1000) < int(time.Now().Unix()) {
				delete(store, key)
			}
		}
	}
}

func handleSetCommand(buf []byte, store map[string]StoreValue) map[string]StoreValue {
	split := bytes.Split(buf, []byte("\r\n"))

	key := string(split[4])
	value := string(split[6])

	timeout, err := strconv.Atoi(string(split[10]))
	if err != nil {
		timeout = 0
	}

	store[key] = StoreValue{value: value, timeout: timeout, timeAdded: int(time.Now().Unix())}
	return store
}

func handleGetCommand(buf []byte, store map[string]StoreValue) string {
	split := bytes.Split(buf, []byte("\r\n"))
	key := split[4]

	if _, ok := store[string(key)]; ok {
		v := store[string(key)].value
		return "+" + v + "\r\n"
	} else {
		return "$-1\r\n"
	}
}

func handleRequest(conn net.Conn, store map[string]StoreValue) {
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

		command := strings.ToLower(string(bytes.Split(buf, []byte("\r\n"))[2]))

		switch command {
		case "ping":
			conn.Write([]byte("+PONG\r\n"))
		case "echo":
			message := bytes.Split(buf, []byte("\r\n"))[4]
			conn.Write([]byte("+" + string(message) + "\r\n"))
		case "set":
			store = handleSetCommand(buf, store)
			conn.Write([]byte("+OK\r\n"))
		case "get":
			value := handleGetCommand(buf, store)
			conn.Write([]byte(value))
		default:
			conn.Write([]byte("+OK\r\n"))
		}
	}
	fmt.Println("Client disconnected: ", conn.RemoteAddr().String())
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")

	store := make(map[string]StoreValue)
	go func() {
		for {
			checkAllValuesForExpired(store)
			time.Sleep(500 * time.Millisecond)
		}
	}()

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
