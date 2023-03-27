package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StoreValue struct {
	value     string
	timeout   int64
	timeAdded int64
}

type Store interface {
	Set(key string, value string, timeout int64)
	Get(key string) string
	GetAll() map[string]StoreValue
	CheckAllValuesForExpired()
}

type InMemoryStore struct {
	store      map[string]StoreValue
	storeMutex sync.Mutex
	Store
}

func (store *InMemoryStore) Set(key string, value string, timeout int64) {
	store.storeMutex.Lock()
	defer store.storeMutex.Unlock()
	store.store[key] = StoreValue{
		value:     value,
		timeout:   timeout,
		timeAdded: getTimeInMilliseconds(),
	}
}

func (store *InMemoryStore) GetAll() map[string]StoreValue {
	store.storeMutex.Lock()
	defer store.storeMutex.Unlock()
	return store.store
}

func (store *InMemoryStore) Get(key string) (string, error) {
	store.storeMutex.Lock()
	defer store.storeMutex.Unlock()
	if val, ok := store.store[key]; ok {
		return val.value, nil
	} else {
		return "", errors.New("expired or missing")
	}
}

func (store *InMemoryStore) CheckAllValuesForExpired() {
	for k, v := range store.GetAll() {
		if v.timeout != 0 && getTimeInMilliseconds() > v.timeAdded+v.timeout {
			store.storeMutex.Lock()
			delete(store.store, k)
			store.storeMutex.Unlock()
		}
	}
}

func getTimeInMilliseconds() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func handleSetCommand(buf []byte, store *InMemoryStore) {
	split := bytes.Split(buf, []byte("\r\n"))

	key := string(split[4])
	value := string(split[6])

	var timeout int
	if len(split) > 10 {
		t, err := strconv.Atoi(string(split[10]))
		if err != nil {
			fmt.Println("Error converting timeout to int: ", err.Error())
		}
		timeout = t
	} else {
		timeout = 0
	}

	store.Set(key, value, int64(timeout))
}

func handleGetCommand(buf []byte, store *InMemoryStore) string {
	split := bytes.Split(buf, []byte("\r\n"))
	key := split[4]

	val, err := store.Get(string(key))
	if err != nil {
		return "$-1\r\n"
	}
	return "+" + val + "\r\n"
}

func handleRequest(conn net.Conn, store *InMemoryStore) {
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
			handleSetCommand(buf, store)
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

	store := InMemoryStore{store: map[string]StoreValue{}}
	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		for range ticker.C {
			store.CheckAllValuesForExpired()
		}
		defer ticker.Stop()
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
		go handleRequest(conn, &store)
	}
}
