package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")

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
	conn, err := l.Accept()

	for {
		buf := make([]byte, 1024)

		if _, err := conn.Read(buf); err != nil {
			if err == io.EOF {
				break
			} else {
				fmt.Println("error reading from client: ", err.Error())
				os.Exit(1)
			}
		}

		// Let's ignore the client's input for now and hardcode a response.
		// We'll implement a proper Redis Protocol parser in later stages.
		conn.Write([]byte("+PONG\r\n"))
	}
}
