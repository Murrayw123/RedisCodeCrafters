package main

import (
	"fmt"
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

	for {
		fmt.Println("Waiting for connection")
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting conn: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Connection accepted")
		buf := make([]byte, 1024)
		_, err = conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from conn: ", err.Error())
			os.Exit(1)
		}

		fmt.Println("Read from conn: ", string(buf))
		if err != nil {
			fmt.Println("Error reading from conn: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Successfully read from conn")
		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error writing to conn: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Successfully wrote to conn")
	}
}
