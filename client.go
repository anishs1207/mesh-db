package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:7000")
	if err != nil {
		fmt.Println("Connection failed:", err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	commands := []string{
		"PING",
		"SET user:123 500",
		"GET user:123",
		"GET unknown",
	}

	for _, command := range commands {
		fmt.Fprintf(conn, "%s\n", command)

		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Read failed:", err)
			return
		}

		fmt.Printf("> %s\n", command)
		fmt.Printf("< %s", response)
		fmt.Println(strings.TrimSpace(""))
	}
}
