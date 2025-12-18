package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	// "syscall"
)

var netBuffer []byte

/*
This doesn't time out!
*/
func read(file *os.File) string {
	if len(netBuffer) == 0 {
		netBuffer = make([]byte, 256)
	}

	var firstByte byte
	var resp strings.Builder
	for {
		clear(netBuffer)
		read, err := file.Read(netBuffer)
		if err != nil && err != io.EOF {
			fmt.Println("Something terrible happened when reading data from this socket..")
			fmt.Println(err)
			break
		}

		resp.Write(netBuffer[:read])
		fmt.Println("Read: ", read)
		if firstByte == 0 {
			firstByte = resp.String()[0]
			// Classify the first bytee into: Simple String/Bulk string
		}
		if err == io.EOF {
			break
		}

		if read > 0 {
			break
		}
	}

	return resp.String()
}

func main() {
	fmt.Println("Hello World!")

	// Define a socket to create an endpoint
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Socket created")
	defer syscall.Close(fd)

	err = syscall.Bind(fd, &syscall.SockaddrInet4{
		Port: 8080,
		Addr: [4]byte{0, 0, 0, 0},
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Bound to 0.0.0.0:8080...")

	if err = syscall.Listen(fd, 4); err != nil {
		fmt.Println(err)
		return
	}

	// LHS: network file descriptor, Client sock address, error
	// nfd, _, err := syscall.Accept(fd)

	//defer syscall.Close(nfd)

	if err != nil {
		fmt.Println(err)
		return
	}

	event := syscall.Kevent_t{
		Ident:  uint64(fd),                        // fd of the file we want to track
		Filter: syscall.EVFILT_READ,               // Take the fd as the identifier, and events to watch are in the fflags param
		Flags:  syscall.EV_ADD | syscall.EV_CLEAR, // Add the event |
		Fflags: 0,
		Data:   0,
		Udata:  nil,
	}
	// fmt.Println("NFD: ", nfd)
	kqFd, err := syscall.Kqueue()

	if err != nil {
		fmt.Println("Error creating new event queue")
		fmt.Println(err)
	}
	_, err = syscall.Kevent(kqFd, []syscall.Kevent_t{event}, nil, nil)

	retEvents := make([]syscall.Kevent_t, 10)
	fmt.Println("Done calling kevent!")
	if err != nil {
		fmt.Println("Error when calling kevent!")
		fmt.Println(err)
	}

	connectedSockets := make(map[uint64]bool) // Have we seen this

	for {
		clear(retEvents)
		n, err := syscall.Kevent(kqFd, []syscall.Kevent_t{}, retEvents, nil)
		if err != nil {
			fmt.Println("Error when running kevent")
			fmt.Println(err)
			break
		}

		if len(netBuffer) == 0 {
			netBuffer = make([]byte, 256)
		}
		fmt.Println("KQueue ID: ", kqFd)
		if n > 0 {
			fmt.Println("Event returned..")
			fmt.Println(n, " events returned")
			for _, ev := range retEvents[:n] {
				fmt.Println("Ident: ", ev.Ident)
				data := ev.Ident
				_, ok := connectedSockets[data]
				if !ok {
					// new socket!
					connectedSockets[data] = true
					event := syscall.Kevent_t{
						Ident:  uint64(data),                      // fd of the file we want to track
						Filter: syscall.EVFILT_READ,               // Take the fd as the identifier, and events to watch are in the fflags param
						Flags:  syscall.EV_ADD | syscall.EV_CLEAR, // Add the event |
					}
					_, err = syscall.Kevent(kqFd, []syscall.Kevent_t{event}, nil, nil)
					if err != nil {
						fmt.Println("Error registering new socket! ", err)
						continue
					}
					//syscall.Accept(int(data))
					fmt.Println("New connection - ", data, " added!")
				} else {
					clear(netBuffer)
					readData, err := syscall.Read(int(ev.Ident), netBuffer)
					if err != nil {
						fmt.Println("Failed to read event...")
						fmt.Println(err)
						continue
					}
					fmt.Println("Read data: ", string(netBuffer[:readData]))
				}

			}
		}
	}
}
