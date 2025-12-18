package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	// "syscall"
)

var netBuffer []byte

var READ_BYTE byte = 'R'
var WRITE_BYTE byte = 'W'

var connectedSockets map[uint64]bool
var toWriteMap map[uint64]string

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

func readEvent(ev syscall.Kevent_t, kqfd int) string {
	clear(netBuffer)
	fmt.Println("Read event on id: ", ev.Ident)
	readData, err := syscall.Read(int(ev.Ident), netBuffer)
	if err != nil {
		fmt.Println("Failed to read event...")
		fmt.Println(err)
	}
	echoData := string(netBuffer[:readData])
	toWriteMap[ev.Ident] = toWriteMap[ev.Ident] + echoData
	addWriteEvent(int(ev.Ident), kqfd)
	fmt.Println("\nRead: ", echoData, "\n")
	return string(netBuffer[:readData])
}

func disconnectEvent(ev syscall.Kevent_t, kqFd int) {
	fmt.Println("Disconnection request received!", ev.Fflags)
	deleteEvent := syscall.Kevent_t{
		Ident:  uint64(ev.Ident),
		Flags:  syscall.EV_DELETE,
		Filter: syscall.EVFILT_READ,
		//Flags: syscall.EV_ADD,
	}
	_, err := syscall.Kevent(kqFd, []syscall.Kevent_t{deleteEvent}, nil, nil)
	if err != nil {
		fmt.Println("Error adding deletion of connection from kqueue")
		fmt.Println(err)
	} else {
		delete(connectedSockets, uint64(ev.Data))
		syscall.Close(int(ev.Ident))
		fmt.Println("Connection closed successfully!")
	}
}

func newIncomingConnection(ev syscall.Kevent_t, kqFd, socketFD int) error {
	nfd, _, err := syscall.Accept(socketFD)
	if err != nil {
		fmt.Println("Error accepting connection!")
		fmt.Println(err)
		return err
	}
	event := syscall.Kevent_t{
		Ident:  uint64(nfd),                        // fd of the file we want to track
		Filter: syscall.EVFILT_READ,                // Take the fd as the identifier, and events to watch are in the fflags param
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE, // Add the event |
		Udata:  &READ_BYTE,
	}
	_, err = syscall.Kevent(kqFd, []syscall.Kevent_t{event}, nil, nil)
	if err != nil {
		fmt.Println("Error registering new socket! ", err)
		return err
	}
	connectedSockets[uint64(nfd)] = true
	// //syscall.Accept(int(data))
	fmt.Println("New connection - ", ev.Data, " added!")
	return nil
}

func addWriteEvent(nfd, kqFd int) error {
	event := syscall.Kevent_t{
		Ident:  uint64(nfd),                       // fd of the file we want to track
		Filter: syscall.EVFILT_WRITE,              // Take the fd as the identifier, and events to watch are in the fflags param
		Flags:  syscall.EV_ADD | syscall.EV_CLEAR, // Add the event |
	}
	_, err := syscall.Kevent(kqFd, []syscall.Kevent_t{event}, nil, nil)
	if err != nil {
		fmt.Println("Error registering new socket! ", err)
		return err
	}
	//toWriteMap[uint64(nfd)] = contentToWrite
	fmt.Println("toWriteMap: ")
	fmt.Println(toWriteMap)
	return nil
}

func write(ev syscall.Kevent_t, kqfd int) error {

	contentToWrite, ok := toWriteMap[ev.Ident]
	if !ok {
		return errors.New("No content to write to FD")
	}
	if contentToWrite == "" || len(contentToWrite) == 0 {
		fmt.Println("We are writing emtpy strings. Maybe event not terminating correctly?")
	}

	fd := int(ev.Ident)

	fmt.Println("Writing to ", fd, contentToWrite)
	n, err := syscall.Write(fd, []byte(contentToWrite))
	fmt.Println("Wrote ", n, " bytes..")
	if err != nil {
		fmt.Println("Error when writing to ", fd)
		fmt.Println(err)
		return err
	}
	if n >= len(contentToWrite) {
		// fully written. Done!
		contentToWrite = ""
		delete(toWriteMap, uint64(fd))
		deleteEvent := syscall.Kevent_t{
			Ident:  uint64(ev.Ident),
			Filter: syscall.EVFILT_WRITE,
			Flags:  syscall.EV_DELETE,
		}
		_, err := syscall.Kevent(kqfd, []syscall.Kevent_t{deleteEvent}, nil, nil)
		if err != nil {
			fmt.Println("Error adding deletion of connection from kqueue")
			fmt.Println(err)
		}
		return nil
	}
	fmt.Println("Wrote ", (contentToWrite)[:n])
	toWriteMap[ev.Ident] = contentToWrite[n:]
	addWriteEvent(fd, kqfd)
	return nil
}

func main() {
	fmt.Println("Hello World!")

	// Define a socket to create an endpoint
	socketFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Socket created")
	defer syscall.Close(socketFD)

	err = syscall.Bind(socketFD, &syscall.SockaddrInet4{
		Port: 8080,
		Addr: [4]byte{0, 0, 0, 0},
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Bound to 0.0.0.0:8080...")

	if err = syscall.Listen(socketFD, 4); err != nil {
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
	fmt.Println("Socket file descriptor: ", socketFD)
	event := syscall.Kevent_t{
		Ident:  uint64(socketFD),                                      // fd of the file we want to track
		Filter: syscall.EVFILT_READ,                                   // Take the fd as the identifier, and events to watch are in the fflags param
		Flags:  syscall.EV_ADD | syscall.EV_CLEAR | syscall.EV_ENABLE, // Add the event |
		Fflags: 0,
		Data:   0,
		Udata:  nil,
	}
	// fmt.Println("NFD: ", nfd)
	kqFd, err := syscall.Kqueue()

	connectedSockets = make(map[uint64]bool)
	toWriteMap = make(map[uint64]string)

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

	for {
		clear(retEvents)
		n, err := syscall.Kevent(kqFd, []syscall.Kevent_t{}, retEvents, nil)
		if err != nil {
			fmt.Println("Error when running kevent")
			fmt.Println(err)
			break
		}

		if len(netBuffer) == 0 {
			netBuffer = make([]byte, 5)
		}
		fmt.Println("KQueue ID: ", kqFd)
		if n > 0 {
			fmt.Println("Event returned..")
			fmt.Println(n, " events returned")
			for _, ev := range retEvents[:n] {
				fmt.Println("Ident: ", ev.Ident)
				data := ev.Ident
				_, ok := connectedSockets[data]

				if data == uint64(socketFD) {
					// A new incoming connection! since event's identifier is the network socket
					newIncomingConnection(ev, kqFd, socketFD)
				} else if ok && ev.Flags&syscall.EV_EOF > 0 {
					disconnectEvent(ev, kqFd)
				} else if ok {
					if ev.Udata != nil && ev.Udata == &READ_BYTE {
						fmt.Println("READ!")
						readEvent(ev, kqFd)
					} else {
						// Write event
						fmt.Println("WRITE!")
						fmt.Println(toWriteMap)
						write(ev, kqFd)
					}
				}

			}
		}
	}
}
