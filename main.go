package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	var port int = 514
	var err error
	var filter net.IP

	// Input Args
	for i, v := range os.Args {
		switch v {
		// Port
		case "-p":
			port, err = strconv.Atoi(os.Args[(i + 1)])
			if err != nil {
				log.Fatalln(err)
			}
			// filter address
		case "-f":
			filter = net.ParseIP(os.Args[(i + 1)])
		}
	}

	ln, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalln(err)
		return
	}

	fmt.Printf("Listening on %s\n", ln.Addr().String())

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		if checkConnection(conn, filter) {
			go handleConnection(conn)
		} else {
			conn.Close()
		}

	}
}

// CheckConnection prevents any unwanted incomming logs from an ip or port
func checkConnection(conn net.Conn, filter net.IP) bool {
	if filter == nil {
		return true
	}

	if strings.Contains(conn.RemoteAddr().String(), filter.String()) {
		return true
	}
	log.Printf("Blocked Connection From: %s\n", conn.RemoteAddr().String())
	return false
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 2096)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Printf("Received: %s\n", buf)
	parseBuffer(buf)
}

func parseBuffer(buf []byte) {
	data := strings.Split(fmt.Sprintf("%s", buf), ",")
	if len(data) != 30 {
		log.Printf("Expeceted 30 columns: Received %d", len(data))
		return
	}
	log.Printf("Received data successfuly")
}

type SMDR_Packet struct {
	CallStart              string
	ConnectedTime          string
	RingTime               string
	Caller                 string
	CallDirection          string
	CalledNumber           string
	DialedNumber           string
	Account                string
	IsInternal             string
	CallId                 string
	Continuation           string
	Party1Device           string
	Party1Name             string
	Party2Device           string
	Party2Name             string
	ExternalTargeterId     string
	HoldTime               string
	ParkTime               string
	AuthValid              string
	AuthCode               string
	UserCharged            string
	CallCharge             string
	Currency               string
	AmountatLastUserChange string
	CallUnits              string
	UnitsatLastUserChange  string
	CostperUnit            string
	MarkUp                 string
	ExternalTargetingCause string
	ExternalTargetedNumber string
}
