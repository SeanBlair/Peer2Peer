/*
Implements the solution to assignment 3 for UBC CS 416 2016 W2.

Usage:

Bootstrapping:
go run peer.go -b [physical-peerID] [peer-IP:port] [server ip:port]

Joining:
go run peer.go -j [physical-peerID] [peer-IP:port] [other-peer ip:port]

Example:
go run peer.go -b 0 127.0.0.1:19000 127.0.0.1:20000
go run peer.go -j 0 127.0.0.1:19001 127.0.0.1:19000

See run.sh for example running script.
*/

package main

import (
	"fmt"
	"os"
	"strconv"
	//can be deleted, only for example printing
	// "math/rand"
	// TODO
	"net/rpc"
	"net"
	"log"
)

//Modes of operation
const (
	BOOTSTRAP = iota
	JOIN
)

// Resource server type.
type RServer int

// Request that peer sends in call to RServer.InitSession
// Return value is int
type Init struct {
	IPaddr string // Must be set to ""
}

// Request that peer sends in call to RServer.GetResource
type ResourceRequest struct {
	SessionID int
	IPaddr    string // Must be set to ""
}

// Response that the server returns from RServer.GetResource
type Resource struct {
	Resource      string
	LogicalPeerID int
	NumRemaining  int
}

//An array of resources
type Resources []Resource

// My stuffs =======================================================

type PeerAddressAndStatus struct {
	Address string
	Status bool
}

type JoinRequest struct {
	MyAddress string
}

type JoinResponse struct {
	SessID int
	RServerAddress string
	// all except myIpPort
	AllPeers []PeerAddressAndStatus
	AllResources []string
}

var (
	sessionID int
	rServerConn *rpc.Client
	myIpPort string
	myID int
	// TODO should not need this globally because have rServerConn whith which to talk to server..
	serverIpPort string
	peerList []PeerAddressAndStatus
	resourceList []string
)
// Main workhorse method.
func main() {
	// Parse the command line args, panic if error
	mode, physicalPeerId, peerIpPort, otherIpPort, err := ParseArguments()
	if err != nil {
		panic(err)
	}

	// Example illustrating JoinPrint and FinalPrint usage:
	//
	// var res Resources
	// for i := 0; i < 5; i++ {
	// 	res = append(res, Resource{Resource: fmt.Sprintf("%d", i), LogicalPeerID: rand.Int(), NumRemaining: 5 - i})
	// }
	// JoinPrint(physicalPeerId)
	// res.FinalPrint(physicalPeerId)

	// TODO
	// if -j call Join RPC to otherIpPort, will receive sessionID, serverIpPort, peerList, resourceList

	myIpPort = peerIpPort
	myID = physicalPeerId

	if mode == BOOTSTRAP {
		serverIpPort = otherIpPort
	}

	// Append PeerAddressAndStatus{myIpPort, true} to peerList
	// if bootstrapper, will be first in peerList
	peerList = append(peerList, PeerAddressAndStatus{myIpPort, true}) 


	// TODO Start listenRPC thread

	// start connection to Server for all peers and keep open
	raddr, err := net.ResolveTCPAddr("tcp", serverIpPort)
	checkError("Connecting to server: ", err, true)

	// Connection to the server
	serverConn, err := net.DialTCP("tcp", nil, raddr)
	checkError("Dialing the server: ", err, true)
	rServerConn = rpc.NewClient(serverConn)


	// if -b get session id
	if mode == BOOTSTRAP {
		in := Init{
			IPaddr:   "",
		}
		err = rServerConn.Call("RServer.InitSession", in, &sessionID)
		checkError("", err, false)
	}

	fmt.Println("The RServer.InitSession responded with sessionID: ", sessionID)

	fmt.Println("Bye bye !!   :)")
}





func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}

// Parses the command line arguments, two cases are valid
func ParseArguments() (mode int, physicalPeerId int, peerIpPort string, serverOrJoinerIpPort string, err error) {
	args := os.Args[1:]
	if len(args) != 4 {
		err = fmt.Errorf("Please supply 4 command line arguments as seen in the spec http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign3/index.html")
		return
	}
	switch args[0] {
	case "-b":
		mode = BOOTSTRAP
	case "-j":
		mode = JOIN
	default:
		err = fmt.Errorf("Valid modes are bootstrapping -b and joining -j not %s", args[0])
		return
	}
	physicalPeerId, err = strconv.Atoi(args[1])
	if err != nil {
		err = fmt.Errorf("unable to parse physical peer id (argument 2) please supply an integer Error:%s", err.Error())
	}
	peerIpPort = args[2]
	serverOrJoinerIpPort = args[3]
	return
}

/////////////////// Use functions below for state notifications (DO NOT MODIFY).

func JoinPrint(physicalPeerId int) {
	fmt.Printf("JOINED: using %d\n", physicalPeerId)
}

//
func (r Resource) String() string {
	return fmt.Sprintf("Resource: %s\tLogicalPeerID: %d\tNumRemaining: %d", r.Resource, r.LogicalPeerID, r.NumRemaining)
}

//
func (r Resources) String() (rString string) {
	for _, resource := range r {
		rString += resource.String() + "\n"
	}
	return
}

//
func (r Resources) FinalPrint(physicalPeerId int) {
	for _, resource := range r {
		fmt.Printf("ALLOC: %d %d %s\n", physicalPeerId, resource.LogicalPeerID, resource.Resource)
	}
}