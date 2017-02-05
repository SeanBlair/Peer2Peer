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
	"math/rand"
	// TODO
	"net/rpc"
	"net"
	"log"
	"time"
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

type Peer int


type JoinRequest struct {
	MyAddress string
}

type JoinResponse struct {
	SessID int
	RServerAddress string
	// all except myIpPort
	AllPeers []PeerAddressAndStatus
	AllResources Resources
}

type AddPeerRequest struct {
	PeerAddress string
}

type AddResourceRequest struct {
	TheResource Resource
}


var (
	sessionID int
	myIpPort string
	myID int
	serverIpPort string
	peerList []PeerAddressAndStatus
	resourceList Resources
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

	myIpPort = peerIpPort
	myID = physicalPeerId

	done := make(chan int)

		// Set up RPC so peers can talk to each other
	go func() {
		pServer := rpc.NewServer()
		p := new(Peer)
		pServer.Register(p)

		l, err := net.Listen("tcp", myIpPort)
		checkError("", err, true)

		for {
			conn, err := l.Accept()
			checkError("", err, false)
			go pServer.ServeConn(conn)
		}
	}()	


	// if -b get session id
	if mode == BOOTSTRAP {
		serverIpPort = otherIpPort
		peerList = append(peerList, PeerAddressAndStatus{myIpPort, true})

		raddr, err := net.ResolveTCPAddr("tcp", serverIpPort)
		checkError("Connecting to server: ", err, true)

		// Connection to the server
		serverConn, err := net.DialTCP("tcp", nil, raddr)
		checkError("Dialing the server: ", err, true)
		rServerConn := rpc.NewClient(serverConn)

		in := Init{
			IPaddr:   "",
		}
		err = rServerConn.Call("RServer.InitSession", in, &sessionID)
		checkError("", err, false)

		// to allow other peers on same machine to connect at different times to RServer
		// Having a connection with RServer seems to be required to be synchronously
		rServerConn.Close()

		// fmt.Println("The RServer.InitSession responded with sessionID: ", sessionID)

		JoinPrint(physicalPeerId)

		//TODO get resource, manage, delegate if exists peer, else wait till peer exists.

		go getResource()

	// Joining peer
	} else {

		var joinResp JoinResponse
		joinReq := JoinRequest{myIpPort}

		client, err := rpc.Dial("tcp", otherIpPort)
		checkError("rpc.Dial in Joining", err, false)

		err = client.Call("Peer.Join", joinReq, &joinResp)
		checkError("client.Call(Peer.Join: ", err, false)

		err = client.Close()
		checkError("client.Close() in Join call: ", err, false)

		sessionID = joinResp.SessID
		serverIpPort = joinResp.RServerAddress
		peerList = joinResp.AllPeers
		peerList = append(peerList, PeerAddressAndStatus{myIpPort, true})
		resourceList = joinResp.AllResources

		// fmt.Println("successfully called Peer.Join to: ", otherIpPort)
		// fmt.Println("After Join, my sessionID: ", sessionID, " my serverIpPort: ", serverIpPort)
		// fmt.Println(" my peerList: ", peerList, " my resourceList: ")
		// resourceList.FinalPrint(myID)

		JoinPrint(physicalPeerId)
	}

	go ping()

	// blocks while threads exist alive
	<-done
}


// For determining if peer is alive. If dead caller's rpc.Dial will gracefully fail
func (p *Peer) Ping(PeerId int, reply *bool) error {
	*reply = true
	return nil
}

// Call for peer to terminate
func (p *Peer) Exit(PeerId int, reply *bool) error {	
	os.Exit(0)
	*reply = true
	return nil
}

// Called when a peer wants to join the system, shares new peer with live peers in peerList
// and returns to joining peer: sessionID, serverIpPort, peerList (without joining peer) and
// resourceList
func (p *Peer) Join(JReq JoinRequest, JResp *JoinResponse) error {
	// TODO have to lock stuff???  Probably.
	*JResp = JoinResponse{sessionID, serverIpPort, peerList, resourceList}
	peerList = append(peerList, PeerAddressAndStatus{JReq.MyAddress, true})
	// fmt.Println("After updating peerList when receive Peer.Join rpc, peerList: ", peerList)
	
	go broadcastNewPeer(JReq.MyAddress)
	return nil
}

// Called when a peer gets joined by a peer. Used to update all other peer's peerList
func (p *Peer) AddPeer(AddPeerReq AddPeerRequest, reply *bool) error {
	peerList = append(peerList, PeerAddressAndStatus{AddPeerReq.PeerAddress, true})
	// fmt.Println("Received a Peer.AddPeer call and my peerList now contains: ", peerList)
	*reply = true
	return nil
}

// 
func (p *Peer) AddResource(AddResourceReq AddResourceRequest, reply *bool) error {
	resourceList = append(resourceList, AddResourceReq.TheResource)
	// fmt.Println("Received a Peer.AddResource call and my resourceList now contains: ", resourceList)
	*reply = true
	return nil
}


func (p *Peer) GetNextResource(PeerId int, reply *bool) error {
	fmt.Println("Received a call to Peer.GetNextResource from: ", PeerId)
	// Not serving on new thread to ensure the subsequent call to the RServer returns
	// that is only moment peer is guaranteed to be alive for 3 seconds.
	getResource()
	*reply = true
	return nil
}


// Calls RServer.GetResource and manages returned Resource
func getResource() {

	// for testing purposes, will wait random period between 1 and 10 seconds to allow
	// playing around with peer joins and failures before all resources are retrieved.
	// TODO eliminate/comment out
	rand.Seed(time.Now().Unix())
	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)


	raddr, err := net.ResolveTCPAddr("tcp", serverIpPort)
	checkError("net.ResolveTCPAddr in getResource(): ", err, false)

	// Connection to the server
	serverConn, err := net.DialTCP("tcp", nil, raddr)
	checkError("net.DialTCP in getResource(): ", err, false)
	rServerConn := rpc.NewClient(serverConn)

	getResourceReq := ResourceRequest{sessionID, ""}
	var resource Resource 

	err = rServerConn.Call("RServer.GetResource", getResourceReq, &resource)
	checkError("rServerConn.Call(RServer.GetResource): ", err, false)

	rServerConn.Close()

	// Release caller of Peer.GetNextResource by spawning new thread, because 
	// this peer is guaranteed to not fail for 3 seconds.
	go manageResource(resource)
}

// Shares resource with all peers, if numRemaning > 0, delegates next GetResource,
// else tell all peers to exit, calls FinalPrint and exits.
func manageResource(resource Resource) {
	shareResource(resource)

	if resource.NumRemaining > 0 {
		delegateGetResource()
		fmt.Println("Finished manageResource() call, can die ==========================")
	} else {
		resourceList.FinalPrint(myID)
		exitAllPeers()
	}
}

// Shares given resource with all alive peers
func shareResource(resource Resource) {
	// add to own resource list
	resourceList = append(resourceList, resource)
	for _, peer := range peerList {
		if peer.Status && peer.Address != myIpPort {
			addResource(peer.Address, resource)
		}
	}
}

// Calls Peer.AddResource with given resource, to given peerAddress
func addResource(peerAddress string, resource Resource) {
	var reply bool
	addResourceArg := AddResourceRequest{resource}
	client, err := rpc.Dial("tcp", peerAddress)
	// Dead peer, ignore
	if err != nil {
		return
	}
	err = client.Call("Peer.AddResource", addResourceArg, &reply)
	checkError("Peer.AddResource in addResource(): ", err, false)

	err = client.Close()
	checkError("client.Close() in addResource(): ", err, false)
}

// Calls next alive peer in peerList, if none exist, waits
func delegateGetResource() {
	for {
		peerAddress := getNextPeer()
		if peerAddress != "" {
			err := getNextResource(peerAddress)
			// if returns error, should call next peer in peerList.
			if err != nil {
				continue
			}
			// Happy path: getNextResource() call succeeded at least til point
			// when other peer received Resource from RServer
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

// Returns error if either rpc.Dial or client.Call returns error,
// which probably means peer is dead and the ping thread has not
// yet updated the peers status in the peerList
func getNextResource(peerAddress string) error {
	var reply bool
	client, err := rpc.Dial("tcp", peerAddress)
	// Dead peer
	if err != nil {
		// TODO need to test, maybe put typo for hardcoded peer in client.Call() code.
		return err
	}
	err = client.Call("Peer.GetNextResource", myID, &reply)

	// TODO: maybe implement a timeout here? for if peer is taking too long, yet 
	// not thrown error yet. Would return an error and let caller retry...

	// Maybe peer dead, but ping not yet updated peer status in peerList
	if err != nil {
		return err
	}

	err = client.Close()
	checkError("client.Close() in getNextResource(): ", err, false)
	return nil
} 

// returns first peer in peerList that is alive and not me,
// if none, returns ""
func getNextPeer() string {
	for _, peer := range peerList {
		if peer.Status && peer.Address != myIpPort {
			return peer.Address
		}
	}
	fmt.Println("No other peer at this point!!!")
	return ""
}

func exitAllPeers() {
	for _, peer := range peerList {
		if peer.Status && peer.Address != myIpPort {
			exit(peer.Address)
		}
	}
	os.Exit(0)
}

func exit(peerAddress string) {
	var reply bool
	client, err := rpc.Dial("tcp", peerAddress)
	// Dead peer, ignore
	if err != nil {
		return
	}
	err = client.Call("Peer.Exit", myID, &reply)
	// ignore error (unexpected EOF)....
	// checkError("Peer.Exit in exit(): ", err, false)

	err = client.Close()
	checkError("client.Close() in exit(): ", err, false)
}

// Pings all peers currently alive, if peer dead, updates peerList
func ping() {
	// TODO need to lock??
	for {
		for i, peer := range peerList {
			if shouldPing(peer) {
				// TODO make multithreaded
				pingPeer(peer.Address, i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Calls peer and if dead, sets its status to false in peerList
// TODO lock peerList??
func pingPeer(peerAddress string, peerListIndex int) {
	var reply bool
	client, err := rpc.Dial("tcp", peerAddress)
	// Dead peer
	if err != nil {
		markDeadPeer(peerListIndex)
		// fmt.Println("peerList after updating dead peer:", peerAddress, " is: ", peerList)
		return
	}
	err = client.Call("Peer.Ping", myID, &reply)
	checkError("Peer.Ping in pingPeer: ", err, false)

	err = client.Close()
	checkError("client.Close() in pingPeer: ", err, false)
}


// Sets peer at given index in peerList to Status = false
func markDeadPeer(peerListIndex int) {
	// TODO lock??
	peerList[peerListIndex].Status = false
}

// Returns true if peer is alive and not myIpPort
func shouldPing(peer PeerAddressAndStatus) bool {
	return peer.Status && peer.Address != myIpPort
}

// Shares new peer address to all peers except myself and joining peer. (All peers unaware of join)
func broadcastNewPeer(joiningPeer string) {
	// needs to concurrently add to all peers. Needs to support nonexistent peers, not yet updated in peerList
	// TODO need to block peerList?? probably...
	for _, peerAddr := range peerList {
		// do not call joiningPeer or myIpPort
		if shouldBroadcastPeerList(peerAddr, joiningPeer) {
			go sendNewPeer(peerAddr.Address, joiningPeer)
		}
	}
}

// Returns true if peer in peerList (peerAddrNStatus), is not dead and neither me, nor new joining peer
func shouldBroadcastPeerList(peerAddrNStatus PeerAddressAndStatus, joiningPeer string) bool {
	peerAddress := peerAddrNStatus.Address
	return peerAddrNStatus.Status && peerAddress != joiningPeer && peerAddress != myIpPort 
}

// Calls Peer.AddPeer with arg newPeer to peerAddress
func sendNewPeer(peerAddress string, newPeer string) {
	var reply bool
	req := AddPeerRequest{newPeer}
	client, err := rpc.Dial("tcp", peerAddress)
	// Dead peer, ignore
	if err != nil {
		return
	}
	err = client.Call("Peer.AddPeer", req, &reply)
	checkError("Peer.AddPeer in sendNewPeer: ", err, false)

	err = client.Close()
	checkError("client.Close() in sendNewPeer: ", err, false)
}

//
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
