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
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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

// The address and status of a peer that has joined the system
type PeerAddressAndStatus struct {
	Address string
	Status  bool
}

// Peer server type
type Peer int

// Request that a peer sends when joining the system
type JoinRequest struct {
	MyAddress string
}

// Response from call to Peer.Join
type JoinResponse struct {
	SessID         int
	RServerAddress string
	AllPeers       []PeerAddressAndStatus
	AllResources   Resources
}

// Request to communicate new peer in system
type AddPeerRequest struct {
	PeerAddress string
}

// Request to add resource that other peer retrieved from RServer
type AddResourceRequest struct {
	TheResource Resource
}

// Request sent when pinging peer, used to identify missing
// peers in peerList due to a race condition scenario.
type PingRequest struct {
	PeerList []PeerAddressAndStatus
}

// Response to Peer.ShareResourceList rpc
// Used to verify that the resourceList printed is the
// longest (most complete). For the error case when the peer
// that received numRemaining == 0 did not receive all Resources
type ShareResourcesResponse struct {
	ResourceList Resources
}

var (
	// Returned from call to RServer.InitSession
	sessionID int
	// Address at which peer listens for RPC's
	myIpPort string
	// Peer's physical id
	myID int
	// Address at which RServer is listening
	serverIpPort string
	// Array of peer + status that have joined the system
	peerList []PeerAddressAndStatus
	// Resources retrieved so far from RServer
	resourceList Resources
)

// Program entry point and main workhorse method.
func main() {
	// Parse the command line args, panic if error
	mode, physicalPeerId, peerIpPort, otherIpPort, err := ParseArguments()
	if err != nil {
		panic(err)
	}
	myIpPort = peerIpPort
	myID = physicalPeerId
	// for managing threads
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
		checkError("Connecting to server: ", err, false)
		// Connection to the server
		serverConn, err := net.DialTCP("tcp", nil, raddr)
		checkError("Dialing the server: ", err, false)
		rServerConn := rpc.NewClient(serverConn)
		in := Init{
			IPaddr: "",
		}
		err = rServerConn.Call("RServer.InitSession", in, &sessionID)
		checkError("rServerConn.Call(RServer.InitSession fail: ", err, false)
		rServerConn.Close()

		JoinPrint(physicalPeerId)
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

		JoinPrint(physicalPeerId)
	}

	go ping()
	// blocks while threads exist alive
	<-done
}

// For determining if peer is alive. If dead, caller's rpc will gracefully fail,
// triggering the caller to update the status of this peer in peerList to false
// Also for determing if calling peer knows about more peers than callee,
// if so, callee overwrites its peerList with calling peers larger list.
func (p *Peer) Ping(PingReq PingRequest, reply *bool) error {
	if len(PingReq.PeerList) > len(peerList) {
		fmt.Println("Found a discrepancy in peerLists, mine was shorter, fixed it")
		peerList = PingReq.PeerList
	}
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
	*JResp = JoinResponse{sessionID, serverIpPort, peerList, resourceList}
	peerList = append(peerList, PeerAddressAndStatus{JReq.MyAddress, true})
	// Returns once all existing peers have been sent requests to update their peerLists
	// with new peer. If any of these requests fail, destination peer is assumed dead.
	broadcastNewPeer(JReq.MyAddress)
	return nil
}

// Called when a peer gets joined by a peer. Used to update all other peer's peerList
func (p *Peer) AddPeer(AddPeerReq AddPeerRequest, reply *bool) error {
	peerList = append(peerList, PeerAddressAndStatus{AddPeerReq.PeerAddress, true})
	*reply = true
	return nil
}

// Provides the latest resource from RServer to ensure all peers have all resources
func (p *Peer) AddResource(AddResourceReq AddResourceRequest, reply *bool) error {
	resourceList = append(resourceList, AddResourceReq.TheResource)
	*reply = true
	return nil
}

// Tells this peer to be the one currently interacting with RServer
// Manages returned resource, sharing it with all peers and calls another
// peer if more resources. Otherwise, prints the resources and closes all peers.
func (p *Peer) GetNextResource(PeerId int, reply *bool) error {
	// In order to guarantee the success of this call, not serving on new thread
	// to ensure the subsequent call to RServer returns. That moment, peer is
	// guaranteed to be alive for 3 seconds.
	getResource()
	*reply = true
	return nil
}

func (p *Peer) ShareResourceList(PeerId int, ShareResResp *ShareResourcesResponse) error {
	ShareResResp.ResourceList = resourceList
	return nil
}

// Calls RServer.GetResource and manages returned Resource
func getResource() {
	// Stall to allow all previous communications to finish, helps avoid deadlocks
	time.Sleep(100 * time.Millisecond)

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
	} else {
		// finds longest resourceList among peers
		findLongestResourceList()

		// checker that returns true if all resourceCounts
		// are consecutive. for debugging.
		// TODO eliminate this code...???
		if allResourcesConsecutive() {
			fmt.Println("There are ", len(resourceList), " resources and they are all consecutive! :) ")
		} else {
			fmt.Println("!!!Non-consecutive resources.... !!!!!!")
		}

		resourceList.FinalPrint(myID)
		exitAllPeers()
	}
}

// Checks if any peer has a longer resource List, if so,
// replaces resourceList with the longer one.
func findLongestResourceList() {
	for _, peer := range peerList {
		if peer.Status && peer.Address != myIpPort {
			TheirResourceList, err := getResourceList(peer.Address)
			// Dead peer
			if err != nil {
				continue
			}
			if len(TheirResourceList) > len(resourceList) {
				resourceList = TheirResourceList
			}
		}
	}
}

// Gets the resourceList of given peerAddress, any error is returned and
// interpreted as a dead peer.
func getResourceList(peerAddress string) (theirResources Resources, err error) {
	var shareResourceList ShareResourcesResponse
	client, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return
	}
	err = client.Call("Peer.ShareResourceList", myID, &shareResourceList)
	if err != nil {
		return
	}
	err = client.Close()
	if err != nil {
		return
	}
	return shareResourceList.ResourceList, nil
}

// Returns true if no missing resources, for debugging only
// TODO eliminate
func allResourcesConsecutive() bool {
	for i, resource := range resourceList {
		resourceSlice := strings.Split(resource.Resource, " ")
		count, _ := strconv.Atoi(resourceSlice[1])
		if (count - 1) != i {
			fmt.Println("the value is: ", count, " when ", i+1, " was expected!!!")
			return false
		}
	}
	return true
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
// Any error is interpreted as a dead peer and ignored
func addResource(peerAddress string, resource Resource) {
	var reply bool
	addResourceArg := AddResourceRequest{resource}
	client, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return
	}
	err = client.Call("Peer.AddResource", addResourceArg, &reply)
	if err != nil {
		return
	}
	err = client.Close()
	if err != nil {
		return
	}
}

// Calls next alive peer in peerList, if none exist, waits 1 second and retries
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
			// no peer available, wait 1 second to retry
			time.Sleep(1 * time.Second)
		}
	}
}

// Returns error if found, interpreted as peer is dead and
// the ping thread has not yet updated the status in the peerList
func getNextResource(peerAddress string) error {
	var reply bool
	client, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return err
	}
	err = client.Call("Peer.GetNextResource", myID, &reply)
	if err != nil {
		return err
	}
	err = client.Close()
	if err != nil {
		return err
	}
	return nil
}

// Returns first peer in peerList that is alive and not me,
// if none, returns ""
func getNextPeer() string {
	for _, peer := range peerList {
		if peer.Status && peer.Address != myIpPort {
			return peer.Address
		}
	}
	return ""
}

// Tells all peers in system to exit, then exits program
func exitAllPeers() {
	for _, peer := range peerList {
		if peer.Status && peer.Address != myIpPort {
			exit(peer.Address)
		}
	}
	os.Exit(0)
}

// Call Peer.Exit rpc to given peerAddress
// Any errors interpreted as dead peer and returns
func exit(peerAddress string) {
	var reply bool
	client, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return
	}
	err = client.Call("Peer.Exit", myID, &reply)
	if err != nil {
		return
	}
	err = client.Close()
	if err != nil {
		return
	}
}

// Pings all peers currently alive, if peer dead, updates peerList
// repeats every 100 milliseconds
func ping() {
	for {
		for i, peer := range peerList {
			if shouldPing(peer) {
				go pingPeer(peer.Address, i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Calls peer, any error interpreted as dead and sets its status to false in peerList
func pingPeer(peerAddress string, peerListIndex int) {
	var reply bool
	pingReq := PingRequest{peerList}
	client, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		markDeadPeer(peerListIndex)
		return
	}
	err = client.Call("Peer.Ping", pingReq, &reply)
	if err != nil {
		markDeadPeer(peerListIndex)
		return
	}
	err = client.Close()
	if err != nil {
		markDeadPeer(peerListIndex)
		return
	}
}

// Sets peer at given index in peerList to peer.Status = false
func markDeadPeer(peerListIndex int) {
	peerList[peerListIndex].Status = false
}

// Returns true if peer is alive and not myIpPort
func shouldPing(peer PeerAddressAndStatus) bool {
	return peer.Status && peer.Address != myIpPort
}

// Shares new peer address to all peers except myself and joining peer. (All peers unaware of join)
func broadcastNewPeer(joiningPeer string) {
	for _, peerAddr := range peerList {
		// do not call joiningPeer or myIpPort
		if shouldBroadcastPeer(peerAddr, joiningPeer) {
			go sendNewPeer(peerAddr.Address, joiningPeer)
		}
	}
}

// Returns true if peer in peerList (peerAddrNStatus), is not dead and neither me, nor new joiningPeer
func shouldBroadcastPeer(peerAddrNStatus PeerAddressAndStatus, joiningPeer string) bool {
	peerAddress := peerAddrNStatus.Address
	return peerAddrNStatus.Status && peerAddress != joiningPeer && peerAddress != myIpPort
}

// Calls Peer.AddPeer with arg newPeer to peerAddress
// If errors, return because peer is assumed dead.
func sendNewPeer(peerAddress string, newPeer string) {
	var reply bool
	req := AddPeerRequest{newPeer}
	client, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return
	}
	err = client.Call("Peer.AddPeer", req, &reply)
	if err != nil {
		return
	}
	err = client.Close()
	if err != nil {
		return
	}
}

// Prints msg + err to console and exits program if exit == true
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
