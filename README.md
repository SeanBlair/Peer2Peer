A Peer to Peer Resource Distribution System
------
**System summary:** Please see http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign3/index.html for more details.
Some number of peers interact with each other to coordinate their interactions with a server in order to 
retrieve a set of resources (character strings). Consecutive server calls must be made by different peers, if there is only 
one peer alive, the system stalls until a new peer joins to satisfy this constraint. Once all the resources are retrieved, 
the last peer to call the server prints out all the strings and triggers all peers to terminate. The first peer to join
the system (bootstrapper) connects to the server, all subsequent peers connect to peers that must be alive. A peer can join 
the system any time after the bootstrapper joins and before all the resources are retrieved. Once joined, a peer can die
at any time.

Dependencies:
------
Developed and tested with Go version 1.7.4 linux/amd64 on a Linux ubuntu 14.04 LTS machine

Running instructions:
-------
Setup:
----
- Open a terminal and clone the repo: git clone https://github.com/SeanBlair/Peer2Peer.git
- Enter the folder: cd Peer2Peer
- Set executable permission: chmod +x server

System Demo
-------

**Step 0)** Run the server binary: ./server localhost:9999 211

Server arguments: [ip:port to listen for bootstrapper peer] [number of logical peers (minimum number of resources)]


**Step 1)** With the server already running, prepare 4 terminals with the following commands without yet running them:
- a) go run peer.go -b 1 localhost:1111 localhost:9999
- b) go run peer.go -j 2 localhost:2222 localhost:1111
- c) go run peer.go -j 3 localhost:3333 localhost:2222
- d) go run peer.go -j 4 localhost:4444 localhost:3333

**Step 2)** Run terminal a, note from the server terminal that only one resource is served as the system has only one peer.

**Step 3)** Run terminal b, note that the server starts rapidly serving all its resources.

**Step 4)** Run terminals c and d, note that the server continues to serve resources.

**Step 5) (Optional)** Kill (Ctrl + c) the processes on terminals a and b before the server finishes serving all the 
resources. 
Note that the last peer to interact with the server prints out all the retrieved resources and triggers all live peers
to terminate.


