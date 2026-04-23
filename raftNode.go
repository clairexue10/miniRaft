package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type RaftNode int

type VoteArguments struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type AppendEntryArgument struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index int
	Term  int
}

// global state variable
var currentTerm int
var votedFor int
var logEntries []LogEntry

// volatile state on all servers
var commitIndex int
var lastAppliedIndex int

// volatile state on leaders
var nextIndex []int
var matchIndex []int

// other global variables
var selfID int
var serverNodes []ServerConnection

var electionTimer *time.Timer
var electionTime time.Duration
var heartbeatTime time.Duration

var role Role
var failed bool
var mu sync.Mutex

func lastLogIndex() int {
	return len(logEntries)
}

func lastLogTerm() int {
	if len(logEntries) == 0 {
		return 0
	}
	return logEntries[len(logEntries)-1].Term
}

func getLogTerm(index int) int {
	if index <= 0 || index > len(logEntries) {
		return 0
	}
	return logEntries[index-1].Term
}

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	mu.Lock()
	defer mu.Unlock()

	fmt.Println("RequestVote is called by ", arguments.CandidateID)
	if failed {
		return fmt.Errorf("node %d is unavailable", selfID)
	}

	reply.Term = currentTerm

	//reject if term is old
	if arguments.Term < currentTerm {
		reply.ResultVote = false
		return nil
	}

	//step down to be follower if receive a vote request from some server with higher tem
	if arguments.Term > currentTerm {
		currentTerm = arguments.Term
		role = Follower
		votedFor = -1 //reset when term advances
	}

	//Checking the up-to-date-ness of the log at the requesting server
	candidateLogUpToDate := arguments.LastLogTerm > lastLogTerm() ||
		(arguments.LastLogTerm == lastLogTerm() && arguments.LastLogIndex >= lastLogIndex())

	if (votedFor == -1 || votedFor == arguments.CandidateID) && candidateLogUpToDate { //vote in a new term
		votedFor = arguments.CandidateID
		reply.ResultVote = true
		reply.Term = currentTerm
		if electionTimer != nil {
			electionTimer.Reset(electionTime)
		}
		fmt.Printf("Node %d voted for %d\n", selfID, arguments.CandidateID)
	} else {
		reply.ResultVote = false
	}

	return nil
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	mu.Lock()
	defer mu.Unlock()

	fmt.Println("AppendEntry is called by ", arguments.LeaderID)
	if failed {
		return fmt.Errorf("node %d is unavailable", selfID)
	}

	reply.Term = currentTerm

	//reply false if current term is higher
	if arguments.Term < currentTerm {
		reply.Success = false
		return nil
	}

	//step to be a follower if
	if arguments.Term > currentTerm {
		currentTerm = arguments.Term
		votedFor = -1
	}
	role = Follower
	if electionTimer != nil {
		electionTimer.Reset(electionTime)
	}

	fmt.Printf("PrevLogIndex=%d PrevLogTerm=%d myLogLen=%d\n", arguments.PrevLogIndex, arguments.PrevLogTerm, len(logEntries))

	//reply false if log does not have entry at prevLogIndex with matching term
	if arguments.PrevLogIndex > 0 {
		if arguments.PrevLogIndex > len(logEntries) ||
			getLogTerm(arguments.PrevLogIndex) != arguments.PrevLogTerm {
			reply.Success = false
			return nil
		}
	}

	for i, entry := range arguments.Entries {
		logIndex := arguments.PrevLogIndex + 1 + i
		if logIndex <= len(logEntries) {
			if getLogTerm(logIndex) != entry.Term {
				// Conflict, truncate from here and append the rest
				logEntries = logEntries[:logIndex-1]
				logEntries = append(logEntries, arguments.Entries[i:]...)
				break
			}
			// Entry already matches, continue
		} else {
			// Beyond end of our log, append remaining entries
			logEntries = append(logEntries, arguments.Entries[i:]...)
			break
		}
	}

	if arguments.LeaderCommit > commitIndex {
		lastNewEntry := arguments.PrevLogIndex + len(arguments.Entries)
		if arguments.LeaderCommit < lastNewEntry {
			commitIndex = arguments.LeaderCommit
		} else {
			commitIndex = lastNewEntry
		}
	}

	reply.Success = true
	fmt.Printf("Node %d received AppendEntry from leader %d, log: %v\n", selfID, arguments.LeaderID, logEntries)
	return nil
}

func applyEntries() {
	for {
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		for commitIndex > lastAppliedIndex {
			lastAppliedIndex++
			entry := logEntries[lastAppliedIndex-1]
			fmt.Printf("Node %d applying entry %d (term %d) to state machine\n", selfID, entry.Index, entry.Term)
		}
		mu.Unlock()
	}
}

// This function is designed to emulate a client reaching out to the
// server. Note that many of the realistic details are removed, for simplicity
func ClientAddToLog() {
	// In a realistic scenario, the client will find the leader node and communicate with it
	// In this implementation, we are pretending that the client reached out to the server somehow
	// But any new log entries will not be created unless the server node is a leader
	// isLeader here is a boolean to indicate whether the node is a leader or not
	//if role == Leader {
	// lastAppliedIndex here is an int variable that is needed by a node
	//to store the value of the last index it used in the log
	//lastAppliedIndex := logs.LastLogIndex + 1
	//entry := LogEntry{lastAppliedIndex, currentTerm}
	//log.Println("Client communication created the new log entry at index " + strconv.Itoa(entry.Index))
	// Add rest of logic here
	// HINT 1: using the AppendEntry RPC might happen here
	//}
	// HINT 2: force the thread to sleep for a good amount of time (less
	//than that of the leader election timer) and then repeat the actions above.
	//You may use an endless loop here or recursively call the function
	// HINT 3: you don’t need to add to the logic of creating new log
	//entries, just handle the replication
	for {
		time.Sleep(8 * time.Second)

		mu.Lock()

		//only continue if the node is a leader
		if role != Leader {
			mu.Unlock()
			continue
		}
		//append entry to local log
		newIndex := lastLogIndex() + 1
		entry := LogEntry{newIndex, currentTerm}
		logEntries = append(logEntries, entry)
		log.Printf("Client communication created the new log entry at index %d\n", newIndex)
		term := currentTerm
		mu.Unlock()

		//replicate to followers
		replicateToFollowers(term)
	}
}

func replicateToFollowers(term int) {
	for i, node := range serverNodes {
		go func(index int, server ServerConnection) {
			for {
				mu.Lock()
				if role != Leader || currentTerm != term {
					mu.Unlock()
					return
				}
				ni := nextIndex[index]
				prevLogIndex := ni - 1
				prevLogTerm := getLogTerm(prevLogIndex)
				entries := make([]LogEntry, len(logEntries[ni-1:]))
				copy(entries, logEntries[ni-1:])
				ci := commitIndex
				mu.Unlock()

				args := AppendEntryArgument{
					Term:         term,
					LeaderID:     selfID,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: ci,
				}
				var reply AppendEntryReply
				err := server.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
				if err != nil {
					fmt.Printf("Failed to replicate to node %d: %v\n", server.serverID, err)
					return
				}

				mu.Lock()
				//if response contains term T > currentTerm: convert to follower
				if reply.Term > currentTerm {
					currentTerm = reply.Term
					role = Follower
					votedFor = -1
					mu.Unlock()
					return
				}
				if reply.Success {
					//update nextIndex and matchIndex for follower
					matchIndex[index] = prevLogIndex + len(entries)
					nextIndex[index] = matchIndex[index] + 1

					//check if we can advance commitIndex
					//if there exists N > commitIndex, majority of matchIndex[i] >= N,
					//and log[N].term == currentTerm: set commitIndex = N
					for n := lastLogIndex(); n > commitIndex; n-- {
						if getLogTerm(n) == currentTerm {
							count := 1 // count self
							for _, mi := range matchIndex {
								if mi >= n {
									count++
								}
							}
							if count > len(serverNodes)/2 {
								commitIndex = n
								fmt.Printf("Leader advancing commitIndex to %d\n", n)
								break
							}
						}
					}
					mu.Unlock()
					fmt.Printf("Node %d accepted entries up to index %d\n", server.serverID, matchIndex[index])
					return
				} else {
					// decrement nextIndex and retry
					if nextIndex[index] > 1 {
						nextIndex[index]--
					}
					mu.Unlock()
				}
			}
		}(i, node)
	}
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	fmt.Println("LeaderElection is called.")
	var wg sync.WaitGroup

	mu.Lock()
	currentTerm += 1
	termForThisElection := currentTerm
	fmt.Println("Candidate's term is: ", currentTerm)
	votedFor = selfID
	if electionTimer != nil {
		electionTimer.Reset(electionTime)
	}
	lastIndex := lastLogIndex()
	lastTerm := lastLogTerm()
	mu.Unlock()

	totalVotes := 1

	for _, node := range serverNodes {
		wg.Add(1)
		go func(server ServerConnection) {
			defer wg.Done() //now defers until RPC actually completes

			voteArgs := VoteArguments{
				Term:         termForThisElection,
				CandidateID:  selfID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			var voteReply VoteReply

			done := make(chan error, 1)
			go func() {
				done <- server.rpcConnection.Call("RaftNode.RequestVote", voteArgs, &voteReply)
			}()

			select {
			case err := <-done:
				if err == nil {
					mu.Lock()
					//if response contains term T > currentTerm: convert to follower
					if voteReply.Term > currentTerm {
						currentTerm = voteReply.Term
						role = Follower
						votedFor = -1
						mu.Unlock()
						return
					}
					if voteReply.ResultVote {
						totalVotes++
					}
					mu.Unlock()
				}
			case <-time.After(5 * time.Second): // give up after 5s
				fmt.Println("Vote request to", server.serverID, "timed out")
			}
		}(node)
	}
	wg.Wait()

	mu.Lock()
	won := totalVotes > len(serverNodes)/2
	if won && role == Candidate && currentTerm == termForThisElection {
		role = Leader
		fmt.Println("Elected as leader by majority")
		//reinitialize nextIndex and matchIndex after election
		nextIndex = make([]int, len(serverNodes))
		matchIndex = make([]int, len(serverNodes))
		for i := range nextIndex {
			nextIndex[i] = lastLogIndex() + 1 // last log index + 1
			matchIndex[i] = 0
		}
	}
	mu.Unlock()

	if won {
		electionTimer.Stop()
		go Heartbeat(termForThisElection)
		//send initial empty AppendEntries immediately upon election
		replicateToFollowers(termForThisElection)
	} else {
		fmt.Println("Election failed")
	}
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat(termWhenElected int) {
	heartBeatTimer := time.NewTimer(heartbeatTime)
	defer heartBeatTimer.Stop()

	for {
		mu.Lock()
		shouldStop := role != Leader || failed || currentTerm != termWhenElected
		mu.Unlock()
		if shouldStop {
			return
		}

		<-heartBeatTimer.C

		mu.Lock()
		shouldStop = role != Leader || failed || currentTerm != termWhenElected
		mu.Unlock()
		if shouldStop {
			return //heartbeat should only be alive when the node is a leader
		}

		fmt.Println("Leader sending heartbeats")
		replicateToFollowers(termWhenElected)
		heartBeatTimer.Reset(heartbeatTime)
	}
}

func simulateFailure() {
	for {
		time.Sleep(time.Duration(60+rand.Intn(30)) * time.Second)
		fmt.Println("Node", selfID, "simulating failure.")
		mu.Lock()
		failed = true
		role = Follower
		mu.Unlock()

		time.Sleep(time.Duration(10+rand.Intn(20)) * time.Second)
		fmt.Println("Node", selfID, "rejoining cluster.")
		mu.Lock()
		failed = false
		mu.Unlock()
		if electionTimer != nil {
			electionTimer.Reset(electionTime)
		}
	}
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort := "localhost"

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			//index++
			//continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// following lines are to register the RPCs of this object of type RaftNode
	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Printf("serving rpc on port" + myPort)

	time.Sleep(30 * time.Second) //wait for other nodes to be started

	selfID = myID
	votedFor = -1
	commitIndex = 0
	lastAppliedIndex = 0

	for index, element := range lines {
		if index == selfID {
			continue
		}

		// Attemp to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	//simulate failure (this is ran in goroutine so the sleep func within it should not interfere
	//with other operations)
	go simulateFailure()

	//simulate log appends
	go ClientAddToLog()
	go applyEntries()

	//create time and timer variables
	electionTime = time.Duration(15+rand.Intn(10)) * time.Second
	electionTimer = time.NewTimer(electionTime)
	heartbeatTime = 5 * time.Second

	role = Follower

	for {
		select {
		case <-electionTimer.C:
			mu.Lock()
			isFailed := failed
			mu.Unlock()
			if isFailed {
				electionTimer.Reset(electionTime)
				continue
			}
			fmt.Println("Follower notices election timeout.")
			fmt.Println("Candidate self-declared.")
			mu.Lock()
			role = Candidate
			mu.Unlock()
			LeaderElection()
			electionTimer.Reset(electionTime)
		}
	}

}
