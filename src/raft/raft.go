package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	currentState     int
	commitIndex      int
	lastApplied      int
	leaderId         int
	nextIndex        []int
	matchIndex       []int
	electionTimeout  time.Duration
	electionTimer    *time.Ticker
	heartBeatTimeout time.Duration
	heartBeatTimer   *time.Ticker

	logEntryPublishChanSize int
	logEntryPublishChan     chan LogEntry

	userApplyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.currentTerm), rf.currentState == LEADER
}

func (rf *Raft) PrintState() {
	log.Printf("server %d: current term: %d, current state %d, apply idx: %d,  logs %v", rf.me, rf.currentTerm, rf.currentState, rf.lastApplied, rf.log)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// Assumes that the caller holds mutex and state is consistent
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(len(rf.log))
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.currentState = FOLLOWER
	if data == nil || len(data) < 1 {
		log.Printf("Restoring from empty state")
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var logLength int

	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil || d.Decode(&logLength) != nil {
		log.Fatal("Failed to restore state")
	} else {
		rf.log = make([]LogEntry, logLength)
		if d.Decode(&rf.log) != nil {
			log.Fatal("Failed to restore state, could not decode log")
		}
		log.Printf("restored state: currentTerm %d, votedFor %d, logs: %d", rf.currentTerm, rf.votedFor, rf.log)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func getRandomTimeout(timeout time.Duration) time.Duration {
	return timeout + time.Duration(rand.Float64()*float64(timeout))
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		reply.VoteGranted = false
		return
	}
	log.Printf("Server %d received vote request: %v", rf.me, args)
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf("Server %d voted no for %d in term %d due to my term > his, reply: %v", rf.me, args.CandidateId, rf.currentTerm, reply)
		return
	}
	if args.Term > rf.currentTerm || rf.votedFor == -1 {
		var myLastLogIndex int
		var myLastLogTerm int
		if len(rf.log) == 0 {
			myLastLogIndex = -1
			myLastLogTerm = -1
		} else {
			myLastLogTerm = rf.log[len(rf.log)-1].Term
			myLastLogIndex = rf.log[len(rf.log)-1].Index
		}
		// I am ahead of candidate
		if myLastLogTerm > args.LastLogTerm ||
			(myLastLogTerm == args.LastLogTerm && myLastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			reply.Term = args.Term
			log.Printf("Server %d voted no for %d in term %d due to me more updated than him, reply: %v", rf.me, args.CandidateId, rf.currentTerm, reply)
			return
		}
		// Candidate is as uptodate as me
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.currentState = FOLLOWER
		rf.persist()
		rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))
		log.Printf("Server %d voted for %d in term %d, reply: %v", rf.me, rf.votedFor, rf.currentTerm, reply)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Success = false
		reply.Term = args.Term
		return
	}

	log.Printf("server %d recieved appendEntries request: %v", rf.me, *args)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		log.Printf("server %d: append entry %v failed, in my term: %d", rf.me, args, rf.currentTerm)
		return
	}
	reply.Term = args.Term
	reply.Success = false
	rf.currentState = FOLLOWER
	rf.currentTerm = args.Term
	rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))

	rf.leaderId = args.LeaderId

	reply.Success = false

	if len(args.Entries) == 0 {
		reply.Success = true
	}

	if args.PrevLogIndex == -1 {
		reply.Success = true
	} else if len(rf.log) >= args.PrevLogIndex+1 &&
		rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.Success = true
	} else {
		if reply.Success == true { // Request was heartbeat, but my last index didn't match so I can't apply
			log.Printf("server %d : append entries(hb) req %v success\n but last index not match,  my term is %d", rf.me, args, rf.currentTerm)
		} else {
			// request was not a heartbeat and last index didnt match
			log.Printf("server %d : append entries req %v failed\n due to last index not match,  my term is %d", rf.me, args, rf.currentTerm)
		}
		log.Printf("server %d: sending append entry reply: %v", rf.me, reply)
		return
	}

	// My log matches with leader at the sent index.

	// Append / Update Entries
	// Assuming leader has sent us all entries he has from his prev_index in continuous order

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.commitIndex = args.LeaderCommitIndex
	if len(args.Entries) != 0 {
		log.Printf("server %d appended entires: %v, curr log: %v", rf.me, args.Entries, rf.log)
	}
	for rf.lastApplied < rf.commitIndex {
		idx := rf.lastApplied + 1
		log.Printf("server %d applied entry: %v", rf.me, rf.log[idx])
		rf.userApplyChan <- ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[idx].Command,
			CommandIndex:  rf.log[idx].Index + 1,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.lastApplied++
	}
	log.Printf("server %d: sending append entry reply: %v", rf.me, reply)

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(voteChan chan bool, server int, args *RequestVoteArgs) bool {
	log.Printf("Sending voteRequest %v to server %d", *args, server)
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok || !reply.VoteGranted {
		voteChan <- false
		if ok && reply.Term > args.Term {
			rf.mu.Lock()
			if rf.currentTerm > reply.Term || rf.currentState == FOLLOWER {
				// ignore
			} else {
				rf.currentState = FOLLOWER
				rf.currentTerm = reply.Term
				rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))
			}
			rf.mu.Unlock()
		}
	} else {
		voteChan <- true
	}
	return ok
}

type AppendEntriesRequest struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	LeaderCommitIndex int
	Entries           []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Used only for sending heartbeats for now
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest) bool {
	log.Printf("Sending appendEntries %v to server %d", *args, server)
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if ok == true {
		if reply.Success == false {
			// Someone is more up to date than me
			// Become follower
			log.Printf("server %d received negative append entry reply %v from %d in term %d", rf.me, reply, server, rf.currentTerm)
			rf.mu.Lock()
			if rf.currentState != LEADER || rf.currentTerm > args.Term {
				// ignore
			} else {
				rf.currentTerm = reply.Term
				rf.currentState = FOLLOWER
				rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))
			}
			rf.mu.Unlock()
		}
	}
	return ok
}

func (rf *Raft) heartBeatSender() {
	if rf.killed() {
		return
	}
	for {
		<-rf.heartBeatTimer.C
		rf.mu.Lock()
		if rf.currentState != LEADER {
			rf.heartBeatTimer.Stop()
			rf.mu.Unlock()
			continue
		}
		log.Printf("Heartbeat timeout reset at server %d", rf.me)

		prevLogIndex := -1
		prevLogTerm := -1
		if len(rf.log) > 0 {
			prevLogIndex = rf.log[len(rf.log)-1].Index
			prevLogTerm = rf.log[len(rf.log)-1].Term
		}
		heartBeatRequest := AppendEntriesRequest{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      prevLogIndex,
			PrevLogTerm:       prevLogTerm,
			LeaderCommitIndex: rf.commitIndex,
			Entries:           []LogEntry{},
		}
		rf.mu.Unlock()

		for idx, _ := range rf.peers {
			if idx != rf.me {
				go rf.sendAppendEntries(idx, &heartBeatRequest)
			}
		}

	}
}

// will be called as a goroutine
// TODO garbage collect these goroutines using an exit channel
func (rf *Raft) handleAppendEntry(successChan chan bool, server int, entry LogEntry) {

	for rf.matchIndex[server] < entry.Index {
		// Read a consistent state
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		currentState := rf.currentState
		currCommitIndex := rf.commitIndex

		if currentTerm != entry.Term || currentState != LEADER {
			successChan <- false
			return
		}

		prevLogTerm := -1
		prevLogIndex := -1
		lastMatchIndex := rf.matchIndex[server]
		if lastMatchIndex < -1 {
			log.Fatalf("WTF DOG @ server %d, last match index of %d < -1", rf.me, server)
		}
		if lastMatchIndex >= 0 {
			prevLogTerm = rf.log[lastMatchIndex].Term
			prevLogIndex = rf.log[lastMatchIndex].Index
		}

		request := AppendEntriesRequest{
			Term:              entry.Term,
			LeaderId:          rf.me,
			PrevLogIndex:      prevLogIndex,
			PrevLogTerm:       prevLogTerm,
			LeaderCommitIndex: currCommitIndex,
			Entries:           rf.log[lastMatchIndex+1 : entry.Index+1],
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}

		log.Printf("Server %d : Sending appendEntries %v to server %d", rf.me, request, server)
		ok := rf.peers[server].Call("Raft.AppendEntries", &request, &reply)

		if !ok {
			time.Sleep(time.Second * 2) // Server Unreachable, Probably Down, Sleep!
		}
		log.Printf("Server %d : recevied appendEntriesReply %v from server %d", rf.me, reply, server)

		if !reply.Success {
			if reply.Term > currentTerm {
				// Stop
				rf.mu.Lock()
				if rf.currentState != FOLLOWER || rf.currentTerm > reply.Term {
					// No need to do anything
				} else {
					// revert to follower
					log.Printf("WTF DOG")
					rf.currentState = FOLLOWER
					rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))
				}
				rf.mu.Unlock()
				successChan <- false
				return
			}
			// reply.Term == currentTerm == request.Term
			rf.matchIndex[server]--
			if rf.matchIndex[server] < -1 {
				rf.matchIndex[server] = -1
			}

		} else {
			// append entries success
			rf.matchIndex[server] = entry.Index
			successChan <- true
		}
	}

}

func (rf *Raft) logEntryPublisher() {

	for {
		logEntry := <-rf.logEntryPublishChan
		rf.mu.Lock()
		currState := rf.currentState
		currTerm := rf.currentTerm
		currCommitIdx := rf.commitIndex
		rf.mu.Unlock()
		if currState != LEADER {
			continue
		}
		if currTerm != logEntry.Term {
			// Do not ***directly*** publish entries from previous terms
			log.Printf("Ignoring log entry: %v from previous term in our log", logEntry)
			continue
		}
		Assert(currCommitIdx < logEntry.Index,
			"stopping due to bug : about to publish log entry: %v, whose index is less than commit index: %d",
			logEntry, currCommitIdx)

		clusterLen := len(rf.peers)
		requiredSucess := clusterLen/2 + 1
		gotSucess := 1
		successChan := make(chan bool, clusterLen)

		for idx, _ := range rf.peers {
			if idx != rf.me {
				go rf.handleAppendEntry(successChan, idx, logEntry)
			}
		}

		for i := 0; i < clusterLen-1; i++ {
			success := <-successChan
			if success == true {
				gotSucess++
			}
			if gotSucess == requiredSucess {
				break
			}
		}

		if gotSucess == requiredSucess {
			rf.mu.Lock()
			if rf.currentState != LEADER || rf.currentTerm != logEntry.Term {
				// do not commit this entry
				rf.mu.Unlock()
				continue
			}
			// entry committed
			rf.commitIndex = logEntry.Index
			log.Printf("server %d commited entry: %v", rf.me, logEntry)
			// apply
			for rf.lastApplied < rf.commitIndex {
				idx := rf.lastApplied + 1
				log.Printf("server %d applied entry: %v", rf.me, rf.log[idx])
				rf.userApplyChan <- ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[idx].Command,
					CommandIndex:  rf.log[idx].Index + 1,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				rf.lastApplied++
			}

			rf.mu.Unlock()
		}

	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		return -1, -1, false
	}

	log.Printf("server %d received req: %v", rf.me, command)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.currentState == LEADER

	if !isLeader {
		return -1, -1, isLeader
	}

	index := len(rf.log)
	term := rf.currentTerm

	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}

	rf.log = append(rf.log, entry)

	go func() { // Do not block the return
		rf.logEntryPublishChan <- entry
	}()

	return index + 1, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Assumes caller does not hold lock
func (rf *Raft) convertToLeader() {
	if rf.currentState != CANDIDATE {
		panic("Invalid state while converting to leader")
	}
	log.Printf("Server %d became leader", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Stop()
	rf.currentState = LEADER
	rf.heartBeatTimer.Reset(rf.heartBeatTimeout)

	// TODO: send last log entry? // No

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		<-rf.electionTimer.C
		log.Printf("Election timeout occured at server %d in term: %d", rf.me, rf.currentTerm)
		rf.mu.Lock()
		if rf.currentState == LEADER {
			rf.electionTimer.Stop()
			rf.mu.Unlock()
			continue
		}
		rf.currentTerm++
		rf.currentState = CANDIDATE
		rf.votedFor = rf.me
		lastLogIndex := -1
		lastLogTerm := -1
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
			lastLogIndex = rf.log[len(rf.log)-1].Index
		}

		request := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogTerm:  lastLogTerm,
			LastLogIndex: lastLogIndex,
		}

		rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))

		rf.mu.Unlock()

		go func() {
			clusterSize := len(rf.peers)
			voteChan := make(chan bool, clusterSize)
			for idx, _ := range rf.peers {
				if idx != rf.me {
					i := idx
					go rf.sendRequestVote(voteChan, i, &request)
				}
			}

			votesReceived := 1
			votesRequired := clusterSize/2 + 1

			for i := 0; i < clusterSize-1; i++ {
				vote := <-voteChan
				if vote == true {
					votesReceived++
				}
				if votesRequired == votesReceived {
					break
				}
			}

			if votesReceived < votesRequired {
				log.Printf("Voting failed for candidate %d in term %d", rf.me, request.Term)
			} else {
				if request.Term != rf.currentTerm {
					log.Printf("Voting passed for candidate %d in an **expired** term %d", rf.me, request.Term)
				} else {
					log.Printf("Voting passed for candidate %d in an term %d", rf.me, request.Term)
					rf.convertToLeader()
				}
			}

		}()

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.currentState = FOLLOWER
	rf.lastApplied = -1
	rf.commitIndex = -1
	rf.leaderId = -1
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i, _ := range peers {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = 0
	}

	rf.electionTimeout = time.Millisecond * 400
	rf.electionTimer = time.NewTicker(getRandomTimeout(rf.electionTimeout))
	rf.heartBeatTimeout = 200 * time.Millisecond
	rf.heartBeatTimer = time.NewTicker(10000000)
	rf.heartBeatTimer.Stop() // Hacky fix, cant initialize a ticker that's not stopped

	rf.userApplyChan = applyCh
	rf.logEntryPublishChanSize = 1000
	rf.logEntryPublishChan = make(chan LogEntry, rf.logEntryPublishChanSize) // No more than 1000 outstanding commands

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeatSender()
	go rf.logEntryPublisher()
	log.Printf("Initlized raft server %d %v", rf.me, rf)

	return rf
}
