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

type AppendEntryInternal struct {
	entry       LogEntry
	successChan chan bool
}

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

	currentState int
	commitIndex  int
	lastApplied  int
	leaderId     int
	//nextIndex    []int     // stored locally in handlers
	//matchIndex   []int

	electionTimeout          time.Duration
	electionTimer            *time.Ticker
	heartBeatTimeout         time.Duration
	notifyChanLength         int
	leaderPromoteNotifyChan  []chan int // term
	leaderDemoteNotifyChan   []chan int // term
	newEntryAppendedChanSize int
	newEntryAppendedChan     []chan int // index

	baseIndex int
	snapShot  []byte

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
func (rf *Raft) persist(saveSnapShot bool) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(len(rf.log))
	e.Encode(rf.log)
	state := w.Bytes()
	if saveSnapShot {
		rf.persister.SaveStateAndSnapshot(state, rf.snapShot)
	} else {
		rf.persister.SaveRaftState(state)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()
	rf.snapShot = rf.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		rf.snapShot = nil
		//log.Printf("Restoring from empty state")
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
		//log.Printf("restored state: currentTerm %d, votedFor %d, logs: %d", rf.currentTerm, rf.votedFor, rf.log)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// deprecated
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if index <= rf.baseIndex {
		return
	}
	Assert(rf.commitIndex >= index, "commit index should be greater than snapshot index, (%d > %d)", rf.commitIndex, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Assert(index <= rf.baseIndex+len(rf.log)-1, "snapshot index %d should be in my log %v", index, rf.log)
	rf.log = rf.log[index-rf.baseIndex:]
	rf.baseIndex = index
	rf.snapShot = snapshot
	rf.persist(true)
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

type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int
	BaseLogEntry LogEntry
	Snapshot     []byte
}

type InstallSnapshotReply struct {
	Term int // Reply.Term > Arg.Term means snapshot rejected(sender should revert to follower), otherwise installed
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		reply.Term = 0
	}
	log.Printf("server %d recieved Install snapshot request: %v, \n"+
		"curr log: %v, baseIdx: %d, commitIdx: %d, applyIdx %d", rf.me, *args, rf.log, rf.baseIndex, rf.commitIndex, rf.lastApplied)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term
	rf.currentTerm = args.Term

	if rf.baseIndex >= args.BaseLogEntry.Index {
		// probably an old delayed request
		// return success but do nothing here
		reply.Term = args.Term
		return
	}

	if rf.log[len(rf.log)-1].Index < args.BaseLogEntry.Index {
		rf.log = rf.log[:1]
		rf.log[0] = args.BaseLogEntry
		rf.snapShot = args.Snapshot
	} else {
		// my log already has the snapshot index
		snapshotLogArrayIndex := args.BaseLogEntry.Index - rf.baseIndex
		rf.log = rf.log[snapshotLogArrayIndex:]
		rf.log[0] = args.BaseLogEntry
		rf.snapShot = args.Snapshot
	}

	rf.baseIndex = rf.log[0].Index
	if rf.baseIndex > rf.lastApplied {
		rf.lastApplied = rf.baseIndex
		rf.userApplyChan <- ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  -1,
			SnapshotValid: true,
			Snapshot:      rf.snapShot,
			SnapshotTerm:  args.BaseLogEntry.Term,
			SnapshotIndex: args.BaseLogEntry.Index,
		}
	}
	if rf.baseIndex > rf.commitIndex {
		rf.commitIndex = rf.baseIndex
	}

	rf.persist(true)
	log.Printf("server %d  Install snapshot success, curr log: %v, baseIdx: %d, commitIdx: %d, applyidx: %d",
		rf.me, rf.log, rf.baseIndex, rf.commitIndex, rf.lastApplied)

}

func getRandomTimeout(timeout time.Duration) time.Duration {
	return timeout + time.Duration(rand.Float64()*float64(timeout))
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		reply.VoteGranted = false
		return
	}
	log.Printf("Server %d received vote request: %v", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf("Server %d voted no for %d in term %d due to my term > his, reply: %v", rf.me, args.CandidateId, rf.currentTerm, reply)
		return
	}
	if args.Term > rf.currentTerm || (rf.votedFor == -1 && rf.leaderId == -1) {

		myLastLogTerm := rf.log[len(rf.log)-1].Term
		myLastLogIndex := rf.log[len(rf.log)-1].Index

		// my log is ahead of candidate
		if myLastLogTerm > args.LastLogTerm ||
			(myLastLogTerm == args.LastLogTerm && myLastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			reply.Term = args.Term
			log.Printf("Server %d voted no for %d in term %d due to me more updated than him, reply: %v", rf.me, args.CandidateId, rf.currentTerm, reply)
			if args.Term > rf.currentTerm {
				// however, convert to follower if requester is on higher term
				rf.currentTerm = args.Term
				rf.convertToFollower()
			}
			return
		}
		// Candidate is atleast as uptodate as me
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.currentState = FOLLOWER
		rf.persist(false)
		rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))
		//log.Printf("Server %d voted for %d in term %d, reply: %v", rf.me, rf.votedFor, rf.currentTerm, reply)
	}
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
	log.Printf("Server %d: Sending voteRequest %v to server %d", rf.me, *args, server)
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok || !reply.VoteGranted {
		voteChan <- false
		if ok && reply.Term > args.Term {
			// someone may have higher term than me
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
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

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Success = false
		reply.Term = args.Term
		return
	}

	log.Printf("server %d recieved appendEntries request: %v, curr log: %v, baseIdx: %d", rf.me, *args, rf.log, rf.baseIndex)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		log.Printf("server %d: append entry %v failed, in my term: %d", rf.me, args, rf.currentTerm)
		return
	}

	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	rf.leaderId = args.LeaderId
	rf.currentState = FOLLOWER
	rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))

	reply.Term = args.Term
	reply.Success = false

	if args.PrevLogIndex-rf.baseIndex >= 0 &&
		args.PrevLogIndex <= rf.baseIndex+len(rf.log)-1 &&
		rf.log[args.PrevLogIndex-rf.baseIndex].Term == args.PrevLogTerm {
		// prev log index match
		reply.Success = true
	} else {
		reply.Success = false
		return
	}

	// My log matches with leader at the prev index.

	// Append / Update Entries
	// Assuming leader has sent us entries  after prev_index in continuous order

	appendIndex := args.PrevLogIndex - rf.baseIndex

	for idx, entry := range args.Entries {
		appendIndex++
		if appendIndex >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[idx:]...)
			appendIndex += len(args.Entries[idx:]) - 1
			break
		}
		if rf.log[appendIndex].Term != entry.Term {
			rf.log = append(rf.log[:appendIndex], args.Entries[idx:]...)
			appendIndex += len(args.Entries[idx:]) - 1
			break
		}
		rf.log[appendIndex] = entry
	}

	appendIndex += rf.baseIndex

	if args.LeaderCommitIndex > rf.commitIndex {
		if args.LeaderCommitIndex <= appendIndex {
			rf.commitIndex = args.LeaderCommitIndex
		} else {
			rf.commitIndex = appendIndex
		}
	}

	if appendIndex > args.PrevLogIndex {
		log.Printf("server %d appended entires: %v, curr log: %v, baseIdx : %d", rf.me, args.Entries, rf.log, rf.baseIndex)
		rf.persist(false)
	}

	for rf.lastApplied < rf.commitIndex {
		idx := rf.lastApplied + 1 - rf.baseIndex
		entry := rf.log[idx]
		log.Printf("server %d applied entry: %v", rf.me, entry)
		userMsg := ApplyMsg{
			CommandValid:  true,
			Command:       entry.Command,
			CommandIndex:  entry.Index,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.userApplyChan <- userMsg
		rf.lastApplied++
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) {
	ok := false
	continuousFails := 0
	for ok == false {
		log.Printf("server %d : sending append entry request to %d, %v", rf.me, server, args)
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !ok {
			continuousFails++
			if continuousFails > 2 {
				break
			}
			log.Printf("server %d : timedout on append entry reply from %d... retrying", rf.me, server)
		}
	}
	log.Printf("server %d : received append entry reply from %d, %v", rf.me, server, reply)

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := false
	continuousFails := 0
	for ok == false {
		log.Printf("server %d : sending install snapshot request to %d, %v", rf.me, server, args)
		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if !ok {
			continuousFails++
			if continuousFails > 2 {
				//time.Sleep(time.Millisecond * 1)
				continuousFails = 0
			}
		}
	}
	log.Printf("server %d : received snapshot install reply from %d, %v", rf.me, server, reply)
}

// Assumes caller has lock
func (rf *Raft) checkReplyValidity(term int) bool {
	if rf.currentState != LEADER {
		return false
	}
	if rf.currentTerm < term {
		// convert to follower
		rf.currentTerm = term
		rf.convertToFollower()
		return false
	}
	if term < rf.currentTerm {
		// ignore replies from previous terms
		return false
	}
	return true
}

// assumes caller has lock
func (rf *Raft) checkAndCommit(index int) {

}

func (rf *Raft) appendEntryHandler(server int) {

	matchIndex := 0
	nextIndex := 0

	for {
		term := <-rf.leaderPromoteNotifyChan[server]
		if term != rf.currentTerm {
			// ignore
			continue
		}
		// term is current term, I am leader
		rf.mu.Lock()
		matchIndex = 0 // reset match and next index
		nextIndex = rf.baseIndex + len(rf.log) - 1
		rf.mu.Unlock()
		log.Printf("server %d: started hbting", rf.me)

		for {
			rf.mu.Lock()
			currTerm := rf.currentTerm
			currState := rf.currentState
			currCommitIndex := rf.commitIndex
			if currState != LEADER {
				rf.mu.Unlock()
				break
			}
			// for safety
			matchIndex = Min(matchIndex, rf.baseIndex+len(rf.log)-1)
			nextIndex = Min(nextIndex, rf.baseIndex+len(rf.log)-1)

			Assert(matchIndex <= nextIndex, "match index %d > nextIndex %d",
				matchIndex, nextIndex)

			if nextIndex-matchIndex <= 1 {
				nextIndex = rf.baseIndex + len(rf.log) - 1
				// update nextIndex and send entries in a batch
				var entries []LogEntry
				if nextIndex == matchIndex {
					entries = make([]LogEntry, 0)
				} else {
					entries = rf.log[matchIndex+1-rf.baseIndex : nextIndex+1-rf.baseIndex]
				}
				prevLogEntry := rf.log[matchIndex-rf.baseIndex]
				rf.mu.Unlock()
				req := AppendEntriesRequest{
					Term:              currTerm,
					LeaderId:          rf.me,
					PrevLogIndex:      prevLogEntry.Index,
					PrevLogTerm:       prevLogEntry.Term,
					LeaderCommitIndex: currCommitIndex,
					Entries:           entries,
				}
				reply := AppendEntriesReply{}
				rf.sendAppendEntry(server, &req, &reply)
				// check if iam still leader
				rf.mu.Lock()
				if !rf.checkReplyValidity(reply.Term) {
					rf.mu.Unlock()
					break
				}
				if !reply.Success {
					// match index should have matched
					// need to install snapshot
					snapshotReq := InstallSnapshotArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						BaseLogEntry: rf.log[0],
						Snapshot:     rf.snapShot,
					}
					rf.mu.Unlock()

					snapshotReply := InstallSnapshotReply{}

					rf.sendInstallSnapshot(server, &snapshotReq, &snapshotReply)

					rf.mu.Lock()
					if !rf.checkReplyValidity(snapshotReply.Term) {
						break
						rf.mu.Unlock()
						continue
					}
					// Snapshot install success
					matchIndex = snapshotReq.BaseLogEntry.Index
					nextIndex = matchIndex + 1
					rf.mu.Unlock()

				} else {
					// reply success
					if len(req.Entries) > 0 {
						matchIndex = req.Entries[len(req.Entries)-1].Index
					}
					nextIndex = matchIndex + 1
					rf.mu.Unlock()
				}

			} else {
				// Note: Lock is already held
				for nextIndex-matchIndex > 1 {

					if nextIndex <= rf.baseIndex {
						// need to install snapshot
						snapshotReq := InstallSnapshotArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							BaseLogEntry: rf.log[0],
							Snapshot:     rf.snapShot,
						}
						rf.mu.Unlock()

						snapshotReply := InstallSnapshotReply{}

						rf.sendInstallSnapshot(server, &snapshotReq, &snapshotReply)

						rf.mu.Lock()
						if !rf.checkReplyValidity(snapshotReply.Term) {
							rf.mu.Unlock()
							break
						}
						// Snapshot install success
						matchIndex = snapshotReq.BaseLogEntry.Index
						nextIndex = matchIndex + 1
						rf.mu.Unlock()
					} else {

						prevLogEntry := rf.log[nextIndex-1-rf.baseIndex]
						req := AppendEntriesRequest{
							Term:              currTerm,
							LeaderId:          rf.me,
							PrevLogIndex:      prevLogEntry.Index,
							PrevLogTerm:       prevLogEntry.Term,
							LeaderCommitIndex: currCommitIndex,
							Entries:           []LogEntry{rf.log[nextIndex-rf.baseIndex]},
						}
						rf.mu.Unlock()

						reply := AppendEntriesReply{}
						rf.sendAppendEntry(server, &req, &reply)

						rf.mu.Lock()

						if !rf.checkReplyValidity(reply.Term) {
							rf.mu.Unlock()
							break
						}

						if reply.Success {
							// reply success
							if len(req.Entries) > 0 {
								matchIndex = req.Entries[len(req.Entries)-1].Index
							}
							nextIndex = matchIndex + 1
						} else {
							nextIndex--
						}

						rf.mu.Unlock()
					}
				}
			}
			time.Sleep(rf.heartBeatTimeout)
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

	//log.Printf("server %d received req: %v", rf.me, command)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.currentState == LEADER

	if !isLeader {
		return -1, -1, isLeader
	}

	index := rf.baseIndex + len(rf.log)
	term := rf.currentTerm

	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	rf.persist(false)

	go func() { // Do not block the return
		for i, _ := range rf.peers {
			if i != rf.me {
				rf.newEntryAppendedChan[i] <- entry.Index
			}
		}
	}()

	return index, term, isLeader
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
	rf.leaderId = -1
	rf.votedFor = rf.me
	rf.persist(false)

	for i, _ := range rf.peers {
		if i != rf.me {
			rf.leaderPromoteNotifyChan[i] <- rf.currentTerm
		}
	}
}

// convert to follower if received a request / reply from higher term
// assumes caller is holding lock
func (rf *Raft) convertToFollower() {
	if rf.currentState == FOLLOWER {
		return
	}
	log.Printf("Server %d converting to follower", rf.me)
	rf.votedFor = -1
	rf.leaderId = -1
	rf.electionTimer.Reset(getRandomTimeout(rf.electionTimeout))
	if rf.currentState == LEADER {
		for i, _ := range rf.peers {
			if i != rf.me {
				rf.leaderDemoteNotifyChan[i] <- rf.currentTerm
			}
		}
	}
	rf.currentState = FOLLOWER
	rf.persist(false)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		<-rf.electionTimer.C
		//log.Printf("Election timeout occured at server %d in term: %d", rf.me, rf.currentTerm)
		rf.mu.Lock()
		if rf.currentState == LEADER {
			rf.electionTimer.Stop()
			rf.mu.Unlock()
			continue
		}
		rf.currentTerm++
		rf.currentState = CANDIDATE
		rf.votedFor = rf.me

		lastLogTerm := rf.log[len(rf.log)-1].Term
		lastLogIndex := rf.log[len(rf.log)-1].Index

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
				//log.Printf("Voting failed for candidate %d in term %d", rf.me, request.Term)
			} else {
				if request.Term != rf.currentTerm {
					//log.Printf("Voting passed for candidate %d in an **expired** term %d", rf.me, request.Term)
				} else {
					//log.Printf("Voting passed for candidate %d in an term %d", rf.me, request.Term)
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
	rf.leaderId = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		Term:    0,  // Snapshot Term  will be set in readPersist()
		Index:   0,  // Snapshot Index will be set in readPersist()
		Command: -1, // Garbage Value
	}

	rf.currentState = FOLLOWER
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.leaderId = -1

	rand.Seed(time.Now().UnixNano())

	rf.electionTimeout = time.Millisecond * 500
	rf.electionTimer = time.NewTicker(getRandomTimeout(rf.electionTimeout))
	rf.heartBeatTimeout = 200 * time.Millisecond

	rf.newEntryAppendedChanSize = 5000
	rf.notifyChanLength = 1000

	rf.newEntryAppendedChan = make([]chan int, len(rf.peers))
	rf.leaderPromoteNotifyChan = make([]chan int, len(rf.peers))
	rf.leaderDemoteNotifyChan = make([]chan int, len(rf.peers))

	for i, _ := range peers {
		if i != me {
			rf.newEntryAppendedChan[i] = make(chan int, rf.newEntryAppendedChanSize)
			rf.leaderPromoteNotifyChan[i] = make(chan int, rf.notifyChanLength)
			rf.leaderDemoteNotifyChan[i] = make(chan int, rf.notifyChanLength)
			go rf.appendEntryHandler(i)
		}
	}

	rf.userApplyChan = applyCh

	rf.baseIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist()

	go rf.ticker()

	log.Printf("Initlized raft server %d %v", rf.me, rf)

	//log.SetOutput(ioutil.Discard)
	return rf
}
