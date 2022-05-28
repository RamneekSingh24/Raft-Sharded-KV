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
	//leaderId     int
	nextIndex  []int
	matchIndex []int

	electionTimeout          time.Duration
	electionTimer            *time.Ticker
	heartBeatTimeout         time.Duration
	notifyChanLength         int
	leaderPromoteNotifyChan  []chan int // term
	commitCheckApplyChanSize int
	commitCheckApplyChan     chan int // match index

	heartBeatTicker []*time.Ticker

	baseIndex int
	snapShot  []byte

	userApplyChan chan ApplyMsg

	indirectApplyChan chan *ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.currentTerm), rf.currentState == LEADER
}

func (rf *Raft) PrintState() {
	//log.Printf("server %d: current term: %d, current state %d, apply idx: %d,  logs %v", rf.me, rf.currentTerm, rf.currentState, rf.lastApplied, rf.log)
	//log.Printf("server %d: match index: %v, next index: %v", rf.me, rf.matchIndex, rf.nextIndex)
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
		////log.Printf("Restoring from empty state")
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
	rf.baseIndex = rf.log[0].Index
	rf.lastApplied = rf.baseIndex
	rf.commitIndex = rf.baseIndex
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
	Assert(rf.lastApplied >= index, "applied index should be greater than snapshot index, (%d > %d)", rf.commitIndex, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Assert(index <= rf.baseIndex+len(rf.log)-1, "snapshot index %d should be in my log %v", index, rf.log)

	var reApplyLogs []LogEntry

	if rf.lastApplied > index {
		reApplyLogs = append(reApplyLogs, rf.log[index+1-rf.baseIndex:rf.lastApplied-rf.baseIndex+1]...)
	}

	rf.log = rf.log[index-rf.baseIndex:]
	rf.baseIndex = index
	rf.snapShot = snapshot
	rf.persist(true)
	rf.indirectApplyChan <- &ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  -1,
		SnapshotValid: true,
		Snapshot:      rf.snapShot,
		SnapshotTerm:  rf.log[0].Term,
		SnapshotIndex: rf.log[0].Index,
	}
	for _, entry := range reApplyLogs {
		rf.indirectApplyChan <- &ApplyMsg{
			CommandValid:  true,
			Command:       entry.Command,
			CommandIndex:  entry.Index,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}

	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.matchIndex[i] < rf.baseIndex {
				rf.matchIndex[i] = rf.baseIndex
			}
			if rf.nextIndex[i] <= rf.matchIndex[i] {
				rf.nextIndex[i] = rf.matchIndex[i] + 1
			}
		}
	}
	//log.Printf("server %d: created snapshot %v", rf.me, rf.snapShot)
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

	//log.Printf("server %d recieved Install snapshot request: %v, \n"+
	//"curr log: %v, baseIdx: %d, commitIdx: %d, applyIdx %d", rf.me, *args, rf.log, rf.baseIndex, rf.commitIndex, rf.lastApplied)

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
		rf.indirectApplyChan <- &ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  -1,
			SnapshotValid: true,
			Snapshot:      rf.snapShot,
			SnapshotTerm:  args.BaseLogEntry.Term,
			SnapshotIndex: args.BaseLogEntry.Index,
		}
		rf.lastApplied = rf.baseIndex
	}
	if rf.baseIndex > rf.commitIndex {
		rf.commitIndex = rf.baseIndex
	}

	rf.persist(true)
	//log.Printf("server %d  Install snapshot success, curr log: %v, baseIdx: %d, commitIdx: %d, applyidx: %d",
	//rf.me, rf.log, rf.baseIndex, rf.commitIndex, rf.lastApplied)

}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	defer rf.PrintState()
	//log.Printf("Server %d received vote request: %v", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//log.Printf("Server %d voted no for %d in term %d due to my term > his, reply: %v", rf.me, args.CandidateId, rf.currentTerm, reply)
		return
	}
	if args.Term > rf.currentTerm || (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {

		newVote := args.Term > rf.currentTerm || rf.votedFor == -1
		myLastLogTerm := rf.log[len(rf.log)-1].Term
		myLastLogIndex := rf.log[len(rf.log)-1].Index

		// my log is ahead of candidate
		if myLastLogTerm > args.LastLogTerm ||
			(myLastLogTerm == args.LastLogTerm && myLastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			reply.Term = args.Term
			if args.Term > rf.currentTerm {
				// however, convert to follower if requester is on higher term
				rf.currentTerm = args.Term
				rf.convertToFollower(-1)
			}
			//log.Printf("Server %d voted no for %d in term %d due to me more updated than him, reply: %v", rf.me, args.CandidateId, rf.currentTerm, reply)
			return
		}
		// Candidate is atleast as uptodate as me
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.currentTerm = args.Term
		if newVote {
			rf.convertToFollower(args.CandidateId)
		}
		////log.Printf("Server %d voted for %d in term %d, reply: %v", rf.me, rf.votedFor, rf.currentTerm, reply)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
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
	//log.Printf("Server %d: Sending voteRequest %v to server %d", rf.me, *args, server)
	reply := RequestVoteReply{}

	//atomic.AddInt32(&rf.rpcCount, 1)
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)

	if !ok || !reply.VoteGranted {
		voteChan <- false
		if ok && reply.Term > args.Term {
			// someone may have higher term than me
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.convertToFollower(-1)
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
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {

	defer rf.PrintState()
	//log.Printf("server %d recieved appendEntries request: %v, curr log: %v, baseIdx: %d", rf.me, *args, rf.log, rf.baseIndex)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer //log.Printf("server %d: sent reply: %v", rf.me, reply)
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		//log.Printf("server %d: append entry %v failed, in my term: %d", rf.me, args, rf.currentTerm)
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.convertToFollower(-1)
	} else if rf.currentState == CANDIDATE {
		rf.convertToFollower(rf.me)
	}

	rf.electionTimer.Reset(GetRandomTimeout(rf.electionTimeout))

	reply.Term = args.Term
	reply.Success = false

	Assert(args.PrevLogIndex >= rf.baseIndex,
		"args. prev log index should be >= my base index(%d>=%d)", args.PrevLogIndex, rf.baseIndex)

	if args.PrevLogIndex > rf.baseIndex+len(rf.log)-1 {
		// I dont have prevlogindex
		reply.ConflictIndex = len(rf.log) + rf.baseIndex
		reply.Success = false
		return
	} else if rf.log[args.PrevLogIndex-rf.baseIndex].Term != args.PrevLogTerm {
		i := args.PrevLogIndex - rf.baseIndex
		for i >= 1 && rf.log[i].Term == rf.log[i-1].Term {
			i--
		}
		reply.ConflictIndex = i
		reply.Success = false
		return
	}

	reply.Success = true

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
		//log.Printf("server %d appended entires: %v, curr log: %v, baseIdx : %d", rf.me, args.Entries, rf.log, rf.baseIndex)
		rf.persist(false)
	}

	for rf.lastApplied < rf.commitIndex {
		idx := rf.lastApplied + 1 - rf.baseIndex
		entry := rf.log[idx]
		//log.Printf("server %d: applied entry: %v", rf.me, entry)
		userMsg := ApplyMsg{
			CommandValid:  true,
			Command:       entry.Command,
			CommandIndex:  entry.Index,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.indirectApplyChan <- &userMsg
		rf.lastApplied++
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {

	//log.Printf("server %d : sending install snapshot request to %d, %v", rf.me, server, args)

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	//log.Printf("server %d : %v, received snapshot install reply from %d, %v", rf.me, ok, server, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	//log.Printf("server %d : sending install append request to %d, %v", rf.me, server, args)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//log.Printf("server %d : %v, received append entries reply from %d, %v", rf.me, ok, server, reply)
	return ok
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		userMsg := <-rf.indirectApplyChan
		//log.Printf("server %d : (apply idx : %d), sending msg(index = %d),  %v to userapply chan", rf.me, rf.lastApplied, userMsg.CommandIndex, userMsg)
		rf.userApplyChan <- *userMsg
	}
}

// assumes caller has lock
func (rf *Raft) committer() {
	for rf.killed() == false {
		idx := <-rf.commitCheckApplyChan
		//log.Printf("Server %d: commiter got index: %d, matchindexes: %v", rf.me, idx, rf.matchIndex)
		rf.mu.Lock()
		if rf.currentState != LEADER {
			// dont check and commit if not leader
			rf.mu.Unlock()
			continue
		}
		if idx > rf.baseIndex+len(rf.log)-1 || idx <= rf.baseIndex {
			// (1) deleted, got idx from an old term
			// (2) already committed, ignore
			rf.mu.Unlock()
			continue
		}
		ci := rf.commitIndex
		for ci < idx {
			nextIndex := ci + 1

			req := (len(rf.peers)/2 + 1) - 1
			for i, _ := range rf.peers {
				if i != rf.me {
					if rf.matchIndex[i] >= nextIndex {
						req--
					}
				}
			}
			if req <= 0 {
				ci++
			} else {
				break
			}
		}
		if rf.log[ci-rf.baseIndex].Term == rf.currentTerm {
			// only update if found something from current term
			rf.commitIndex = ci
			//log.Printf("server %d: updating commit index to %d", rf.me, rf.commitIndex)
		}

		// apply entries

		for rf.lastApplied < rf.commitIndex {
			entry := rf.log[rf.lastApplied+1-rf.baseIndex]
			rf.indirectApplyChan <- &ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  entry.Index,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			//log.Printf("server %d: applied entry: %v", rf.me, entry)
			rf.lastApplied++
		}

		rf.mu.Unlock()

	}
}

func (rf *Raft) appendEntryHandler(server int) {
	for rf.killed() == false {
		term := <-rf.leaderPromoteNotifyChan[server]
		if term != rf.currentTerm || rf.currentState != LEADER {
			// ignore
			continue
		}
		// term is current term, I am leader
		rf.mu.Lock()
		rf.matchIndex[server] = 0
		rf.nextIndex[server] = rf.baseIndex + len(rf.log)
		rf.heartBeatTicker[server].Reset(time.Microsecond * 500)
		rf.mu.Unlock()

		for rf.killed() == false {
			// periodically send append entries
			if term != rf.currentTerm {
				break
			}
			<-rf.heartBeatTicker[server].C
			go func(term int) {

				if term != rf.currentTerm {
					return // fast exit
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer rf.PrintState()

				//log.Printf("server %d: append entry timeout occured for server %d", rf.me, server)

				if term != rf.currentTerm {
					return
				}

				matchIndex := rf.matchIndex[server]
				nextIndex := rf.nextIndex[server]

				if nextIndex == matchIndex || nextIndex <= rf.baseIndex {
					//Assert(nextIndex == rf.baseIndex,
					//	"next index == matchindex only if nextindex(%d) = base index(%d)", nextIndex, rf.baseIndex)

					// need to install snapshot
					req := InstallSnapshotArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						BaseLogEntry: rf.log[0],
						Snapshot:     rf.snapShot,
					}
					rf.mu.Unlock()
					reply := InstallSnapshotReply{}

					rf.mu.Lock()
					ok := rf.sendInstallSnapshot(server, &req, &reply)
					if !ok {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertToFollower(-1)
					} else if reply.Term < rf.currentTerm {
						// ignore
					} else {
						// snapshot install success
						rf.nextIndex[server] = rf.matchIndex[server] + 1 // always safe to do this
						// we do not allow match index to go below base index, so no need to change
					}

				} else {
					entriesToSend := make([]LogEntry, 0)
					if nextIndex >= rf.baseIndex+len(rf.log) {
						// empty
					} else {
						entriesToSend = append(entriesToSend, rf.log[nextIndex-rf.baseIndex:]...)
					}
					prevEntry := rf.log[nextIndex-1-rf.baseIndex]

					req := AppendEntriesRequest{
						Term:              term,
						LeaderId:          rf.me,
						PrevLogIndex:      prevEntry.Index,
						PrevLogTerm:       prevEntry.Term,
						LeaderCommitIndex: rf.commitIndex,
						Entries:           entriesToSend,
					}
					rf.mu.Unlock()

					reply := AppendEntriesReply{}

					ok := rf.sendAppendEntries(server, &req, &reply)
					rf.mu.Lock()

					if !ok {
						// ignore
						return
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertToFollower(-1)
						return
					}
					if reply.Term != rf.currentTerm {
						// ignore
						return
					}
					if reply.Success == true {
						newMatchIndex := req.PrevLogIndex + len(req.Entries)
						newNextIndex := newMatchIndex + 1
						if newMatchIndex > rf.matchIndex[server] {
							rf.matchIndex[server] = newMatchIndex // safe
							rf.commitCheckApplyChan <- newMatchIndex
						}
						if newNextIndex > rf.nextIndex[server] {
							rf.nextIndex[server] = newNextIndex
						}
					} else {
						newNextIndex := reply.ConflictIndex
						if newNextIndex >= rf.matchIndex[server]+1 || rf.baseIndex > 0 {
							// if base index is zero we dont have snapshot, cant send install snapshot
							// if baseindex > 0 safe to make nextIndex < matchIndex(next time a snapshot will be installed)
							rf.nextIndex[server] = newNextIndex // safe
						}
					}
				}
			}(term)
			rf.heartBeatTicker[server].Reset(rf.heartBeatTimeout)
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

	for i, _ := range rf.peers {
		if i != rf.me {
			rf.heartBeatTicker[i].Reset(time.Microsecond * 300)
		}
	}

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

// Assumes caller holds lock
func (rf *Raft) convertToLeader() {
	if rf.currentState != CANDIDATE {
		panic("Invalid state while converting to leader")
	}
	//log.Printf("Server %d became leader", rf.me)

	rf.electionTimer.Stop()
	rf.currentState = LEADER
	rf.votedFor = rf.me
	rf.persist(false) // not needed?
	rf.electionTimer.Stop()
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.leaderPromoteNotifyChan[i] <- rf.currentTerm
		}
	}
}

// convert to follower if received a request / reply from higher term
// assumes caller is holding lock
func (rf *Raft) convertToFollower(votedFor int) {

	//log.Printf("Server %d converting to follower", rf.me)
	rf.votedFor = votedFor
	rf.electionTimer.Reset(GetRandomTimeout(rf.electionTimeout))

	rf.currentState = FOLLOWER
	rf.persist(false)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		<-rf.electionTimer.C
		//log.Printf("server %d: Election timeout occured at server in term: %d", rf.me, rf.currentTerm)
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

		rf.electionTimer.Reset(GetRandomTimeout(rf.electionTimeout))

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
				////log.Printf("Voting failed for candidate %d in term %d", rf.me, request.Term)
			} else {
				rf.mu.Lock()
				if request.Term != rf.currentTerm {
					////log.Printf("Voting passed for candidate %d in an **expired** term %d", rf.me, request.Term)
				} else {
					////log.Printf("Voting passed for candidate %d in an term %d", rf.me, request.Term)
					rf.convertToLeader()
				}
				rf.mu.Unlock()
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

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		Term:    0,  // Snapshot Term  will be set in readPersist()
		Index:   0,  // Snapshot Index will be set in readPersist()
		Command: -1, // Garbage Value
	}

	rf.currentState = FOLLOWER

	rand.Seed(time.Now().UnixNano())

	rf.electionTimeout = time.Millisecond * 300
	rf.electionTimer = time.NewTicker(GetRandomTimeout(rf.electionTimeout))
	rf.heartBeatTimeout = 100 * time.Millisecond

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	rf.notifyChanLength = 1000
	rf.commitCheckApplyChanSize = 5000

	rf.commitCheckApplyChan = make(chan int, rf.commitCheckApplyChanSize)

	rf.leaderPromoteNotifyChan = make([]chan int, len(rf.peers))
	rf.heartBeatTicker = make([]*time.Ticker, len(rf.peers))

	for i, _ := range peers {
		if i != me {
			rf.leaderPromoteNotifyChan[i] = make(chan int, rf.notifyChanLength)
			rf.heartBeatTicker[i] = time.NewTicker(time.Millisecond * 1)
			rf.heartBeatTicker[i].Stop()
			go rf.appendEntryHandler(i)
		}
	}

	rf.userApplyChan = applyCh
	rf.indirectApplyChan = make(chan *ApplyMsg, 5000)

	// initialize from state persisted before a crash
	rf.readPersist()

	//rf.rpcCount = 0

	go rf.ticker()
	go rf.committer()
	go rf.applier()
	//log.SetOutput(ioutil.Discard)

	//log.Printf("Initlized raft server %d %v", rf.me, rf)

	return rf
}
