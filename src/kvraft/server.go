package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		//// log.Printf(format, a...)
	}
	return
}

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
)

type Op struct {
	Type        int
	Arg1        string
	Arg2        string
	ClientUuid  int64
	ClientReqNo int32
}

type InternalResp struct {
	ReqNumber int32
	Reply     string
	Valid     bool // Valid = false means send ErrOldRequest reply
	Term      int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	kv                     map[string]string
	lastClientCommandReply map[int64]InternalResp
	ReplyWaitChan          map[string]chan *InternalResp

	lastApplied  int
	snapShotLock sync.Mutex
}

func (kv *KVServer) LockServer() {
	kv.mu.Lock()
	kv.snapShotLock.Lock()
}
func (kv *KVServer) UnLockServer() {
	kv.mu.Unlock()
	kv.snapShotLock.Unlock()
}

func (kv *KVServer) GetKV() *map[string]string {
	return &kv.kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//// log.Printf("kv server %d recvd req:", kv.me)
	raftCmd := Op{
		Type:        GET,
		Arg1:        args.Key,
		Arg2:        "",
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}
	kv.mu.Lock()
	idx, term, isLeader := kv.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		ch := make(chan *InternalResp, 1)
		kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
		kv.mu.Unlock()
		resp := <-ch
		if !resp.Valid {
			reply.Err = ErrOldRequest
		} else if resp.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Value = resp.Reply
			reply.Err = OK
			//// log.Printf("server %d: completed req: %v, state :%v", kv.me, args, kv.kv)
			//kv.rf.PrintState()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//// log.Printf("kv server %d recvd req:", kv.me)
	opType := PUT
	if args.Op == "Append" {
		opType = APPEND
	}
	raftCmd := Op{
		Type:        opType,
		Arg1:        args.Key,
		Arg2:        args.Value,
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}

	kv.mu.Lock()
	idx, term, isLeader := kv.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		ch := make(chan *InternalResp, 1)
		kv.ReplyWaitChan[termIndexToString(term, idx)] = ch
		kv.mu.Unlock()
		resp := <-ch
		if !resp.Valid {
			reply.Err = ErrOldRequest
		} else if resp.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			//// log.Printf("server %d: completed req: %v, state :%v", kv.me, args, kv.kv)
			//kv.rf.PrintState()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyHandler() {
	for kv.killed() == false {

		applyMsg := <-kv.applyCh

		if applyMsg.CommandValid {
			kv.snapShotLock.Lock()
			op, _ := applyMsg.Command.(Op)
			kv.lastApplied = applyMsg.CommandIndex
			lastClientReply, ok := kv.lastClientCommandReply[op.ClientUuid]
			reply := ""
			isValid := true
			if !ok || lastClientReply.ReqNumber < op.ClientReqNo {
				// apply
				if op.Type == GET {
					val, ok := kv.kv[op.Arg1]
					if !ok {
						val = ""
					}
					reply = val
				} else if op.Type == PUT {
					kv.kv[op.Arg1] = op.Arg2
				} else {
					oldVal, ok := kv.kv[op.Arg1]
					if !ok {
						oldVal = ""
					}
					kv.kv[op.Arg1] = oldVal + op.Arg2
				}
				isValid = true
				kv.lastClientCommandReply[op.ClientUuid] = InternalResp{
					ReqNumber: op.ClientReqNo,
					Reply:     reply,
					Valid:     isValid,
				}
			} else {
				// already applied
				if op.ClientReqNo == lastClientReply.ReqNumber {
					// still have reply
					reply = lastClientReply.Reply
					isValid = true
				} else {
					// old
					isValid = false
				}
			}
			kv.snapShotLock.Unlock()

			kv.mu.Lock()
			waitChan, ok := kv.ReplyWaitChan[termIndexToString(applyMsg.CommandTerm, applyMsg.CommandIndex)]
			kv.mu.Unlock()
			if ok {
				waitChan <- &InternalResp{
					Reply: reply,
					Valid: isValid,
					Term:  applyMsg.CommandTerm,
				}
			} else {
				delete(kv.ReplyWaitChan, termIndexToString(applyMsg.CommandTerm, applyMsg.CommandIndex))
			}
		} else {
			kv.snapShotLock.Lock()
			kv.loadSnapShot(applyMsg.Snapshot)
			kv.snapShotLock.Unlock()
		}
	}
}

func (kv *KVServer) loadSnapShot(snapShot []byte) {

	if snapShot == nil || len(snapShot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.lastApplied) != nil || // snapshot index
		d.Decode(&kv.kv) != nil || d.Decode(&kv.lastClientCommandReply) != nil {
		log.Fatal("kvserver: Failed to restore snapshot")
	}

	// log.Printf("kv server %d loaded snapshot: %v", kv.me, string(b))
}

func (kv *KVServer) stateCompactor() {
	if kv.maxraftstate == -1 {
		return
	}
	for kv.killed() == false {
		// create snapshot
		kv.snapShotLock.Lock()
		if kv.rf.GetRaftStateSize() >= kv.maxraftstate/2 {
			kvCopy := make(map[string]string)
			clientReplyCopy := make(map[int64]InternalResp)

			for k, v := range kv.kv {
				kvCopy[k] = v
			}
			for k, v := range kv.lastClientCommandReply {
				clientReplyCopy[k] = v
			}
			snapshotIndex := kv.lastApplied // raft should not delete indexes that we haven't applied
			kv.snapShotLock.Unlock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(snapshotIndex)
			e.Encode(kvCopy)
			e.Encode(clientReplyCopy)
			snapshot := w.Bytes()
			kv.rf.Snapshot(snapshotIndex, snapshot)
		} else {
			kv.snapShotLock.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(map[string]string{})
	labgob.Register(InternalResp{})
	labgob.Register(map[int64]InternalResp{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kv = make(map[string]string)
	kv.lastClientCommandReply = make(map[int64]InternalResp)
	kv.ReplyWaitChan = make(map[string]chan *InternalResp)
	kv.lastApplied = 0

	kv.loadSnapShot(persister.ReadSnapshot())

	for _, entry := range kv.rf.Log {
		if entry.Index > kv.rf.LastApplied {
			break
		}
		if entry.Index > kv.lastApplied {
			// loaded from snapshot
			continue
		}
		kv.lastApplied = entry.Index
		op, _ := entry.Command.(Op)
		lastClientReply, ok := kv.lastClientCommandReply[op.ClientUuid]
		reply := ""
		if !ok || lastClientReply.ReqNumber < op.ClientReqNo {
			// apply and save reply
			if op.Type == GET {
				val, ok := kv.kv[op.Arg1]
				if !ok {
					val = ""
				}
				reply = val
			} else if op.Type == PUT {
				kv.kv[op.Arg1] = op.Arg2
			} else {
				oldVal, ok := kv.kv[op.Arg1]
				if !ok {
					oldVal = ""
				}
				kv.kv[op.Arg1] = oldVal + op.Arg2
			}
			kv.lastClientCommandReply[op.ClientUuid] = InternalResp{
				ReqNumber: op.ClientReqNo,
				Reply:     reply,
			}
		}
	}

	//b, err := json.MarshalIndent(kv.kv, "", "  ")
	//if err != nil {
	//	fmt.Println("error:", err)
	//}

	// log.Printf("kv server %d started: %v", kv.me, string(b))

	go kv.applyHandler()
	go kv.stateCompactor()
	return kv
}
