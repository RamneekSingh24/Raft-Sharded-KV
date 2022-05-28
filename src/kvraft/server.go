package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
)

type Op struct {
	Type int
	Arg1 string
	Arg2 string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	indexReplyChans map[int]*chan string
	kv              map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	raftCmd := Op{
		Type: GET,
		Arg1: args.Key,
		Arg2: "",
	}
	kv.mu.Lock()
	idx, _, isLeader := kv.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		ch := make(chan string, 5)
		kv.indexReplyChans[idx] = &ch
		kv.mu.Unlock()
		val := <-ch
		reply.Value = val
		reply.Err = OK
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	opType := PUT
	if args.Op == "Append" {
		opType = APPEND
	}
	raftCmd := Op{
		Type: opType,
		Arg1: args.Key,
		Arg2: args.Value,
	}
	kv.mu.Lock()
	idx, _, isLeader := kv.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		ch := make(chan string, 5)
		kv.indexReplyChans[idx] = &ch
		kv.mu.Unlock()
		<-ch
		reply.Err = OK
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
			idx := applyMsg.CommandIndex
			reply := ""
			op, _ := applyMsg.Command.(Op)
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
			kv.mu.Lock()
			if ch, ok := kv.indexReplyChans[idx]; ok {
				*ch <- reply
				delete(kv.indexReplyChans, idx)
			}
			kv.mu.Unlock()
		} else {
			// TODO: snapshot
		}
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.indexReplyChans = make(map[int]*chan string)
	kv.kv = make(map[string]string)
	go kv.applyHandler()
	return kv
}
