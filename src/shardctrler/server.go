package shardctrler

import (
	"6.824/raft"
	"log"
	"sort"
	"sync/atomic"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	JOIN  = 0
	LEAVE = 1
	MOVE  = 2
	QUERY = 3
)

type Op struct {
	Type        int
	Args        interface{}
	ClientUuid  int64
	ClientReqNo int32
}

type InternalResp struct {
	ReqNumber int32
	Reply     Config
	Valid     bool // Valid = false means send ErrOldRequest reply
	Term      int
}

type ShardCtrler struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	isKilled int32

	configs []Config // indexed by config num

	lastClientCommandReply map[int64]InternalResp
	ReplyWaitChan          map[string]chan *InternalResp
	lastApplied            int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	raftCmd := Op{
		Type:        JOIN,
		Args:        args.Servers,
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}
	sc.mu.Lock()
	idx, term, isLeader := sc.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	} else {
		ch := make(chan *InternalResp, 1)
		sc.ReplyWaitChan[termIndexToString(term, idx)] = ch
		sc.mu.Unlock()
		resp := <-ch
		if !resp.Valid {
			reply.Err = ErrOldRequest
			reply.WrongLeader = true
		} else if resp.Term != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	raftCmd := Op{
		Type:        LEAVE,
		Args:        args.GIDs,
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}
	sc.mu.Lock()
	idx, term, isLeader := sc.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	} else {
		ch := make(chan *InternalResp, 1)
		sc.ReplyWaitChan[termIndexToString(term, idx)] = ch
		sc.mu.Unlock()
		resp := <-ch
		if !resp.Valid {
			reply.Err = ErrOldRequest
			reply.WrongLeader = true
		} else if resp.Term != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
	}
}

type MoveInfo struct {
	Shard int
	GID   int
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	raftCmd := Op{
		Type: MOVE,
		Args: MoveInfo{
			Shard: args.Shard,
			GID:   args.GID,
		},
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}
	sc.mu.Lock()
	idx, term, isLeader := sc.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	} else {
		ch := make(chan *InternalResp, 1)
		sc.ReplyWaitChan[termIndexToString(term, idx)] = ch
		sc.mu.Unlock()
		resp := <-ch
		if !resp.Valid {
			reply.Err = ErrOldRequest
			reply.WrongLeader = true
		} else if resp.Term != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	raftCmd := Op{
		Type:        QUERY,
		Args:        args.Num,
		ClientUuid:  args.ClientId,
		ClientReqNo: args.RequestNumber,
	}
	sc.mu.Lock()
	idx, term, isLeader := sc.rf.Start(raftCmd)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	} else {
		ch := make(chan *InternalResp, 1)
		sc.ReplyWaitChan[termIndexToString(term, idx)] = ch
		sc.mu.Unlock()
		resp := <-ch
		if !resp.Valid {
			reply.Err = ErrOldRequest
			reply.WrongLeader = true
		} else if resp.Term != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Config = resp.Reply
			reply.Err = OK
			reply.WrongLeader = false
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	atomic.StoreInt32(&sc.isKilled, 1)
}

func (sc *ShardCtrler) killed() bool {
	v := atomic.LoadInt32(&sc.isKilled)
	return v == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyHandler() {
	for sc.killed() == false {

		applyMsg := <-sc.applyCh

		if applyMsg.CommandValid {
			op, _ := applyMsg.Command.(Op)
			//// log.Printf("%d, %v", op.Type, op.Args)
			sc.lastApplied = applyMsg.CommandIndex
			lastClientReply, ok := sc.lastClientCommandReply[op.ClientUuid]
			var reply Config
			isValid := true
			if !ok || lastClientReply.ReqNumber < op.ClientReqNo {
				// apply
				if op.Type == JOIN {
					newGroups, _ := op.Args.(map[int][]string)
					sc.handleJoin(newGroups)
				} else if op.Type == LEAVE {
					leavingGroups, _ := op.Args.([]int)
					sc.handleLeave(leavingGroups)
				} else if op.Type == MOVE {
					moveInfo, _ := op.Args.(MoveInfo)
					sc.handleMove(moveInfo.Shard, moveInfo.GID)
				} else {
					idx, _ := op.Args.(int)
					if idx == -1 || idx >= len(sc.configs) {
						idx = len(sc.configs) - 1
					}
					reply = sc.configs[idx]
				}
				isValid = true
				sc.lastClientCommandReply[op.ClientUuid] = InternalResp{
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

			sc.mu.Lock()
			waitChan, ok := sc.ReplyWaitChan[termIndexToString(applyMsg.CommandTerm, applyMsg.CommandIndex)]
			if ok {
				sc.mu.Unlock()
				waitChan <- &InternalResp{
					Reply: reply,
					Valid: isValid,
					Term:  applyMsg.CommandTerm,
				}
			} else {
				delete(sc.ReplyWaitChan, termIndexToString(applyMsg.CommandTerm, applyMsg.CommandIndex))
				sc.mu.Unlock()
			}
		} else {
			log.Fatal("No snapshotting in shardctrler...")
		}
	}
}

func (sc *ShardCtrler) handleJoin(newGroups map[int][]string) {
	// log.Printf("shardctrler: %v joined", newGroups)
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = v
	}
	for k, v := range newGroups {
		newConfig.Groups[k] = v
	}
	newGIDs := make([]int, 0)
	for k, _ := range newConfig.Groups {
		newGIDs = append(newGIDs, k)
	}
	sort.Ints(newGIDs)
	shardsPerGroup := (NShards + len(newGIDs) - 1) / len(newGIDs)
	numShardsInGroup := make([]int, len(newGIDs))
	{
		tot := 0
		for i, _ := range numShardsInGroup {
			numShardsInGroup[i] = shardsPerGroup
			tot += shardsPerGroup
		}
		i := 0
		for tot > NShards {
			numShardsInGroup[i]--
			tot--
			i++
		}
	}

	idx := 0
	for i := 0; i < NShards; i++ {
		if numShardsInGroup[idx] == 0 {
			idx++
		}
		newConfig.Shards[i] = newGIDs[idx]
		numShardsInGroup[idx]--
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) handleLeave(leavingGroups []int) {
	// log.Printf("shardctrler: %v left", leavingGroups)
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = v
	}
	for _, gid := range leavingGroups {
		delete(newConfig.Groups, gid)
	}
	newGIDs := make([]int, 0)
	for k, _ := range newConfig.Groups {
		newGIDs = append(newGIDs, k)
	}
	sort.Ints(newGIDs)
	if len(newGIDs) == 0 {
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = 0
		}
	} else {
		shardsPerGroup := (NShards + len(newGIDs) - 1) / len(newGIDs)
		numShardsInGroup := make([]int, len(newGIDs))
		{
			tot := 0
			for i, _ := range numShardsInGroup {
				numShardsInGroup[i] = shardsPerGroup
				tot += shardsPerGroup
			}
			i := 0
			for tot > NShards {
				numShardsInGroup[i]--
				tot--
				i++
			}
		}

		idx := 0
		for i := 0; i < NShards; i++ {
			if numShardsInGroup[idx] == 0 {
				idx++
			}
			newConfig.Shards[i] = newGIDs[idx]
			numShardsInGroup[idx]--
		}
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) handleMove(shard int, GID int) {
	// log.Printf("shardctrler: %d moved to %d", shard, GID)
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = v
	}
	for i, gid := range oldConfig.Shards {
		newConfig.Shards[i] = gid
	}

	newConfig.Shards[shard] = GID
	sc.configs = append(sc.configs, newConfig)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	labgob.Register([]int{})
	labgob.Register(MoveInfo{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.isKilled = 0
	sc.lastApplied = 0
	sc.lastClientCommandReply = make(map[int64]InternalResp)
	sc.ReplyWaitChan = make(map[string]chan *InternalResp)

	for _, entry := range sc.rf.Log {
		if entry.Index == 0 {
			continue
		}
		if entry.Index > sc.rf.LastApplied {
			break
		}
		if entry.Index > sc.lastApplied {
			log.Fatalf("No snapshotting in shardctrler")
		}
		op, _ := entry.Command.(Op)
		sc.lastApplied = entry.Index
		lastClientReply, ok := sc.lastClientCommandReply[op.ClientUuid]
		var reply Config
		isValid := true
		if !ok || lastClientReply.ReqNumber < op.ClientReqNo {
			// apply
			if op.Type == JOIN {
				newGroups, _ := op.Args.(map[int][]string)
				sc.handleJoin(newGroups)
			} else if op.Type == LEAVE {
				leavingGroups, _ := op.Args.([]int)
				sc.handleLeave(leavingGroups)
			} else if op.Type == MOVE {
				args, _ := op.Args.(MoveArgs)
				sc.handleMove(args.Shard, args.GID)
			} else {
				idx, _ := op.Args.(int)
				if idx == -1 || idx >= len(sc.configs) {
					idx = len(sc.configs) - 1
				}
				reply = sc.configs[idx]
			}
			isValid = true
			sc.lastClientCommandReply[op.ClientUuid] = InternalResp{
				ReqNumber: op.ClientReqNo,
				Reply:     reply,
				Valid:     isValid,
			}
		}
	}

	go sc.applyHandler()
	return sc
}
