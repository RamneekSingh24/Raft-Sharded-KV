package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync/atomic"
)
import "time"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	uuid      int64
	reqNumber int32
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.reqNumber = 0
	ck.uuid = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:           num,
		ClientId:      ck.uuid,
		RequestNumber: atomic.AddInt32(&ck.reqNumber, 1),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:       servers,
		ClientId:      ck.uuid,
		RequestNumber: atomic.AddInt32(&ck.reqNumber, 1),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:          gids,
		ClientId:      ck.uuid,
		RequestNumber: atomic.AddInt32(&ck.reqNumber, 1),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:         shard,
		GID:           gid,
		ClientId:      ck.uuid,
		RequestNumber: atomic.AddInt32(&ck.reqNumber, 1),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
