package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"log"
	"sync/atomic"
)
import "6.824/shardctrler"
import "time"

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	uuid      int64
	reqNumber int32
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	ck.uuid = nrand()
	ck.reqNumber = 0
	ck.config = ck.sm.Query(-1)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {

	args := GetArgs{
		Key:           key,
		ClientId:      ck.uuid,
		RequestNumber: atomic.AddInt32(&ck.reqNumber, 1),
		ConfigNumber:  ck.config.Num,
	}
	log.Printf("clerk %d, req no: %d, get %v", ck.uuid, args.RequestNumber, key)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					log.Printf("clerk %d, req no: %d,[Got reply] key %v = %v (gid: %d)",
						ck.uuid, args.RequestNumber, key, reply.Value, gid)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		args.ConfigNumber = ck.config.Num
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:           key,
		Value:         value,
		Op:            op,
		ClientId:      ck.uuid,
		RequestNumber: atomic.AddInt32(&ck.reqNumber, 1),
		ConfigNumber:  ck.config.Num,
	}
	args.Key = key
	args.Value = value
	args.Op = op

	log.Printf("clerk %d, req no: %d, %v %v, %v", ck.uuid, args.RequestNumber, op, key, value)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					log.Printf("clerk %d, req no: %d,[Got reply] %v %v, %v done(gid %d)",
						ck.uuid, args.RequestNumber, op, key, value, gid)

					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		args.ConfigNumber = ck.config.Num
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
