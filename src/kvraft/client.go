package kvraft

import (
	"6.824/labrpc"
	"log"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	log.Printf("Clerk: got req get: %v", key)
	for {
		toSend := atomic.LoadInt32(&ck.lastLeader)
		req := GetArgs{Key: key}
		reply := GetReply{}
		ok := ck.servers[toSend].Call("KVServer.Get", &req, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				nextToSend := (toSend + 1) % int32(len(ck.servers))
				atomic.CompareAndSwapInt32(&ck.lastLeader, toSend, nextToSend)
			} else {
				log.Printf("Clerk: got get reply: key %v =  %v", key, reply.Value)
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	for {
		toSend := atomic.LoadInt32(&ck.lastLeader)
		req := PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
		}
		reply := PutAppendReply{}
		ok := ck.servers[toSend].Call("KVServer.PutAppend", &req, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				nextToSend := (toSend + 1) % int32(len(ck.servers))
				atomic.CompareAndSwapInt32(&ck.lastLeader, toSend, nextToSend)
			} else {
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	log.Printf("Clerk: got req: put %v = %v", key, value)
	ck.PutAppend(key, value, "Put")
	log.Printf("Clerk: put req done: put %v = %v", key, value)
}
func (ck *Clerk) Append(key string, value string) {
	log.Printf("Clerk: got req: append %v, %v", key, value)
	ck.PutAppend(key, value, "Append")
	log.Printf("Clerk: apend req done: append %v, %v", key, value)

}
