package kvraft

import (
	"6.824/labrpc"
	"log"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	uuid       int64
	reqNumber  int32
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
	ck.uuid = nrand()
	ck.reqNumber = 0
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

	reqNo := atomic.AddInt32(&ck.reqNumber, 1)
	req := GetArgs{
		Key:           key,
		ClientId:      ck.uuid,
		RequestNumber: reqNo,
	}

	log.Printf("Clerk %v, reqno %v: sending req get: %v", req.ClientId, req.RequestNumber, req.Key)

	doneChan := make(chan GetReply, 20)
	sendReq := func(to int) {
		//log.Printf("clerk %v, reqno %v, sending req to %d", req.ClientId, req.RequestNumber, to)
		reply := GetReply{}
		ok := ck.servers[to].Call("KVServer.Get", &req, &reply)
		if ok {
			doneChan <- reply
		}
	}
	go sendReq(ck.lastLeader)

	for {
		select {
		case getReply := <-doneChan:
			if getReply.Err != OK {
				ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
				go sendReq(ck.lastLeader)
			} else {
				log.Printf("Clerk %v, reqno %v: [got reply] got: %v = %v", req.ClientId, req.RequestNumber, req.Key, getReply.Value)
				return getReply.Value
			}

		case <-time.After(time.Millisecond * 200):
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			go sendReq(ck.lastLeader)
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
	reqNo := atomic.AddInt32(&ck.reqNumber, 1)
	req := PutAppendArgs{
		Key:           key,
		Value:         value,
		Op:            op,
		ClientId:      ck.uuid,
		RequestNumber: reqNo,
	}
	log.Printf("Clerk %v, reqno %v: sending req: %v, %v, %v", req.ClientId, req.RequestNumber, op, key, value)
	doneChan := make(chan PutAppendReply, 20)
	sendReq := func(to int) {
		//log.Printf("clerk %v, reqno %v, sending req to %d", req.ClientId, req.RequestNumber, to)
		reply := PutAppendReply{}
		ok := ck.servers[to].Call("KVServer.PutAppend", &req, &reply)
		//log.Printf("get reply(%v, %v) from to %d", ok, reply, to)
		if ok {
			doneChan <- reply
		}
	}

	go sendReq(ck.lastLeader)

	for {
		select {
		case getReply := <-doneChan:
			if getReply.Err != OK {
				ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
				go sendReq(ck.lastLeader)
			} else {
				log.Printf("Clerk %v, reqno %v:, %v, %v, %v, done", req.ClientId, req.RequestNumber, op, key, value)
				return
			}

		case <-time.After(time.Millisecond * 200):
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			go sendReq(ck.lastLeader)
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
