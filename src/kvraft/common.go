package kvraft

import "strconv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOldRequest  = "ErrOldRequest"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key           string
	Value         string
	Op            string // "Put" or "Append"
	ClientId      int64
	RequestNumber int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key           string
	ClientId      int64
	RequestNumber int32
}

type GetReply struct {
	Err   Err
	Value string
}

func termIndexToString(term int, index int) string {
	return strconv.Itoa(term) + "." + strconv.Itoa(index)
}
