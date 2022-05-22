package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	MapTaskType    = 0
	ReduceTaskType = 1
	NoWorkType     = 2
)

type WorkRequest struct {
	TaskId         int32
	Type           int
	NReduce        int
	ReduceBucketNo int
	InputFiles     []string
}

type WorkReply struct {
	TaskId      int32
	Type        int
	WorkerId    int32
	OutputFiles []string
}

type WorkerId struct {
	Id int32
}

type SubmitResponse struct {
	Ok bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
