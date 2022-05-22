package mr

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	JobDoneChan          chan bool
	TaskChan             chan WorkRequest
	TaskDoneChans        map[int32]chan WorkReply
	IntermediateWorkChan chan WorkReply
	DsLock               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetWork(args *WorkerId, reply *WorkRequest) error {
	request := <-c.TaskChan
	*reply = request
	//reply.TaskId = request.TaskId
	//reply.nReduce = request.nReduce
	//reply.Type = request.Type
	//reply.nReduce = request.nReduce
	//reply.ReduceBucketNo = request.ReduceBucketNo
	//reply.InputFiles = request.InputFiles
	log.Printf("Sent task %v to worker %d", reply, args.Id)
	return nil
}

func (c *Coordinator) SubmitWork(args *WorkReply, reply *SubmitResponse) error {
	log.Printf("got work submission %v", *args)
	c.DsLock.Lock()
	respChan := c.TaskDoneChans[args.TaskId]
	c.DsLock.Unlock()
	respChan <- *args
	reply.Ok = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := <-c.JobDoneChan
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	nMap := len(files)
	c.JobDoneChan = make(chan bool, 1)
	c.TaskChan = make(chan WorkRequest, 2*(nMap+nReduce))
	c.TaskDoneChans = make(map[int32]chan WorkReply)
	c.IntermediateWorkChan = make(chan WorkReply, nMap)

	c.server()

	go func() {

		taskHandler := func(task WorkRequest) {
			var respChan chan WorkReply
			c.DsLock.Lock()
			respChan = make(chan WorkReply, 1)
			c.TaskDoneChans[task.TaskId] = respChan
			c.DsLock.Unlock()
			c.TaskChan <- task

			for {
				select {
				case res := <-respChan:
					c.IntermediateWorkChan <- res
					c.DsLock.Lock()
					delete(c.TaskDoneChans, task.TaskId)
					c.DsLock.Unlock()
					return
				case <-time.After(10 * time.Second):
					log.Printf("Task %d timed out", task.TaskId)
					c.TaskChan <- task
				}
			}
		}

		for idx, file := range files {
			task := WorkRequest{
				TaskId:         int32(idx),
				Type:           MapTaskType,
				NReduce:        nReduce,
				ReduceBucketNo: 0,
				InputFiles:     []string{file},
			}
			go taskHandler(task)
		}

		reduceTaskFiles := make([][]string, nReduce)
		for i := 0; i < nReduce; i++ {
			reduceTaskFiles[i] = make([]string, 0)
		}

		for i := 0; i < nMap; i++ {
			res := <-c.IntermediateWorkChan
			for _, file := range res.OutputFiles {
				args := strings.Split(file, "-")
				if len(args) != 3 {
					panic("got unexpected file name")
				}
				reduceTaskNo, err := strconv.Atoi(args[2])
				if err != nil {
					panic("got unexpected file name")
				}
				reduceTaskFiles[reduceTaskNo] = append(reduceTaskFiles[reduceTaskNo], file)
			}
		}

		if len(c.IntermediateWorkChan) != 0 {
			panic("Intermediate Work chan empty")
		}

		for len(c.TaskChan) > 0 {
			<-c.TaskChan
		}

		for i := 0; i < nReduce; i++ {
			task := WorkRequest{
				TaskId:         int32(i + nReduce),
				Type:           ReduceTaskType,
				InputFiles:     reduceTaskFiles[i],
				ReduceBucketNo: i,
			}
			go taskHandler(task)
		}

		for i := 0; i < nReduce; i++ {
			res := <-c.IntermediateWorkChan
			if len(res.OutputFiles) != 1 {
				panic("Got unexpected reduce output")
			}
		}
		c.JobDoneChan <- true

	}()

	return &c
}
