package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	rand.Seed(time.Now().UnixNano())
	myId := int32(rand.Intn(1000000))
	for {
		work := WorkRequest{}
		ok := call("Coordinator.GetWork", &WorkerId{myId}, &work)
		if ok {
			log.Printf("Worker %d got work: %v", myId, work)
		} else {
			log.Printf("Worker %d failed to get work", myId)
			return
		}

		submission := WorkReply{}

		if work.Type == MapTaskType {
			MapTask(&work, &submission, myId, mapf)
		} else if work.Type == ReduceTaskType {
			ReduceTask(&work, &submission, myId, reducef)
		} else {
			return
		}

		submissionResp := SubmitResponse{}

		log.Printf("submitting work %v", submission)
		ok = call("Coordinator.SubmitWork", submission, &submissionResp)
		if ok {
			log.Printf("Worker %d got reply: %v", myId, submissionResp)
		} else {
			log.Printf("Worker %d failed to get reply", myId)
			return
		}

	}
}

func MapTask(workRequest *WorkRequest, workSubmission *WorkReply,
	myId int32, mapf func(string, string) []KeyValue) {

	reduceOutputKvs := make([][]KeyValue, workRequest.NReduce)
	workSubmission.WorkerId = myId
	workSubmission.TaskId = workRequest.TaskId
	workSubmission.Type = workRequest.Type
	workSubmission.OutputFiles = make([]string, workRequest.NReduce)

	for i := range reduceOutputKvs {
		reduceOutputKvs[i] = make([]KeyValue, 0)
	}

	for _, filename := range workRequest.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			bucketIdx := ihash(kv.Key) % workRequest.NReduce
			reduceOutputKvs[bucketIdx] = append(reduceOutputKvs[bucketIdx], kv)
		}
	}
	for reduceOutputNo, kvs := range reduceOutputKvs {
		file, err := ioutil.TempFile("", "mr-temp-"+strconv.Itoa(int(myId)))
		if err != nil {
			log.Fatalf("failed to create temp file: %s %v", string(myId), err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("failed to write intermediate kv pair")
			}
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("error closing temp file")
		}
		opFileName := "mr-" + strconv.Itoa(int(workRequest.TaskId)) + "-" + strconv.Itoa(reduceOutputNo)
		err = os.Rename(file.Name(), opFileName)
		if err != nil {
			log.Fatalf("failed rename temp file")
		}
		workSubmission.OutputFiles[reduceOutputNo] = opFileName
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ReduceTask(workRequest *WorkRequest, workSubmission *WorkReply,
	myId int32, reducef func(string, []string) string) {

	workSubmission.WorkerId = myId
	workSubmission.TaskId = workRequest.TaskId
	workSubmission.Type = workRequest.Type
	workSubmission.OutputFiles = make([]string, 1)

	intermediate := []KeyValue{}

	for _, filename := range workRequest.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	opFileName := "mr-out-" + strconv.Itoa(workRequest.ReduceBucketNo)

	file, err := ioutil.TempFile("", "mr-temp-"+strconv.Itoa(int(myId)))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("error closing temp file")
	}

	err = os.Rename(file.Name(), opFileName)
	if err != nil {
		log.Fatalf("failed rename temp file")
	}

	workSubmission.OutputFiles[0] = opFileName
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
