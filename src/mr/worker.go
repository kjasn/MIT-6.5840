package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args, reply := &TaskArgs{Status: true}, &TaskReply{}
	flag := true
	for flag {
		switch reply.Phrase {
		case MapTask:
			call("Coordinator.TaskAssign", args, reply)
			go doMapTask(args, reply, mapf)
			log.Printf(">>> after a map task: %#v", reply)
		case ReduceTask:
			call("Coordinator.TaskAssign", args, reply)
			go doReduceTask(args, reply, reducef)
		default:
			os.Exit(0)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func readFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v, err: %s", filename, err.Error())
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func writeToLocalFiles(
	reply *TaskReply,
	mapf func(string, string) []KeyValue,
	done chan struct{},
) {
	readFrom := (*reply.Task.FileSlice)[0]
	content := readFile(readFrom)
	intermediate := mapf(readFrom, string(content))

	sort.Sort(ByKey(intermediate))
	// write intermediate kvs to local file
	// buff := map[int]ByKey{}
	nReduce := reply.Task.NReduce
	// store temp kvs to write to temp files
	buff := make([][]ByKey, nReduce)

	for _, kv := range intermediate {
		idx := ihash(kv.Key) % nReduce
		buff[idx] = append(buff[idx], ByKey{kv})
	}

	for i := 0; i < nReduce; i++ {
		// mr-tmp-taskID-seqNum
		oname := "mr-tmp-" + strconv.Itoa(reply.Task.TaskID) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		// write to temp file in json style
		enc := json.NewEncoder(ofile)
		for _, kv := range buff[i] {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}
	// successfully finish
	done <- struct{}{}
	// log.Printf(">>> Worker: after write: %#v", *reply.Task)
}

func doMapTask(args *TaskArgs, reply *TaskReply,
	mapf func(string, string) []KeyValue) {

	done := make(chan struct{})
	go writeToLocalFiles(reply, mapf, done)

	// check if time out and call feedback to tell coordinator
	timer(args, reply, done)
}

func doReduceTask(args *TaskArgs, reply *TaskReply,
	reducef func(string, []string) string) {

	done := make(chan struct{})
	go reduceEmit(reply, reducef, done)

	// check if time out and call feedback to tell coordinator
	timer(args, reply, done)
}

func reduceEmit(reply *TaskReply,
	reducef func(string, []string) string,
	done chan struct{},
) {

	// create dest file
	oname := fmt.Sprintf("mr-out-%d", reply.Task.TaskID)
	destFile, _ := os.Create(oname)
	defer destFile.Close()

	// read from tmp files, get sorted kvs
	intermediate := shuffle(*reply.Task.FileSlice)
	// write to dest file
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
		fmt.Fprintf(destFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	done <- struct{}{}
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func timer(args *TaskArgs, reply *TaskReply, done chan struct{}) {
	// max 10s
	select {
	case <-done:
		reply.Task.Status = taskCompleted
	case <-time.After(TIMEOUT):
		// task fail as time out
		log.Println(">>> times out")
		reply.Task.Status = taskPending
		// add fail task to TaskQue
		// TODO
		args.Status = false // task failed, worker crashed
	}

	// log.Printf(">>> Worker: before call back, reply's task is: %#v", *reply.Task)
	args.Task = reply.Task
	call("Coordinator.TaskFeedback", args, reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
