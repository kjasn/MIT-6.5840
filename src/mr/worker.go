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
	for {
		args, reply := &TaskArgs{Status: true}, &TaskReply{Phrase: MapPhrase} // init
		// get tasks, return a task OR enter TaskWaiting
		call("Coordinator.TaskAssign", args, reply)

		switch reply.Phrase {
		case MapPhrase:
			go doMapTask(args, reply, mapf)
		case ReducePhrase:
			go doReduceTask(args, reply, reducef)
		case TaskWaiting:
			// log.Println("[INFO] In progressing...")
			time.Sleep(1 * time.Second)
		case AllDone:
			os.Exit(0)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func writeToLocalFiles(
	reply *TaskReply,
	mapf func(string, string) []KeyValue,
	done chan struct{},
) {

	readFrom := reply.Task.FileSlice[0]
	file, err := os.Open(readFrom)
	if err != nil {
		log.Fatalf("cannot open %v", readFrom)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", readFrom)
	}
	file.Close()

	intermediate := mapf(readFrom, string(content))

	// sort.Sort(ByKey(intermediate))
	// write intermediate kvs to local file
	nReduce := reply.Task.NReduce
	// store temp kvs to write to temp files
	buff := make([][]KeyValue, nReduce)

	for _, kv := range intermediate {
		idx := ihash(kv.Key) % nReduce
		buff[idx] = append(buff[idx], kv)
	}

	for i := 0; i < nReduce; i++ {
		// mr-tmp-taskID-seqNum
		oname := "mr-tmp-" + strconv.Itoa(reply.Task.TaskID) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		// write to temp file in json style
		enc := json.NewEncoder(ofile)
		for _, kv := range buff[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("[ERROR] Fail to encode")
				return
			}
		}
		ofile.Close()
		// log.Printf(">>> Map: finish a tmp file: %s", oname)
	}
	// successfully finish
	done <- struct{}{}
}

func doMapTask(args *TaskArgs, reply *TaskReply,
	mapf func(string, string) []KeyValue,
) {

	done := make(chan struct{})
	go writeToLocalFiles(reply, mapf, done)

	// check if time out and call feedback to tell coordinator
	timer(args, reply, done)
	// log.Println("[INFO] Finished a map task")
}

func doReduceTask(args *TaskArgs, reply *TaskReply,
	reducef func(string, []string) string,
) {

	done := make(chan struct{})
	go reduceEmit(reply, reducef, done)

	// check if time out and call feedback to tell coordinator
	timer(args, reply, done)
}

func reduceEmit(reply *TaskReply,
	reducef func(string, []string) string,
	done chan struct{},
) {

	// create tmp file
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-reduce-out-*")
	if err != nil {
		log.Fatal("[ERROR] Fail to create tmp file")
	}

	// read from tmp files, get sorted kvs
	intermediate := shuffle(reply.Task.FileSlice)
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()

	// rename after done
	fn := fmt.Sprintf("mr-out-%d", reply.Task.TaskID)
	err = os.Rename(tempFile.Name(), fn)
	if err != nil {
		log.Fatalf("[ERROR] Fail to rename %s, error: %s", tempFile.Name(), err.Error())
	}
	// log.Printf("Reduce: finish %s", fn)
	done <- struct{}{}
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatalf("Fail to open %s", filepath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF { // ignore io.EOF
					break
				}
				log.Fatalf("Fail to shuffle: %s", err.Error())
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
		args.Status = true
		reply.Task.Status = taskCompleted
	case <-time.After(TIMEOUT):
		// task fail since time out
		reply.Task.Status = taskPending
		log.Printf("[INFO] Task: %#v failed", reply.Task)
		// add fail task to TaskQue
		// TODO
		args.Status = false // task failed, worker crashed
	}

	// TODO
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
