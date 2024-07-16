package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


type WorkerStatus int
const (
	workerIdle WorkerStatus = iota
	workerWorking 
	workerOffline
)
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	mapArgs, mapReply := &TaskArgs{}, &TaskReply{}
	for {
		if ok := call("Coordinator.TaskAssign", mapArgs, mapReply); !ok {
			break
		}

		go mapPhrase(mapArgs, mapReply)
	}

	// // map phrase
	// for {
	// 	// return false denotes no more map tasks
	// 	callTask(mapf)
		
	// }
	// // log.Println(">>> map finished")


	// // reduce phrase
	// reduceArgs, reduceReply := &ReduceArgs{WorkerID: mapArgs.WorkerID}, &ReduceReply{}
	// // for *(mapReply.remainMapTasks) == 0 {
	// for {
	// 	if ok := call("Coordinator.ReduceTaskAssign", reduceArgs, reduceReply); !ok {
	// 		break
	// 	}
	// 	done := make(chan struct{})
	// 	go reduceEmit(reduceReply, reducef, done)
	// 	reduceArgs.TaskID = reduceReply.Task.TaskID
	// 	select {
	// 	case <- done:
	// 		reduceArgs.TaskStatus = taskCompleted	// task finished
	// 	case <- time.After(timeOut):
	// 		// task fail as time out 
	// 		log.Println(">>> times out")
	// 		reduceArgs.TaskStatus = taskPending	// task fail, set free
	// 	}
	// 	call("Coordinator.ReduceTaskFeedback", reduceArgs, reduceReply)
	// }
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func callTask(mapf func(string, string) []KeyValue) {
	taskArgs, taskReply := &TaskArgs{}, &TaskReply{}
	if ok := call("Coordinator.TaskAssign", taskArgs, taskReply); !ok {
		log.Fatal("Worker: task distribute fail")
	}
	// log.Printf(">>> Worker: Received mapReply: %+v", mapReply.Task) // New log

	done := make(chan struct{})
	go writeToLocalFiles(taskReply, mapf, done)
	select {
	case <- done:
		taskArgs.TaskStatus = taskCompleted	// task finished
	case <- time.After(timeOut):
		// task fail as time out 
		log.Println(">>> times out")
		taskArgs.TaskStatus = taskPending	// task fail, set free
	}
	call("Coordinator.MapTaskFeedback", mapArgs, mapReply)
}

func readFile(filename string) []byte{
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

func mapPhrase(args *TaskArgs, reply *TaskReply) {
	
}

func writeToLocalFiles(taskReply *TaskReply, mapf func(string, string) []KeyValue, done chan struct{}) {
	content := readFile(taskReply.Task.FileName)
	intermediate := mapf(taskReply.Task.FileName, string(content))

	sort.Sort(ByKey(intermediate))
	// write intermediate kvs to local file
	buff := map[int]ByKey{}
	nReduce := taskReply.Task.NReduce
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % nReduce
		buff[idx] = append(buff[idx], kv)
	}

	localFiles := []string{}
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("map-tmp-%d", i)
		writeFile(i, buff[i])
		localFiles = append(localFiles, filename)
	} 
	taskReply.LocalFiles = localFiles
	// log.Println(">>> map emit: nReduce is ", nReduce)

	// create tmp dir
	// tmpDir := filepath.Join("..", "main", "tmp")
	// os.MkdirAll(tmpDir, 0755)

	// tmpFiles := []*os.File{}
	// for i := 0; i < nReduce; i++ {
	// 	oname := filepath.Join(tmpDir, fmt.Sprintf("map-out-%d-%d", mapReply.Task.WorkerID, i))
	// 	ofile, err := os.Create(oname)
	// 	if err != nil {
	// 		log.Fatalf("can not create file: %s\n", oname)
	// 	}
	// 	// log.Printf(">>> map emit: created a file %s", oname)
	// 	tmpFiles = append(tmpFiles, ofile)
	// 	defer ofile.Close()
	// }

	// // shuffle intermediate data
	// for i := 0; i < len(intermediate); i++ {
	// 	idx := ihash(intermediate[i].Key) % nReduce
	// 	ofile := tmpFiles[idx]
	// 	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, intermediate[i].Value)
	// }

	done <- struct{}{}
}

func reduceEmit(reduceReply *ReduceReply, reducef func(string, []string) string, done chan struct{}) {
	tmpDir := filepath.Dir(".")
	oname := filepath.Join(filepath.Join(tmpDir, "tmp"), reduceReply.Task.Filename)

	ofile, err := os.Open(oname)
	if err != nil {
		log.Fatalf("cannot open %v, err: %s", oname, err.Error())
	}
	defer ofile.Close()

	intermediate := make(map[string][]string)
	scanner := bufio.NewScanner(ofile)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 2 {
			key, value := parts[0], parts[1]
			intermediate[key] = append(intermediate[key], value)
		}
	}

	parts := strings.Split(oname, "-")
	if len(parts) < 4 {
		log.Fatalf("invalid filename format: %s", oname)
	}
	dest := fmt.Sprintf("mr-out-%s", parts[3])
	destFile, err := os.Create(dest)
	if err != nil {
		log.Fatal("can not create file: ", dest)
	}
	// log.Println("created a file: ", dest)
	for k, v := range intermediate {
		output := reducef(k, v)
		fmt.Fprintf(destFile, "%v %v\n", k, output)
	}
	done <- struct{}{}
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
