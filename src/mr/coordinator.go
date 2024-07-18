package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TIMEOUT = 10 * time.Second

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

// coordinator's tasks
type TaskStatus int

const (
	taskPending TaskStatus = iota
	taskAssigned
	taskCompleted
)

type Task struct {
	TaskID    int
	Type      TaskType
	Status    TaskStatus
	FileSlice *[]string // 1 -- map task, more -- reduce task
	NReduce   int
}

type Coordinator struct {
	// Your definitions here.
	Files         []string // file name
	Mutex         *sync.Mutex
	MapTaskQue    chan *Task // store map tasks
	ReduceTaskQue chan *Task // store reduce tasks
	Buff          [][]string // map reduce task id to files
	TaskID        int        // record the next id, start with 0
	NReduce       int        // count of reduce tasks
	FinishedMapTasks	int 
	FinishedReduceTasks      int        // finished reduce tasks
	Phrase        TaskType   // job phrase, map OR reduce
}

// Your code here -- RPC handlers for the worker to call.
// func (c *Coordinator) selectQue(t TaskType) (chan *Task, error) {
// 	if t == MapTask {
// 		if len(c.ReduceTaskQue) == c.NReduce { // all map done
// 			return nil, fmt.Errorf("all map tasks Done")
// 		}
// 		return c.MapTaskQue, nil
// 	} else {
// 		if c.Done() { // all (reduce) tasks done
// 			return nil, fmt.Errorf("all tasks Done")
// 		}
// 		return c.ReduceTaskQue, nil
// 	}
// }

func (c *Coordinator) TaskAssign(args *TaskArgs, reply *TaskReply) error {
	// select a task queue by task type
	// c.Mutex.Lock()
	// defer c.Mutex.Unlock()
	var task *Task

	if c.Phrase == MapTask {
		// if len(c.ReduceTaskQue) == c.NReduce { // all map done
		if c.FinishedMapTasks == len(c.Files){
			return fmt.Errorf("all map tasks Done")
		}
		task = <-c.MapTaskQue
	} else {
		if c.Done() { // all (reduce) tasks done
			return fmt.Errorf("all tasks Done")
		}
		task = <-c.ReduceTaskQue
	}

	// if len(q) == 0 {
	// 	return fmt.Errorf("No more task to assign")
	// }
	// if len(q) == 0, blocked
	task.Status = taskAssigned
	reply.Task = task
	reply.Phrase = c.Phrase
	// log.Printf(">>> Assign a task: %#v", *task)
	return nil
}
func (c *Coordinator) TaskFeedback(args *TaskArgs, reply *TaskReply) error {
	if !args.Status { // fail
		// add failed task back
		// TODO: delete failed worker tmp files: mr-tmp-reply.Task.TaskID-*
		if c.Phrase == MapTask {
			// c.MapTaskQue <- reply.Task
			c.MapTaskQue <- args.Task
			log.Printf(">>> Coordinator: map task fail: %#v", args.Task)
		} else {
			c.ReduceTaskQue <- args.Task
			log.Printf(">>> Coordinator: reduce task fail: %#v", args.Task)
		}
		// q, _ := c.selectQue(args.Type) // ignore err
		// q <- reply.Task
	} else { // success
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		// if args.Type == MapTask {
		if c.Phrase == MapTask {
			// set reduce task
			// fs := []string{}
			for i := 0; i < c.NReduce; i++ {
				c.Buff[i] = append(c.Buff[i], fmt.Sprintf("mr-tmp-%d-%d", args.Task.TaskID, i))
			}

			c.FinishedMapTasks++
			// if len(c.ReduceTaskQue) == c.NReduce {
			if c.FinishedMapTasks == len(c.Files) {
				c.Phrase = ReduceTask // next phrase
				log.Printf("Step into Reduce Phrase")
			}
		} else {
			c.FinishedReduceTasks++
		}

		reply.Phrase = c.Phrase
		if reply.Phrase == MapTask {
			log.Println("Current is Map Phrase")
		} else {
			log.Println("Current is Reduce Phrase")
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has taskCompleted.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.FinishedReduceTasks == c.NReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:         files,
		Mutex:         &sync.Mutex{},
		MapTaskQue:    make(chan *Task, len(files)),
		ReduceTaskQue: make(chan *Task, nReduce),
		Buff:          make([][]string, nReduce),
		TaskID:        0,
		NReduce:       nReduce,
		FinishedReduceTasks:      0,
		FinishedMapTasks: 0,
		Phrase:        MapTask,
	}

	// add map tasks
	for _, file := range c.Files {
		task := &Task{
			Type:      MapTask,
			TaskID:    c.generateID(),
			FileSlice: &[]string{file}, // 1 file
			Status:    taskPending,
			NReduce: c.NReduce,
		}
		c.MapTaskQue <- task
		// log.Printf("make coordinator add a map task %#v", *c.mapTasks[mapTaskID])
	}

	// add reduce tasks
	for i := 0; i < c.NReduce; i++ {
		task := &Task{
			TaskID:    c.generateID(),
			Type:      ReduceTask,
			Status:    taskPending,
			FileSlice: &c.Buff[i],
		}
		c.ReduceTaskQue <- task
	}

	c.server()
	return &c
}
