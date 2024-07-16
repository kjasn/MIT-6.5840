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

const timeOut = 10 * time.Second

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
	TaskID int
	Type TaskType
	Status TaskStatus
	FileName string
	NReduce int
}

type Coordinator struct {
	// Your definitions here.
	Files []string	// file name
	Mutex *sync.Mutex
	TaskQue chan *Task	// store all tasks
}

// func pickUpPendingTask(tasks map[int]*Task) *Task {
// 	for _, task := range tasks {
// 		if task.Status == taskPending {	// select pending task
// 			task.Status = taskAssigned
// 			// log.Printf("pick up a task: %#v", *task)
// 			return task
// 		}
// 	}
// 	return nil 
// }

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskAssign(args *TaskArgs, reply *TaskReply) error {
	if len(c.TaskQue) == 0 {
		return fmt.Errorf("No more task to distribute")
	}
	task := <- c.TaskQue
	task.Status = taskAssigned
	reply.Task = task
	return nil
}

// func (c *Coordinator) MapTaskAssign(args *MapArgs, reply *MapReply) error {
// 	// pick up a pending task
// 	c.mutex.Lock()
// 	defer c.mutex.Unlock()

// 	if c.remainMapTasks != 0 {	// include assigned task
// 		task := pickUpPendingTask(c.mapTasks)
// 		if task == nil {
// 			return fmt.Errorf("no more pending map tasks to pick up")
// 		}
// 		// task.WorkerID = &args.WorkerID
// 		task.WorkerID = args.WorkerID
// 		reply.Task = task
// 		reply.remainMapTasks = c.remainMapTasks
// 		args.TaskID = task.TaskID
// 		// log.Printf(">>> Coordinator: distribute reply {%#v, %#v}", *reply.Task, reply.remainMapTasks)
// 		return nil

// 		// for _, task := range c.mapTasks {
// 		// 	if task.Status == taskPending {	// select pending task
// 		// 		task.WorkerID = &args.WorkerID
// 		// 		reply.Task = task
// 		// 		args.TaskID = &task.TaskID
// 		// 		return nil
// 		// 	}
// 		// }
// 	}

// 	return fmt.Errorf("no more pending map tasks to pick up")
// }

// func (c *Coordinator) MapTaskFeedback(args *MapArgs, reply *MapReply) error {
// 	c.mutex.Lock()
// 	defer c.mutex.Unlock()

// 	// log.Printf(">>> map feed back: arg: %#v ", *args)
// 	task := c.mapTasks[args.TaskID] 
// 	task.Status = args.TaskStatus
// 	// task.WorkerID = nil
// 	task.WorkerID = -1
// 	if task.Status == taskCompleted {
// 		c.remainMapTasks--
// 		// set reduce task
// 		reduceTaskID := generateID()
// 		c.reduceTasks[reduceTaskID] = &Task{
// 			TaskID: reduceTaskID,
// 			Status: taskPending,
// 			Filename: fmt.Sprintf("map-out-%d-%d", args.WorkerID, args.TaskID),
// 			// WorkerID: nil,
// 			WorkerID: -1,
// 		}
// 		// log.Printf(">>> map feedback: add a reduce task: %+v", *c.reduceTasks[reduceTaskID])
// 		c.remainReduceTasks ++
// 	} 

// 	return nil
// }


// func (c *Coordinator) ReduceTaskAssign(args *ReduceArgs, reply *ReduceReply) error {
// 	c.mutex.Lock()
// 	defer c.mutex.Unlock()

// 	if c.remainReduceTasks != 0 {	// include assigned task
// 		task := pickUpPendingTask(c.reduceTasks)
// 		if task == nil {
// 			return fmt.Errorf("no more pending reduce tasks to pick up")
// 		}
// 		task.WorkerID = args.WorkerID
// 		reply.Task = task
// 		args.TaskID = task.TaskID
// 	}
// 	return nil
// }

// func (c *Coordinator) ReduceTaskFeedback(args *ReduceArgs, reply *ReduceReply) error {
// 	c.mutex.Lock()
// 	defer c.mutex.Unlock()

// 	// log.Printf("reduce feedback: %+v", *args)
// 	task := c.reduceTasks[args.TaskID] 
// 	// log.Printf("reduce feedback: %+v", *task)
// 	task.Status = args.TaskStatus
// 	// task.WorkerID = nil
// 	task.WorkerID = -1
// 	if task.Status == taskCompleted {
// 		c.remainReduceTasks--
// 	}

// 	return nil
// }
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
// if the entire job has taskCompleted.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if len(c.TaskQue) == 0 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files: files,
		Mutex: &sync.Mutex{},
		TaskQue: make(chan *Task, len(files)),
	}
	// add map tasks
	for i, file := range c.Files {
		task := &Task{
			Type: MapTask,
			TaskID: i,
			FileName: file,
			Status: taskPending,
		}
		c.TaskQue <- task
		// log.Printf("make coordinator add a map task %#v", *c.mapTasks[mapTaskID])
	}

	c.server()
	return &c
}
