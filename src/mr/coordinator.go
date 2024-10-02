package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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

type TaskPhrase int

const (
	MapPhrase TaskPhrase = iota
	ReducePhrase
	TaskWaiting
	AllDone
)

type Task struct {
	TaskID    int
	Type      TaskType
	Status    TaskStatus
	FileSlice []string // 1 -- map task, more -- reduce task
	NReduce   int
}

type Coordinator struct {
	// Your definitions here.
	Files []string // file name
	// Mutex sync.Mutex
	Mutex               sync.RWMutex
	MapTaskQue          chan *Task // store map tasks
	ReduceTaskQue       chan *Task // store reduce tasks
	TaskID              int        // record the next id, start with 0
	NReduce             int        // count of reduce tasks
	FinishedMapTasks    int
	FinishedReduceTasks int        // finished reduce tasks
	Phrase              TaskPhrase // job phrase, map, reduce OR all done
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskAssign(args *TaskArgs, reply *TaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	reply.Phrase = c.Phrase
	switch c.Phrase {
	case MapPhrase:
		if len(c.MapTaskQue) > 0 {
			reply.Task = <-c.MapTaskQue
		} else {
			reply.Phrase = TaskWaiting
		}

	case ReducePhrase:
		if len(c.ReduceTaskQue) > 0 {
			reply.Task = <-c.ReduceTaskQue
		} else {
			reply.Phrase = TaskWaiting
		}
	case AllDone:
		reply.Phrase = AllDone
	}

	return nil
}

// add reduce tasks to reduce task queue after all map done
func (c *Coordinator) addReduceTask(seq int) {
	var s []string
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	for _, file := range files {
		// collect all tmp file by map task,  mr-tmp-*-{seq}
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(seq)) {
			s = append(s, file.Name())
		}
	}
	task := &Task{
		TaskID:    c.generateID(),
		Type:      ReduceTask,
		Status:    taskPending,
		FileSlice: s,
		NReduce:   c.NReduce,
	}
	c.ReduceTaskQue <- task
}

func (c *Coordinator) TaskFeedback(args *TaskArgs, reply *TaskReply) error {
	if !args.Status { // fail
		// add failed task back
		if c.Phrase == MapPhrase {
			c.MapTaskQue <- reply.Task
			log.Printf(">>> Coordinator: map task fail: %#v", reply.Task)
		} else {
			c.ReduceTaskQue <- reply.Task
			log.Printf(">>> Coordinator: reduce task fail: %#v", reply.Task)
		}
	} else { // success
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		if c.Phrase == MapPhrase {
			c.FinishedMapTasks++
			if c.FinishedMapTasks == len(c.Files) {
				// add reduce tasks
				for i := 0; i < c.NReduce; i++ {
					c.addReduceTask(i)
				}
				c.Phrase = ReducePhrase // next phrase
				// log.Println("[INFO] Map tasks all done, enter into reduce phrase")
			}
		} else {
			c.FinishedReduceTasks++
			if c.FinishedReduceTasks == c.NReduce {
				log.Println("[INFO] All done~")
			}
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
	c.Mutex.RLock()
	defer c.Mutex.RUnlock()
	// c.Mutex.Lock()
	// defer c.Mutex.Unlock()
	return c.FinishedReduceTasks == c.NReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files: files,
		// Mutex:               sync.RWMutex{},
		Mutex:               sync.RWMutex{},
		MapTaskQue:          make(chan *Task, len(files)),
		ReduceTaskQue:       make(chan *Task, nReduce),
		TaskID:              0,
		NReduce:             nReduce,
		FinishedReduceTasks: 0,
		FinishedMapTasks:    0,
		Phrase:              MapPhrase,
	}

	// add map tasks
	for _, file := range c.Files {
		task := &Task{
			TaskID:    c.generateID(),
			Type:      MapTask,
			Status:    taskPending,
			FileSlice: []string{file}, // 1 file
			NReduce:   c.NReduce,
		}
		c.MapTaskQue <- task
	}

	c.server()
	return &c
}

// generate UNIQUE ID
func (c *Coordinator) generateID() int {
	ret := c.TaskID
	c.TaskID++
	return ret
}
