package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type status int

const (
	taskReady status = iota
	taskQueue
	taskRunning
	taskDone
)

type taskStatus struct {
	NodeID    string
	StartTime int64
	Status    status
}

const (
	Timeout int64 = 5
)

type phase int

const (
	mapPhase phase = iota
	reducePhase
)

type Coordinator struct {
	// Your definitions here.
	intermediates [][]string
	inputs        []string
	nReduce       int
	taskChan      chan int
	tasks         []taskStatus
	phase         phase
	done          bool
	lock          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// Example
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.done
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := <-c.taskChan
	c.lock.Lock()
	defer c.lock.Unlock()
	reply.ID = task
	reply.ReduceCount = c.nReduce
	if c.phase == reducePhase {
		reply.TaskType = ReduceTask
		reply.Files = append(reply.Files, c.intermediates[task]...)
		fmt.Printf("deliver reduce task %d to %s\n", task, args.NodeID)
	} else {
		reply.TaskType = MapTask
		reply.Files = append(reply.Files, c.inputs[task])
		fmt.Printf("deliver map task %d %s to %s\n", task, c.inputs[task], args.NodeID)
	}
	c.tasks[task].Status = taskRunning
	c.tasks[task].NodeID = args.NodeID
	c.tasks[task].StartTime = time.Now().Unix()
	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.tasks[args.ID].NodeID != args.NodeID {
		fmt.Printf("Task done %d but timeout \n", args.ID)
		return nil
	}

	if args.TaskType == MapTask {
		if c.phase != mapPhase {
			return nil
		}
		for i, file := range args.Files {
			if file == "" {
				continue
			}
			c.intermediates[i] = append(c.intermediates[i], file)
		}
		fmt.Printf("map task done %d \n", args.ID)
	} else {
		fmt.Printf("reduce task done %d \n", args.ID)
	}
	c.tasks[args.ID].Status = taskDone
	go c.check()
	return nil
}

func (c *Coordinator) check() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.done {
		return
	}
	finish := true
	for i, _ := range c.tasks {
		switch c.tasks[i].Status {
		case taskReady:
			finish = false
			c.tasks[i].Status = taskQueue
			c.taskChan <- i
		case taskQueue:
			finish = false
		case taskRunning:
			finish = false
			if time.Now().Unix()-c.tasks[i].StartTime > Timeout {
				c.tasks[i].Status = taskQueue
				c.taskChan <- i
			}
		}
	}
	if finish {
		if c.phase == mapPhase {
			c.initReduceTasks()
		} else {
			c.done = true
		}
	}
}

func (c *Coordinator) Check() {
	for !c.Done() {
		c.check()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) initMapTasks() {
	c.tasks = make([]taskStatus, len(c.inputs))
	c.phase = mapPhase
}

func (c *Coordinator) initReduceTasks() {
	fmt.Println("Start reduce")
	c.tasks = make([]taskStatus, c.nReduce)
	c.phase = reducePhase
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputs = files
	c.nReduce = nReduce
	c.lock = sync.Mutex{}
	c.intermediates = make([][]string, nReduce)
	c.initMapTasks()
	if nReduce > len(files) {
		c.taskChan = make(chan int, nReduce)
	} else {
		c.taskChan = make(chan int, len(c.inputs))
	}

	c.server()
	go c.Check()
	return &c
}
