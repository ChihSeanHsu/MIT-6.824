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

type taskStatus struct {
	NodeID    string
	StartTime int64
}

const (
	Timeout int64 = 10
)

type Coordinator struct {
	// Your definitions here.
	intermediates    [][]string
	inputs           []string
	mapTasks         []int
	reduceTasks      []int
	mapDoingTasks    map[int]taskStatus
	reduceDoingTasks map[int]taskStatus
	mapDone          bool
	lock             sync.Mutex
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
	return len(c.reduceTasks) == 0 && len(c.reduceDoingTasks) == 0
}

func (c *Coordinator) checkMapDone() bool {
	if c.mapDone {
		return true
	}
	c.mapDone = len(c.mapDoingTasks) == 0 && len(c.mapTasks) == 0
	return c.mapDone
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	fmt.Printf("Node %s income\n", args.NodeID)
	now := time.Now().Unix()
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.checkMapDone() {
		if len(c.reduceTasks) == 0 && len(c.reduceDoingTasks) == 0 {
			reply.TaskType = NoTask
		} else if len(c.reduceTasks) == 0 {
			reply.TaskType = WaitTask
		} else {
			reply.TaskType = ReduceTask
			right := len(c.reduceTasks) - 1
			for len(c.intermediates[c.reduceTasks[right]]) == 0 {
				right--
			}
			task := c.reduceTasks[right]
			reply.Files = append(reply.Files, c.intermediates[task]...)
			reply.ID = task

			status := taskStatus{
				NodeID:    args.NodeID,
				StartTime: now,
			}
			c.reduceDoingTasks[task] = status
			c.reduceTasks = c.reduceTasks[:right]
			fmt.Printf("deliver reduce task %d to %s\n", task, args.NodeID)
		}
	} else {
		if len(c.mapTasks) != 0 {
			reply.TaskType = MapTask
			task := c.mapTasks[len(c.mapTasks)-1]
			reply.Files = append(reply.Files, c.inputs[task])
			reply.ID = task
			reply.ReduceCount = len(c.reduceTasks)

			status := taskStatus{
				NodeID:    args.NodeID,
				StartTime: now,
			}
			c.mapDoingTasks[task] = status
			c.mapTasks = c.mapTasks[:len(c.mapTasks)-1]
			fmt.Printf("deliver map task %d %s to %s\n", task, c.inputs[task], args.NodeID)
		} else {
			reply.TaskType = WaitTask
		}
	}
	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.TaskType == MapTask {
		if _, ok := c.mapDoingTasks[args.ID]; !ok {
			fmt.Printf("map task done %d but timeout \n", args.ID)
			return nil
		}
		for i, files := range args.Files {
			c.intermediates[i] = append(c.intermediates[i], files)
		}
		delete(c.mapDoingTasks, args.ID)
		fmt.Printf("map task done %d \n", args.ID)
	} else {
		delete(c.reduceDoingTasks, args.ID)
		fmt.Printf("reduce task done %d \n", args.ID)
	}
	return nil
}

func (c *Coordinator) CheckTimeout() {
	for {
		c.lock.Lock()
		if len(c.mapDoingTasks) == 0 && len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 && len(c.reduceDoingTasks) == 0 {
			c.lock.Unlock()
			break
		}
		now := time.Now().Unix()
		for key, value := range c.mapDoingTasks {
			if now-value.StartTime > Timeout {
				fmt.Printf("map task timeout %d \n", key)
				delete(c.mapDoingTasks, key)
				c.mapTasks = append(c.mapTasks, key)
			}
		}
		for key, value := range c.reduceDoingTasks {
			if now-value.StartTime > Timeout {
				fmt.Printf("reduce task timeout %d \n", key)
				delete(c.reduceDoingTasks, key)
				c.mapTasks = append(c.reduceTasks, key)
			}
		}
		c.lock.Unlock()
		time.Sleep(time.Second)
	}
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
	for i := 0; i < len(files); i++ {
		c.mapTasks = append(c.mapTasks, i)
	}
	c.mapDoingTasks = make(map[int]taskStatus, len(files))

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, i)
	}
	c.reduceDoingTasks = make(map[int]taskStatus, nReduce)
	c.intermediates = make([][]string, nReduce)

	c.lock = sync.Mutex{}

	c.server()
	go c.CheckTimeout()
	return &c
}
