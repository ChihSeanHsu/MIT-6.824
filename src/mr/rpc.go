package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int8

const (
	WaitTask TaskType = -1
	NoTask   TaskType = iota
	MapTask
	ReduceTask
)

type GetTaskArgs struct {
	NodeID string
}

type GetTaskReply struct {
	TaskType
	Files       []string
	ID          int
	ReduceCount int
}

type TaskCompleteArgs struct {
	NodeID string
	TaskType
	ID    int
	Files []string
}

type TaskCompleteReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
