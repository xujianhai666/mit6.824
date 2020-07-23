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
type TaskRequest struct {
	WorkerID int64
}

type TaskResponse struct {
	UniqueID    string
	TaskType    TaskType
	MapTasks    MapTaskResponse
	ReduceTasks ReduceTaskResponse
}

type MapTaskResponse struct {
	InputFiles []string
	ReduceOuts int32
}

type ReduceTaskResponse struct {
	InputFiles []string
}

type TaskType int

const (
	MapTask TaskType = iota + 1
	ReduceTask
	Noop
)

type TaskFinishedRequest struct {
	UniqueID    string
	TaskType    TaskType
	OutputFiles []string
}

type OKResponse struct {
	Status bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
