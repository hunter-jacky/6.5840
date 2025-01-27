package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	DoneTask // no more task
	WaitTask // wait for other workers to finish map task
)

type TaskRequest struct{}

type TaskResponse struct {
	Type TaskType
	Uid  int // unique id
	// map task
	FileName string
	NReduce  int
	// reduce task
	ReduceID        int
	ReduceFileNames []string
}

type ReportResult struct {
	Type TaskType
	Uid  int
	// map task
	FileName  string
	MapResult []string
	// reduce task
	ReduceID int
}

type ReportResultResponse struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
