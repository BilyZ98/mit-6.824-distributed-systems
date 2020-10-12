package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

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


type Task struct{
	TaskID	int
	WorkerID int
	State int	// idle inprogress complete
	TaskClass int //map or reduce task
	File string	// for map task
	StartTime time.Time
	ReduceFiles	 []string //for reduce task
}

// Add your RPC definitions here.
type TaskArgs  struct {
	Message string 
	WorkerId int
}

type TaskReply struct {
	AssignTask Task
	//FileName string
	HasTask bool
	NumReduce int
	//WorkClass string //map or reduce
	//TaskNumber int
}

type RegArgs struct{

}

type RegReply struct {
	WorkerId int
}

// after map task done, worker send this to master
type MapArgs struct {
	WorkerID int
	TaskID int
	FilesName []string
}

type MapReply struct {

}

type ReduceArgs struct {
	WorkerID int
	TaskID int
}

type ReduceReply struct {

}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
