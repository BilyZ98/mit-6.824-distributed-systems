package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

const(
	StateIdle = 0
	StateInprogress = 1
	StateComplete = 2

	MapTask  = 3
	ReduceTask = 4

	mapPhase = 5
	reducePhase = 6
	taskDone = 7

	maximumDuration = 10 * time.Second
	scheduleDuration = 100 * time.Millisecond
)



type Master struct {
	// Your definitions here.
	mapTask []Task
	reduceTask []Task
	workers []int
	counter int // for register workers
	mu sync.Mutex
	numReduce int
	//done bool
	taskPhase int
	//taskChannel chan Task
	intermediateFiles [][]string

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

/*
func (m *Master) AssignMapTask(args *MapArgs, reply *MapReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for file, fileEx := range m.FileMap {
		if(!fileEx.Assigned){
			reply.FileName =  file
			reply.HasNewFile = true
			m.FileMap[file].Assigned = true
			return nil;
		}
	}
	reply.HasNewFile = false
	return nil
	//reply.FileName = m.InputFiles[m.Counter++]
}
*/

/*
	call by client, assign a map task or reduce task to client
	will not assign reduce task until all map tasks are finished
*/
func (m *Master) AssignTask(args *TaskArgs, reply *TaskReply) error{
	m.mu.Lock()
	defer m.mu.Unlock()

	done  := m.checkDone()
	if done {
		reply.HasTask = false
		return nil
	}

	workerId := args.WorkerId

	reply.HasTask = false

	if m.taskPhase == mapPhase {
		// still have map task not finished 
		for i, _ := range m.mapTask {
			if m.mapTask[i].State == StateIdle {
				m.mapTask[i].State = StateInprogress
				m.mapTask[i].WorkerID = workerId
				m.mapTask[i].StartTime = time.Now()
				
				reply.HasTask = true
				reply.AssignTask = m.mapTask[i]
				reply.NumReduce = m.numReduce
				
				return nil
			} 
		}
	} else {
			
		// assign reduce task
		for i, _ := range m.reduceTask {
			if m.reduceTask[i].State == StateIdle {
				m.reduceTask[i].State = StateInprogress
				m.reduceTask[i].WorkerID = workerId
				m.reduceTask[i].StartTime = time.Now()
				m.reduceTask[i].ReduceFiles = make([]string, len(m.mapTask))
				for j:=0; j < len(m.mapTask); j++ {
					m.reduceTask[i].ReduceFiles[j] = m.intermediateFiles[j][i]
				}
				reply.HasTask = true
				reply.AssignTask = m.reduceTask[i]
				reply.NumReduce = m.numReduce
				
				return nil
			}
		}
	}



	
	return nil

	//task := <- m.taskChannel
}

func (m *Master) HandleReduce(args *ReduceArgs, reply *ReduceReply)error {
	m.mu.Lock()
	defer m.mu.Unlock()

	workerID := args.WorkerID
	taskID := args.TaskID

	if workerID != m.reduceTask[taskID].WorkerID {

	} else {
		m.reduceTask[taskID].State = StateComplete
	}

	return nil


}

// store the files name sent from worker that finish map task
func (m *Master) HandleMapFiles(args *MapArgs, reply *MapReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	workerID := args.WorkerID
	mapTaskID := args.TaskID
	files := args.FilesName

	// if workerID != wokerId of corresponding task of master, 
	// than it comes from invalid woker, ignore it
	if workerID != m.mapTask[mapTaskID].WorkerID {
		
	} else {
		m.intermediateFiles[mapTaskID] = files
		m.mapTask[mapTaskID].State = StateComplete
	}

	return nil
}

func (m *Master) RegisterWorker(args *RegArgs, reply *RegReply) error {
	reply.WorkerId = m.counter
	m.workers = append(m.workers, m.counter)
	m.counter = m.counter + 1
	return nil
}



func (m *Master) checkMapTask() {
	//m.mu.Lock()
	//defer m.mu.Unlock()

	mapAllDone := true
	for i:=0; i < len(m.mapTask); i++ {
		curtime := time.Now()
		if m.mapTask[i].State == StateInprogress {
			if curtime.Sub(m.mapTask[i].StartTime) > maximumDuration {
				m.mapTask[i].State = StateIdle
			}
		}
		if m.mapTask[i].State != StateComplete {
			mapAllDone = false
		}
	}

	if mapAllDone {
		m.taskPhase = reducePhase
	}

}

// lock must be held before the call to this func
func (m *Master) checkReduceTask() {
	//m.mu.Lock()
	//defer m.mu.Unlock()

	reduceAllDone := true
	for i:=0; i < len(m.reduceTask); i++ {
		curtime := time.Now()
		if m.reduceTask[i].State == StateInprogress {
			if curtime.Sub(m.reduceTask[i].StartTime) > maximumDuration {
				m.reduceTask[i].State = StateIdle
			}
		}
		if m.reduceTask[i].State != StateComplete {
			reduceAllDone = false
		}
	}
	if reduceAllDone {
		m.taskPhase = taskDone
	}


}

func (m *Master)checkDone() bool {
	//m.mu.Lock()
	//defer m.mu.Unlock()

	for i:=0; i <m.numReduce; i++ {
		if m.reduceTask[i].State != StateComplete{
			return false;
		}
	}
	return true;
}



func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()
	//var routinedone sync.WaitGroup
	m.checkMapTask()

	

	m.checkReduceTask()

	if m.checkDone() {
		m.taskPhase = taskDone
	} else {
		
	}
	//routinedone.Wait()
	
}

func (m *Master) tickSchedule() {
	for ;!m.Done(); {
		go m.schedule()
		time.Sleep(scheduleDuration)
	}
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	//ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.taskPhase == taskDone


	//return ret
}

//
func (m *Master)initMaster(files []string, nReduce int) {
	m.numReduce = nReduce
	m.counter = 0
	taskID := 0
	m.mu = sync.Mutex{}
	//m.done = false
	m.taskPhase = mapPhase
	for _, v := range files{
		task := Task{taskID, -1, StateIdle, MapTask, v, time.Now(), nil}
		m.mapTask = append(m.mapTask, task)
		taskID += 1
	}

	taskID = 0
	for i:=0;i < nReduce ; i++{
		task := Task{taskID, -1, StateIdle, ReduceTask, "", time.Now(), nil}
		taskID += 1
		m.reduceTask = append(m.reduceTask, task)
	}

	m.intermediateFiles = make([][]string, len(files))
	for i:= range m.intermediateFiles {
		m.intermediateFiles[i] = make([]string, nReduce)
	}
}
 
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	//m.InputFiles = files


	m.initMaster(files, nReduce)

	//m.FileMap = GetFileMap(files)

	go m.tickSchedule()
	m.server()

	return &m
}
