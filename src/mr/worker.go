package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := CallRegister()
	// uncomment to send the Example RPC to the master.
	//CallExample()
	for {
		has_return, reply := CallForWrok(workerId)
		if(!has_return){
			return
		} else {
			if(reply.HasTask) {
				if(reply.AssignTask.TaskClass == MapTask) {
					//fmt.Println("reply is %v", reply)
					fileName := reply.AssignTask.File
					mapTaskID := reply.AssignTask.TaskID
					numReduce := reply.NumReduce

					file, err := os.Open(reply.AssignTask.File)
					if(err != nil) {
						log.Fatalf("cannot open %v", fileName)
					}
					content, err := ioutil.ReadAll(file)
					if(err != nil) {
						log.Fatalf("cannot read %v", fileName)
					}
					file.Close()

					kva := mapf(fileName, string(content))
					
					intermediateFile := GetIntermediateFiles(numReduce)
					encoders := GetEncoders(intermediateFile)
					//tempfile, err := ioutil.TempFile("", "mapTempFile*")
					for i, v := range kva {
						idx := ihash(v.Key) % numReduce
						err := encoders[idx].Encode(&kva[i])
						if err != nil {
							log.Fatal(err)
						}
					}

					//rename temporary files 
					// send intermediate file name to master, tell master that the map work is done
					files := make([]string, numReduce)
					for i:=0; i < numReduce; i++ {
						files[i] = "mr-" + strconv.Itoa(mapTaskID) + "-" + strconv.Itoa(i) 
						os.Rename(intermediateFile[i].Name(), "mr-" + strconv.Itoa(mapTaskID) + "-" + strconv.Itoa(i))
					}


					SendMapFiles(workerId, reply.AssignTask.TaskID, files)
					
				} else if(reply.AssignTask.TaskClass == ReduceTask) {
					oname := "mr-out-" + strconv.Itoa(reply.AssignTask.TaskID)
					ofile, _ := os.Create(oname)
					
					fmt.Println(ofile.Name())
					intermediatefiles := reply.AssignTask.ReduceFiles

					kva := GetReduceKV(intermediatefiles)

					sort.Sort(ByKey(kva))

					i:=0
					for i < len(kva) {
						j:= i+1
						for j <len(kva) && kva[j].Key == kva[i].Key{
							j++
						}
						values := []string{}
						for k := i; k <j; k++ {
							values = append(values, kva[k].Value)
						}
						output := reducef(kva[i].Key, values)
						fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
						i = j
					}

					ofile.Close()

					CallEndReduce(workerId, reply.AssignTask.TaskID)
				}
			} else {
				// didn't get task from master, so wait for a while
				time.Sleep(time.Second)
			}
	
		}

		
	}

}

func CallEndReduce(workerID, TaskID int) {
	args := ReduceArgs{workerID, TaskID}
	reply := ReduceReply{}

	call("Master.HandleReduce", &args, &reply)
}

func GetIntermediateFiles(numfile int) []*os.File{
	intermediateFile := make([]*os.File, numfile)
	for i:=0; i <numfile; i++ {
		file, err := ioutil.TempFile("", "mr-tmp-*")
		intermediateFile[i] = file
		if err != nil {
			fmt.Println("cannot open tmp file")
			log.Fatal(err)
		}
	}
	return intermediateFile

}

// get intermediate kv pairs for reduce task
func GetReduceKV(files []string) []KeyValue {
	kva := []KeyValue{}

	openfiles := make([]*os.File, len(files))
	for i, filename:= range files{
		if filename == "" {
			continue
		}
		file, err := os.Open(filename)
		openfiles[i] = file
		if err != nil {
			fmt.Println(len(files), i, files)
			log.Fatalf("cannot open file wode %v", filename)
		}

	}

	decoders := GetDecoders(openfiles)
	for i:=0; i< len(decoders); i++ {
		for {
			var kv KeyValue
			if err := decoders[i].Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// close files
	for i,_ := range openfiles {
		openfiles[i].Close()
	}
	return kva
}

func GetEncoders(files []*os.File) []*json.Encoder{
	encoders := make([]*json.Encoder, len(files))
	for i, _ := range files{
		encoders[i] = json.NewEncoder(files[i])
	}
	return encoders
}

func GetDecoders(files []*os.File) []*json.Decoder {
	decoders := make([]*json.Decoder, len(files))
	for i, _ := range files {
		decoders[i] = json.NewDecoder(files[i])
	}
	return decoders
}

//func RenameFiles()

func CallForWrok(workerID int) (bool, TaskReply){
	args := TaskArgs{"give me a fucking work", workerID}

	reply := TaskReply{}

	call_return := call("Master.AssignTask", &args, &reply)

	if(!call_return){
		return false, TaskReply{}
	} else {
		return true, reply	
	}

}

func CallRegister() int {
	args := RegArgs{}
	reply := RegReply{}

	call_return := call("Master.RegisterWorker", &args, &reply)

	if(!call_return) {
		log.Fatalf("fatal cannot get return from call")
	}

	return reply.WorkerId
}

func SendMapFiles(workerID, taskID int, files []string) {
	args := MapArgs{workerID, taskID, files}
	reply := MapReply{}

	call("Master.HandleMapFiles", &args, &reply)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
