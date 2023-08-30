package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type TaskType int

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
)

type TaskStatus int

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Completed  TaskStatus = 2
)

type Task struct {
	Type     TaskType
	Id       int
	Filename string
	NReduce  int
	Status   TaskStatus
}

type GetTaskArgs struct {
	Task *Task
}

type GetTaskReply struct {
	Task *Task
}

type ReportArgs struct {
	Task *Task
}

type ReportReply struct {
	Task *Task
}

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
	for {
		// ask for a task
		task := GetTask()
		//no task available or all tasks are done
		if task == nil || task.Status == Idle {
			break
		}
		// do the task
		if task.Status == InProgress {
			switch task.Type {
			case MapTask:
				DoMapTask(task, mapf)
			case ReduceTask:
				DoReduceTask(task, reducef)
			default:
				log.Fatalf("Unknown task type: %v", task.Type)
			}
			// report the task is done
			ReportTaskDone(task)
		}
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

func GetTask() *Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.ScheduleTask", &args, &reply)
	if !ok {
		reply.Task = nil
	}
	return reply.Task
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// create intermediate files
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%task.NReduce] = append(intermediate[ihash(kv.Key)%task.NReduce], kv)
	}
	for i := 0; i < task.NReduce; i++ {
		oname := fmt.Sprintf("mr-%v-%v", task.Id, i)
		ofile, _ := ioutil.TempFile("", oname+"*")
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	intermediate := make([]KeyValue, 0)
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, task.Id)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			errDecode := dec.Decode(&kv)
			if errDecode != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	oname := fmt.Sprintf("mr-out-%v", task.Id)
	ofile, _ := ioutil.TempFile("", oname+"*")
	// sort intermediate
	sortIntermediate := make(map[string][]string)
	for _, kv := range intermediate {
		sortIntermediate[kv.Key] = append(sortIntermediate[kv.Key], kv.Value)
	}
	// call reducef on each key
	for key, values := range sortIntermediate {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()
}

func ReportTaskDone(task *Task) {
	args := ReportArgs{Task: task}
	reply := ReportReply{}
	ok := call("Coordinator.GetTaskReport", &args, &reply)
	if !ok {
		log.Fatalf("ReportTaskDone failed")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
