package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// needed state
	MapTasks        []Task
	ReduceTasks     []Task
	mu              sync.Mutex
	reduceCompleted int
	mapCompleted    int
}

const TIMEOUT = 10 * time.Second

// Your code here -- RPC handlers for the worker to call.
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapTasks = make([]Task, len(files))
	c.mu = sync.Mutex{}
	c.ReduceTasks = make([]Task, nReduce)
	for i, file := range files {
		c.MapTasks[i] = Task{Id: i, Status: Idle, Type: MapTask, Filename: file}
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{Id: i, Status: Idle, Type: ReduceTask}
	}
	c.server()
	return &c
}

func (c *Coordinator) ScheduleTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	//defer c.mu.Unlock()
	mapIdle := false
	reduceIdle := false
	if c.mapCompleted < len(c.MapTasks) {
		for i, task := range c.MapTasks {
			if task.Status == Idle {
				task.Status = InProgress
				if reply.Task == nil {
					reply.Task = &Task{}
				}
				*reply.Task = c.MapTasks[i]
				mapIdle = true
				go func(i int, task *Task) {
					time.Sleep(TIMEOUT)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.MapTasks[i].Status == InProgress {
						c.MapTasks[i].Status = Idle
					}
				}(i, &c.MapTasks[i])
			}
		}
		//waiting for map tasks to complete
		if !mapIdle {
			*reply.Task = Task{Type: MapTask, Status: Completed}
			c.mu.Unlock()
		}
	} else if c.mapCompleted == len(c.MapTasks) && c.reduceCompleted < len(c.ReduceTasks) {
		for i, task := range c.ReduceTasks {
			if task.Status == Idle {
				task.Status = InProgress
				if reply.Task == nil {
					reply.Task = &Task{}
				}
				*reply.Task = c.ReduceTasks[i]
				reduceIdle = true
				go func(i int, task *Task) {
					time.Sleep(TIMEOUT)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.ReduceTasks[i].Status == InProgress {
						c.ReduceTasks[i].Status = Idle
					}
				}(i, &c.ReduceTasks[i])
			}
		}
		if !reduceIdle {
			*reply.Task = Task{Type: ReduceTask, Status: Completed}
			c.mu.Unlock()
		}
	} else {
		*reply.Task = Task{Status: Idle}
		c.mu.Unlock()
	}
	return nil
}

func (c *Coordinator) GetTaskReport(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task.Type == MapTask {
		c.MapTasks[args.Task.Id].Status = Completed
		c.mapCompleted++
	} else {
		c.ReduceTasks[args.Task.Id].Status = Completed
		c.reduceCompleted++
	}
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.reduceCompleted == len(c.ReduceTasks)
	return ret
}
