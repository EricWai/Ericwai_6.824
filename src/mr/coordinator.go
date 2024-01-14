package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int
	TaskId            int
	DistPhase         Phase
	TaskChannelReduce chan *Task
	TaskChannelMap    chan *Task
	taskMetaHolder    TaskMetaHolder
	files             []string
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

type TaskMetaInfo struct {
	state     State
	StartTime time.Time
	TaskAdr   *Task
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		if len(c.TaskChannelMap) > 0 {
			*reply = *<-c.TaskChannelMap
			if !c.taskMetaHolder.judgeState(reply.TaskId) {
				fmt.Printf("taskid[ %d ] is running", reply.TaskId)
			}
		} else {
			reply.TaskType = WaittingTask
			if c.checkTaskDone() {
				c.makeReduceTasks()
				c.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(c.TaskChannelReduce) > 0 {
			*reply = *<-c.TaskChannelReduce
			if !c.taskMetaHolder.judgeState(reply.TaskId) {
				fmt.Printf("taskid[ %d ] is running", reply.TaskId)
			}
		} else {
			reply.TaskType = WaittingTask
			if c.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	default:
		reply.TaskType = ExitTask
	}
	return nil
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
	default:
		panic("The task type undefined ! ! !")
	}
	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

func (c *Coordinator) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUndoneNum    = 0
		reduceDoneNum   = 0
		reduceUndoneNum = 0
	)
	t := c.taskMetaHolder
	for _, metaInfo := range t.MetaMap {
		if metaInfo.TaskAdr.TaskType == MapTask {
			if metaInfo.state == Done {
				mapDoneNum++
			} else {
				if metaInfo.state == Working && time.Since(metaInfo.StartTime) > time.Second*2 {
					metaInfo.state = Waiting
					c.TaskChannelMap <- metaInfo.TaskAdr
					fmt.Printf("the task id[%d] crashed~~\n", metaInfo.TaskAdr.TaskId)
				}
				mapUndoneNum++
				
			}
		} else if metaInfo.TaskAdr.TaskType == ReduceTask {
			if metaInfo.state == Done {
				reduceDoneNum++
			} else {
				if metaInfo.state == Working && time.Since(metaInfo.StartTime) > time.Second*2 {
					metaInfo.state = Waiting
					c.TaskChannelReduce <- metaInfo.TaskAdr
				}
				reduceUndoneNum++
			}
		}
	}
	fmt.Printf("map tasks  are finished %d/%d, reduce task are finished %d/%d \n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	if mapDoneNum > 0 && mapUndoneNum == 0 && reduceDoneNum == 0 && reduceUndoneNum == 0 {
		return true
	} else if reduceDoneNum > 0 && reduceUndoneNum == 0 {
		return true
	}
	return false
}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

//
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
	mu.Lock()
	defer mu.Unlock()
	ret := false

	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		ret = true
	}
	// Your code here.

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	c.makeMapTasks()

	// Your code here.
	c.server()
	return &c
}

func (c *Coordinator) makeMapTasks() {
	for _, file := range c.files {
		id := c.genTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			Filename:   []string{file},
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("make a map task :", &task)
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.genTaskId()
		filename := make([]string, 0)
		for j := 0; j < len(c.files); j++ {
			fn := "mr-tmp-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
			filename = append(filename, fn)
		}
		task := Task{
			TaskType:   ReduceTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			Filename:   filename,
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Println("make a reduce task :", &task)
		c.TaskChannelReduce <- &task
	}
}

func (c *Coordinator) genTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	if _, ok := t.MetaMap[taskId]; ok {
		fmt.Println("meta contain task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}
