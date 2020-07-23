package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type _TaskStatus int

const (
	_timeoutS = time.Second * 10
)

// Job represent system input for executing map/reduce
type Job struct {
	files   []string
	nReduce int32
}

// Task represent fragment of Job to assign to each task
type Task struct {
	name      string
	operation TaskType
	files     []string

	startTime time.Time
}

// Master assigns task to worker to execute, which tasks splitting from Job. at present, each task is one file,
// Master assigns all tasks to workers by cap, each worker just get task after finish one task.
// workers register by heartbeat rpc, but at this context, we did not detect worker lifecycle because
// master-worker model is pull-model (actually in production, we use push-mode, heartbeat is necessary).
type Master struct {
	p TaskManager
}

type TaskManager struct {
	sync.Mutex

	input *Job // system input

	inFlights    int32      // unfinished task counter, indicate current state: done or not
	unAssignment chan *Task // prepare to assigned tasks
	unfinished   sync.Map   // delete only by worker ack or timeouts
	reduceTasks  [][]string // temp

	done int32
}

// Your code here -- RPC handlers for the worker to call.

func (m *TaskManager) split() {
	size := len(m.input.files)
	if len(m.input.files) < int(m.input.nReduce) {
		size = int(m.input.nReduce)
	}
	m.unAssignment = make(chan *Task, size)
	for i, file := range m.input.files {
		task := &Task{
			name:      strconv.Itoa(i),
			files:     []string{file},
			operation: MapTask,
		}
		m.unAssignment <- task
	}
	m.inFlights = int32(len(m.input.files))
	m.reduceTasks = make([][]string, m.input.nReduce)
	for i := 0; i < int(m.input.nReduce); i++ {
		m.reduceTasks[i] = make([]string, 0)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

// Tasks allocate map/reduce tasks for worker.
func (m *TaskManager) AllocateTasks(req *TaskRequest, resp *TaskResponse) error {
	if m.stateDone() {
		resp.TaskType = Noop
		return nil
	}
	select {
	case task := <-m.unAssignment:
		resp.UniqueID = task.name
		resp.TaskType = task.operation
		switch task.operation {
		case MapTask:
			resp.MapTasks = MapTaskResponse{
				InputFiles: task.files,
				ReduceOuts: m.input.nReduce,
			}
		case ReduceTask:
			resp.ReduceTasks = ReduceTaskResponse{
				InputFiles: task.files,
			}
		}
		task.startTime = time.Now()
		m.unfinished.Store(task.name, task)
	}
	return nil
}

// TODO(zero.xu): wait all map task finished, then assign reduce task
func (m *TaskManager) FinishedTasks(req *TaskFinishedRequest, reply *OKResponse) error {
	// slow machine will ack task after finished state
	if m.stateDone() {
		return nil
	}

	// in case concurrent ack for same task
	m.Lock()
	defer m.Unlock()

	if _, ok := m.unfinished.Load(req.UniqueID); !ok {
		return nil
	}
	atomic.AddInt32(&m.inFlights, -1) // maybe atomic is extra
	m.unfinished.Delete(req.UniqueID)
	switch req.TaskType {
	case MapTask:
		// maintain reduce task by reduce i
		for _, name := range req.OutputFiles {
			params := strings.Split(name, "-")
			param := params[2]
			i, e := strconv.Atoi(param)
			if e != nil {
				log.Fatal(fmt.Sprintf("convert %s to number failed", param))
			}
			m.reduceTasks[i] = append(m.reduceTasks[i], name)
		}
		if m.inFlights == 0 {
			// prepare reduce task
			m.inFlights = m.input.nReduce
			for i, names := range m.reduceTasks {
				t := &Task{
					name:      strconv.Itoa(i),
					operation: ReduceTask,
					files:     names,
					startTime: time.Time{},
				}
				m.unAssignment <- t
			}
		}
	case ReduceTask:
		if m.inFlights == 0 {
			atomic.StoreInt32(&m.done, 1)
		}
	}
	//m.reduceTasks = append(m.reduceTasks,)
	return nil
}

func (m *TaskManager) healthCheck() {
	for range time.NewTicker(5 * time.Second).C {
		m.unfinished.Range(func(key, value interface{}) bool {
			task := value.(*Task)
			if time.Since(task.startTime) > _timeoutS {
				m.unAssignment <- task
			}
			return true
		})
	}
}

func (m *TaskManager) stateDone() bool {
	// Your code here.
	//return atomic.LoadInt32(&m.inFlights) == 0
	return atomic.LoadInt32(&m.done) == 1
}

//
// start a thread that listens for RPCs from worker.go
//

type Server struct {
	m *Master
}

func (s *Server) AllocateTasks(req *TaskRequest, resp *TaskResponse) error {
	return s.m.p.AllocateTasks(req, resp)
}

func (s *Server) FinishedTasks(req *TaskFinishedRequest, reply *OKResponse) error {
	return s.m.p.FinishedTasks(req, reply)
}

func (s *Server) server() {
	rpc.Register(s)
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
// main/mrmaster.go calls stateDone() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.p.stateDone()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	p := TaskManager{
		input: &Job{
			files:   files,
			nReduce: int32(nReduce),
		},
	}
	m := &Master{
		p: p,
	}

	p.split()
	go p.healthCheck()

	s := &Server{m: m}
	s.server()
	return m
}
