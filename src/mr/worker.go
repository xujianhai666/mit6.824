package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
	w := &worker{
		mapf:    mapf,
		reducef: reducef,
	}
	w.run()
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// run get task and execute loop mode
func (w *worker) run() {
	for {
		reply := w.requestTask()
		switch reply.TaskType {
		case MapTask:
			w.doMap(reply)
		case ReduceTask:
			w.doReduce(reply)
		}
	}
}

// requestTask get allocated task from master, blocking if nothing available
func (w *worker) requestTask() *TaskResponse {
	req := &TaskRequest{}
	reply := &TaskResponse{}
	call("Server.AllocateTasks", req, reply)
	return reply
}

func (w *worker) doMap(reply *TaskResponse) {
	var kva []KeyValue
	for _, fileName := range reply.MapTasks.InputFiles {
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		f.Close()
		kva = w.mapf(fileName, string(content))
	}

	encs := make([]*json.Encoder, 0, reply.MapTasks.ReduceOuts)
	fs := make([]*os.File, 0, reply.MapTasks.ReduceOuts)
	for i := 0; i < int(reply.MapTasks.ReduceOuts); i++ {
		fileName := fmt.Sprintf("mr-%s-%d", reply.UniqueID, i)
		f, err := ioutil.TempFile("", fileName)
		if err != nil {
			log.Fatalf("cannot create temp file %v", fileName)
		}
		//f, err := os.Open(fileName)
		//if err != nil {
		//	f, err = os.Create(fileName)
		//	if err != nil {
		//		// no care for lab
		//		log.Fatalf("cannot create %v", fileName)
		//		return
		//	}
		//}
		enc := json.NewEncoder(f)
		encs[i] = enc
		fs[i] = f
	}

	for _, kv := range kva {
		i := ihash(kv.Key) % int(reply.MapTasks.ReduceOuts)
		encs[i].Encode(kv.Value)
	}

	finishedFileNames := make([]string, 0)
	for i, f := range fs {
		fileName := fmt.Sprintf("mr-%s-%d", reply.UniqueID, i)
		finishedFileNames = append(finishedFileNames, fileName)
		os.Rename(f.Name(), fileName)
		f.Close()
	}

	// send finish response
	finishedRequest := &TaskFinishedRequest{
		UniqueID:    reply.UniqueID,
		TaskType:    ReduceTask,
		OutputFiles: finishedFileNames,
	}
	okReply := &OKResponse{}
	call("Server.FinishedTasks", finishedRequest, okReply)
}

func (w *worker) doReduce(reply *TaskResponse) {
	var kva = make([]KeyValue, 0)
	for _, name := range reply.ReduceTasks.InputFiles {
		f, err := os.Open(name)
		if err != nil {
			log.Fatal("open file failed")
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%s", reply.UniqueID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	// send finish response
	finishedRequest := &TaskFinishedRequest{
		UniqueID:    reply.UniqueID,
		TaskType:    ReduceTask,
		OutputFiles: []string{oname},
	}
	okReply := &OKResponse{}
	call("Server.FinishedTasks", finishedRequest, okReply)
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
