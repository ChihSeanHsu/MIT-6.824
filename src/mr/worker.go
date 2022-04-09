package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

// KeyValue
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
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DoMap(mapf func(string, string) []KeyValue, task GetTaskReply, nodeID string) {
	content, _ := readFile(task.Files[0])
	kva := mapf(task.Files[0], string(content))
	partitionArr := make([][]KeyValue, 0, task.ReduceCount)
	for _, kv := range kva {
		partition := ihash(kv.Key) % task.ReduceCount
		partitionArr[partition] = append(partitionArr[partition], kv)
	}
	for idx, part := range partitionArr {
		if len(part) == 0 {
			continue
		}
		jsonKV, err := json.Marshal(part)
		if err != nil {
			log.Fatalf("marshal json failed %v", err)
		}
		newFile := fmt.Sprintf("mr-%d-%d", task.ID, idx)
		writeFile(newFile, jsonKV)
	}
	CallTaskComplete(nodeID, task.TaskType, task.ID)
}

func DoReduce(reducef func(string, []string) string, task GetTaskReply, nodeID string) {
	var intermediates []KeyValue
	// read map result from each intermediate files
	for _, filename := range task.Files {
		var data []KeyValue
		content, _ := readFile(filename)
		err := json.Unmarshal(content, &data)
		if err != nil {
			log.Fatalf("unmarshal json failed %v", err)
		}
		intermediates = append(intermediates, data...)
	}

	outputFilename := fmt.Sprintf("mr-out-%d", task.ID)
	outputFile, _ := os.Create(outputFilename)
	defer outputFile.Close()
	// sort to use sliding window
	sort.Sort(ByKey(intermediates))
	left := 0
	for left < len(intermediates) {
		right := left + 1
		for right < len(intermediates) && intermediates[left].Key == intermediates[left].Key {
			right++
		}
		values := make([]string, 0, right)
		for k := left; k < right; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[left].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediates[left].Key, output)
		left = right
	}
	CallTaskComplete(nodeID, task.TaskType, task.ID)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// TODO: need to check with another worker
	pid := os.Getpid()
	nodeID := fmt.Sprintf("%d-%s", pid, uuid.New().String())
	task := CallGetTask(nodeID)

	// Your worker implementation here.
	for task.TaskType != NoTask {
		switch task.TaskType {
		case WaitTask:
			fmt.Println("Wait for others")
		case MapTask:
			DoMap(mapf, task, nodeID)
		case ReduceTask:
			DoReduce(reducef, task, nodeID)
		}
		time.Sleep(time.Second)
		task = CallGetTask(nodeID)
	}
	fmt.Println("Done")
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

func readFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	_ = file.Close()
	return content, err
}

func writeFile(filename string, content []byte) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}
	_, _ = f.Write(content)
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

func CallGetTask(nodeID string) GetTaskReply {
	args := GetTaskArgs{
		NodeID: nodeID,
	}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

func CallTaskComplete(nodeID string, taskType TaskType, taskID int) {
	args := TaskCompleteArgs{
		NodeID:   nodeID,
		TaskType: taskType,
		TaskID:   taskID,
	}
	reply := TaskCompleteReply{}
	call("Coordinator.TaskComplete", &args, &reply)
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
