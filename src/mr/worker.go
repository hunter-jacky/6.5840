package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const INTERMEDIATE_DIR = "/home/jiaqiwu/go/src/6.5840/src/main/intermediate/"

type NONE struct{}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		response := CallGetTask()
		if response == nil {
			log.Printf("no task to do")
			return
		}
		switch response.Type {
		case MapTask:
			// fmt.Printf("worker %d start map task %s\n", response.Uid, response.FileName)
			resultFileList := doMap(mapf, response)
			CallReportResult(&ReportResult{
				Type:      MapTask,
				Uid:       response.Uid,
				FileName:  response.FileName,
				MapResult: resultFileList,
			})
			// fmt.Printf("worker %d finish map task %s\n", response.Uid, response.FileName)
		case ReduceTask:
			// fmt.Printf("worker %d start reduce task %d\n", response.Uid, response.ReduceID)
			doReduce(reducef, response)
			CallReportResult(&ReportResult{
				Type:     ReduceTask,
				Uid:      response.Uid,
				ReduceID: response.ReduceID,
			})
			// fmt.Printf("worker %d finish reduce task %d\n", response.Uid, response.ReduceID)
		case DoneTask:
			return
		case WaitTask:
			time.Sleep(time.Second)
		default:
			log.Printf("unknown task type")
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, response *TaskResponse) []string {
	intermediate := map[int][]KeyValue{}
	file, err := os.Open(response.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", response.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", response.FileName)
	}
	file.Close()

	kva := mapf(response.FileName, string(content))

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % response.NReduce
		intermediate[reduceId] = append(intermediate[reduceId], kv)
	}

	resultFileList := []string{}
	if _, err := os.Stat(INTERMEDIATE_DIR); os.IsNotExist(err) {
		_ = os.Mkdir(INTERMEDIATE_DIR, 0755)
	}
	for reduceId, kvs := range intermediate {
		filename := fmt.Sprintf("mr-%s-%d", strings.ReplaceAll(response.FileName, "/", "-"), reduceId)
		fullpath := filepath.Join(INTERMEDIATE_DIR, filename)
		resultFileList = append(resultFileList, fullpath)
		storeIntermediate(kvs, fullpath)
	}
	return resultFileList
}

func doReduce(reducef func(string, []string) string, response *TaskResponse) {
	intermediate := []KeyValue{}
	for _, filename := range response.ReduceFileNames {
		intermediate = append(intermediate, loadIntermediate(filename)...)
	}

	sort.Sort(ByKey(intermediate))

	tmpFile, err := os.CreateTemp("", "mr-tmp-*")
	defer os.Remove(tmpFile.Name())
	if err != nil {
		log.Fatalf("create tmp file failed: %v", err)
	}
	i := 0
	nLine := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		nLine++
		i = j
	}

	tmpFile.Close()
	oname := fmt.Sprintf("mr-out-%d", response.ReduceID)
	if err = os.Rename(tmpFile.Name(), oname); err != nil {
		log.Fatalf("rename tmp file failed: %v", err)
	}
	// fmt.Println("write to file:", response.ReduceID, len(intermediate), len(response.ReduceFileNames), nLine)
}

func storeIntermediate(intermediate []KeyValue, filename string) {
	if _, err := os.Stat(filename); err == nil {
		os.Remove(filename)
	}

	tmpFile, err := os.CreateTemp("", "mr-tmp-*")
	defer os.Remove(tmpFile.Name())
	if err != nil {
		log.Fatalf("create tmp file failed: %v", err)
	}
	enc := json.NewEncoder(tmpFile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			panic(err)
		}
	}
	tmpFile.Close()
	if err = os.Rename(tmpFile.Name(), filename); err != nil {
		log.Fatalf("rename tmp file failed: %v", err)
	}

}

func loadIntermediate(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func CallGetTask() *TaskResponse {
	response := &TaskResponse{}
	if !call("Coordinator.GetTask", &TaskRequest{}, response) {
		panic("call GetTask() failed")
	}
	return response
}

func CallReportResult(report *ReportResult) {
	_ = call("Coordinator.ReportResult", &report, &ReportResultResponse{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, request interface{}, response interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, request, response)
	if err == nil {
		return true
	}

	fmt.Println("call error:", err)
	return false
}
