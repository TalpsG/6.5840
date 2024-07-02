package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type RpcType int
type ByKey []KeyValue

type TestType int

const (
	PrintWaitMap TestType = iota
	PrintMapping
	PrintWaitReduce
	PrintReducing
)

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type AskMapArgs struct {
}
type AskReduceArgs struct {
}
type ResponseMapArgs struct {
	TaskId int
}
type Void struct {
}
type TestArgs struct {
	Type TestType
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	talps_path := "/home/talps/gitrepo/6.5840/src/main/test/"
	askmap_rpcname := "Coordinator.RequestMap"
	finishmap_rpcname := "Coordinator.ResponseMap"
	//test_rpcname := "Coordinator.Test"
	askreduce_rpcname := "Coordinator.RequestReduce"
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// Mapping
	for {
		args := AskMapArgs{}
		reply := TaskInfo{}
		err := c.Call(askmap_rpcname, &args, &reply)
		if err != nil {
			log.Println("no more map task : ", err)
			break
		}
		fmt.Println(reply)
		mapfile, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatal("map file open fail :", err)
			break
		}
		content, err := io.ReadAll(mapfile)
		if err != nil {
			log.Fatal("read mapfile fail:", err)
			break
		}
		mapfile.Close()
		// kv对
		kvs := mapf(reply.Filename, string(content))
		sort.Sort(ByKey(kvs))
		map_number := reply.Taskid
		// 创建临时文件及encoder
		tempfiles := make([]*os.File, reply.NReduce)
		encoders := make([]*json.Encoder, reply.NReduce)
		for i := 0; i < reply.NReduce; i++ {
			pattern := fmt.Sprintf("mr-%d-%d-*", map_number, i)
			file, err := os.CreateTemp(talps_path, pattern)
			if err != nil {
				log.Fatal("CreateTemp fail : ", err)
			}
			tempfiles[i] = file
			encoders[i] = json.NewEncoder(file)

		}
		// 将kv按hash放入临时文件
		fmt.Println(reply.NReduce)

		for _, kv := range kvs {
			hashv := ihash(kv.Key) % reply.NReduce
			encoders[hashv].Encode(&kv)
		}

		for i := 0; i < reply.NReduce; i++ {
			oldname := tempfiles[i].Name()
			newname := fmt.Sprintf("mr-%d-%d", map_number, i)
			tempfiles[i].Close()
			err := os.Rename(oldname, talps_path+newname)
			if err != nil {
				log.Fatal("rename fail :", err)
				break
			}
		}
		fmt.Println(reply.Taskid, "finish")
		// rpc告知服务器完成任务
		fargs := ResponseMapArgs{TaskId: reply.Taskid}
		freply := Void{}
		c.Call(finishmap_rpcname, &fargs, &freply)
	}

	// test reduce
	//	test_args := TestArgs{Type: PrintWaitReduce}
	//	fmt.Println("waitlist reduce")
	//	c.Call(test_rpcname, &test_args, &Void{})
	//	fmt.Println("call ")

	// ask reduce
	//
	for {
		args := AskReduceArgs{}
		reply := TaskInfo{}
		err := c.Call(askreduce_rpcname, &args, &reply)
		if err != nil {
			log.Println("no more reduce task :", err)
			break
		}
		fmt.Println("get reduce ", reply)
	}
}
