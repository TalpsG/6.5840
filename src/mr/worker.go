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
	"time"
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
type ResponseReduceArgs struct {
	TaskId int
}
type Void struct {
}
type TestArgs struct {
	Type TestType
}
type WaitArgs struct {
	NeedWait bool
}
type Flag bool

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

	talps_path := "./"
	askmap_rpcname := "Coordinator.RequestMap"
	finishmap_rpcname := "Coordinator.ResponseMap"
	//test_rpcname := "Coordinator.Test"
	askreduce_rpcname := "Coordinator.RequestReduce"
	finishreduce_rpcname := "Coordinator.ResponseReduce"
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

		// fmt.Println("map task ", reply)
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
		// rpc告知服务器完成任务
		fargs := ResponseMapArgs{TaskId: reply.Taskid}
		freply := Void{}
		c.Call(finishmap_rpcname, &fargs, &freply)

		// fmt.Println("finish map task ", reply)
	}

	// test reduce
	//test_args := TestArgs{Type: PrintWaitReduce}
	//fmt.Println("waitlist reduce")
	//c.Call(test_rpcname, &test_args, &Void{})
	//fmt.Println("call ")

	wait_rpcname := "Coordinator.WaitReduce"
	args := AskMapArgs{}
	map_done := WaitArgs{false}
	for {
		c.Call(wait_rpcname, &args, &map_done)
		if !map_done.NeedWait {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
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
		// fmt.Println("reduce task ", reply)
		inter_files := make([]*os.File, reply.Total)
		inter_decoders := make([]*json.Decoder, reply.Total)
		// reply.Taskid 代表该任务需要处理mr-x-taskid的文件
		// kvs存储着所有hash到taskid的kv对
		kvs := make(map[string][]string)
		for i := 0; i < reply.Total; i++ {
			filename := fmt.Sprintf("mr-%d-%d", i, reply.Taskid)
			inter_files[i], err = os.Open(talps_path + filename)
			if err != nil {
				log.Fatal("missing inter file :", err)
				break
			}
			inter_decoders[i] = json.NewDecoder(inter_files[i])
			var kv KeyValue
			for {
				if err := inter_decoders[i].Decode(&kv); err != nil {
					break
				}
				if _, ok := kvs[kv.Key]; !ok {
					// 不存在就插入一个空的
					kvs[kv.Key] = make([]string, 0)
				}
				kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
			}
		}
		// 准备好了reduce需要的参数
		// kvs的key就是每一个单词，value则是一堆1,长度为key的出现次数
		// TODO:
		// reduce 生成每个NReduce个文件
		var keys []string
		for key := range kvs {
			keys = append(keys, key)
		}
		reduce_out := fmt.Sprintf("mr-reduce-%d", reply.Taskid)
		reduce_temp := fmt.Sprintf("mr-reduce-%d-*", reply.Taskid)
		file, err := os.CreateTemp(talps_path, reduce_temp)
		if err != nil {
			log.Panic("reduce CreateTemp : ", err)
		}

		encoder := json.NewEncoder(file)
		for _, key := range keys {
			encoder.Encode(&KeyValue{key, reducef(key, kvs[key])})
		}
		oldname := file.Name()
		newname := reduce_out
		file.Close()
		os.Rename(oldname, talps_path+newname)
		response_arg := ResponseReduceArgs{reply.Taskid}
		var res_reply Flag
		err = c.Call(finishreduce_rpcname, &response_arg, &res_reply)
		if err != nil {
			log.Fatal("response reduce fail : ", err)
		}
		if res_reply {
			break
		}
		// fmt.Println("finish reduce task ", reply)
	}

}
