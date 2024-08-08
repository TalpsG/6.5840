package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type TaskInfo struct {
	// 只有map任务需要文件名字
	Filename string

	Taskid  int
	NReduce int
	// 文件/任务数
	Total int

	// 计数器，判断超时
	Cnt int
}

func NewMapNode(filename string, taskid int, nReduce int, num int) *ListNode[TaskInfo] {
	ptr := new(ListNode[TaskInfo])
	ptr.Value = new(TaskInfo)
	ptr.Value.Filename = filename
	ptr.Value.Taskid = taskid
	ptr.Value.NReduce = nReduce
	ptr.Value.Total = num
	return ptr
}

type ListNode[T any] struct {
	Next  *ListNode[T]
	Prev  *ListNode[T]
	Value *T
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	cv *sync.Cond

	wait_list_for_map    *ListNode[TaskInfo]
	mapping_list         *ListNode[TaskInfo]
	wait_list_for_reduce *ListNode[TaskInfo]
	reducing_list        *ListNode[TaskInfo]

	count   int
	nReduce int
	total   int

	done     bool
	reducing bool
}

func (node *ListNode[T]) Insert(ptr *ListNode[T]) {
	// 插到尾部，这样老任务在前
	node.Prev.Next = ptr
	ptr.Prev = node.Prev
	ptr.Next = node
	node.Prev = ptr
}

// 将该节点从所在双链表取下
func (node *ListNode[T]) Release() {
	node.Prev.Next = node.Next
	node.Next.Prev = node.Prev
	node.Prev = nil
	node.Next = nil
}
func (node *ListNode[TaskInfo]) Print() {
	ptr := node.Next
	for {
		if ptr == node {
			return
		}
		ptr = ptr.Next
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestMap(args *AskMapArgs, reply *TaskInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// taskid = -1为无map任务
	if c.wait_list_for_map.Next == c.wait_list_for_map {
		reply.Filename = ""
		reply.Taskid = -1
		return rpc.ServerError("no map task")
	}
	node := c.wait_list_for_map.Next
	node.Release()
	*reply = *node.Value
	c.mapping_list.Insert(node)
	fmt.Println("co map", *reply)
	if node.Value.Taskid == 6 {
		fmt.Println("there should be one task rest")
		ptr := c.wait_list_for_map.Next
		for {
			if ptr == c.wait_list_for_map {
				break
			}
			fmt.Println(ptr.Value)
			ptr = ptr.Next
		}
	}
	return nil

}
func (c *Coordinator) ResponseMap(args *ResponseMapArgs, reply *Void) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("co finish map", *args)
	taskid := args.TaskId
	node := c.mapping_list.Next
	if node == c.mapping_list {
		log.Fatal("no mapping task")
	}
	for {
		if node.Value.Taskid == taskid {
			// 找到对应任务
			break
		}

		node = node.Next

		if node == c.mapping_list {
			log.Fatal("taskid ", taskid, " not found")
			break
		}
	}

	node.Release()

	if c.wait_list_for_map.Next == c.wait_list_for_map &&
		c.mapping_list.Next == c.mapping_list {
		// waitmap和mapping都是空的
		// 则所有任务都到了waitreduce阶段
		c.reducing = true
		for i := 0; i < c.nReduce; i++ {
			c.wait_list_for_reduce.Insert(NewMapNode("", i, c.nReduce, c.total))
		}
	}
	return nil
}

func (c *Coordinator) WaitReduce(args *AskMapArgs, reply *WaitArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果map任务队列不为空
	map_list_empty := c.wait_list_for_map.Next == c.wait_list_for_map
	mapping_list_empty := c.mapping_list.Next == c.mapping_list
	reduce_list_empty := c.wait_list_for_reduce.Next == c.wait_list_for_reduce

	if map_list_empty && mapping_list_empty && !reduce_list_empty {
		reply.NeedWait = true
	} else {
		reply.NeedWait = false
	}
	return nil
}

func (c *Coordinator) Test(args *TestArgs, reply *Void) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case PrintWaitMap:
		c.wait_list_for_map.Print()
	case PrintMapping:
		c.mapping_list.Print()
	case PrintWaitReduce:
		c.wait_list_for_reduce.Print()
	case PrintReducing:
		c.reducing_list.Print()
	}
	return nil
}

func (c *Coordinator) RequestReduce(args *AskReduceArgs, reply *TaskInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.wait_list_for_reduce.Next == c.wait_list_for_reduce {
		// wait_list_for_reduce 没有task了
		reply.Taskid = -1
		return rpc.ServerError("no reduce task")
	}
	node := c.wait_list_for_reduce.Next
	node.Release()
	*reply = *node.Value
	c.reducing_list.Insert(node)
	fmt.Println("co reduce", *reply)
	return nil
}
func (c *Coordinator) ResponseReduce(args *ResponseReduceArgs, reply *Flag) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reducing_list.Next == c.reducing_list {
		// reducing_list 没有task了
		// 所有reduce任务都完成了
		return nil
	}
	node := c.reducing_list.Next
	for node != c.reducing_list {
		if node.Value.Taskid == args.TaskId {
			node.Release()
			fmt.Println("finish reduce", *args)

			*reply = false
			if c.reducing_list.Next == c.reducing_list && c.wait_list_for_reduce.Next == c.wait_list_for_reduce {
				// reducing_list 没有task了
				c.Merge()
				c.done = true
				*reply = true
			}
			return nil
		}
	}
	return rpc.ServerError("not found reducing task")
}

// start a thread that listens for RPCs from worker.go
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
func (c *Coordinator) Merge() {
	file_pattern := "mr-reduce-%d"
	talps_path := "./"
	pattern := "mr-out-0-*"
	out, err := os.CreateTemp(talps_path, pattern)
	if err != nil {
		log.Fatal("merge createtemp file fail : ", err)
	}
	var kvs []KeyValue
	for i := 0; i < c.nReduce; i++ {
		filename := fmt.Sprintf(file_pattern, i)

		file, err := os.Open(talps_path + filename)
		if err != nil {
			log.Fatal("merge fail : ", err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	sort.Sort(ByKey(kvs))
	for _, kv := range kvs {
		temp := kv.Key + " " + kv.Value + "\n"
		out.WriteString(temp)
	}
	oldname := out.Name()
	out.Close()
	os.Rename(oldname, talps_path+"mr-out-0")

}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	ret := false
	// Your code here.

	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.done
	if ret == false {
		if c.reducing {
			// TODO
			// 给所有reduce任务的计数器++
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{count: 0, nReduce: nReduce, total: len(files)}
	// 等待map的任务列表
	c.wait_list_for_map = new(ListNode[TaskInfo])
	c.wait_list_for_map.Prev = c.wait_list_for_map
	c.wait_list_for_map.Next = c.wait_list_for_map

	// 正在map的任务列表
	c.mapping_list = new(ListNode[TaskInfo])
	c.mapping_list.Prev = c.mapping_list
	c.mapping_list.Next = c.mapping_list

	// map完毕等待reduce的列表
	c.wait_list_for_reduce = new(ListNode[TaskInfo])
	c.wait_list_for_reduce.Prev = c.wait_list_for_reduce
	c.wait_list_for_reduce.Next = c.wait_list_for_reduce

	// reducing列表
	c.reducing_list = new(ListNode[TaskInfo])
	c.reducing_list.Prev = c.reducing_list
	c.reducing_list.Next = c.reducing_list

	// 添加task到队列
	// 不需要锁，因为不会并发
	for _, str := range files {
		c.wait_list_for_map.Insert(NewMapNode(str, c.count, c.nReduce, c.total))
		c.count += 1
	}
	c.count = 0

	ptr := c.wait_list_for_map.Next
	for {
		if ptr == c.wait_list_for_map {
			break
		}
		ptr = ptr.Next
	}

	c.server()
	fmt.Println("total ", c.total)
	return &c
}
