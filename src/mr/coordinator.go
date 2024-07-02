package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskInfo struct {
	// 只有map任务需要文件名字
	Filename string

	Taskid  int
	NReduce int
	Start   time.Time
}

func NewMapNode(filename string, taskid int, nReduce int) *ListNode[TaskInfo] {
	ptr := new(ListNode[TaskInfo])
	ptr.Value = new(TaskInfo)
	ptr.Value.Filename = filename
	ptr.Value.Taskid = taskid
	ptr.Value.NReduce = nReduce
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
		fmt.Println(ptr.Value)
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
	return nil

}
func (c *Coordinator) ResponseMap(args *ResponseMapArgs, reply *Void) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.wait_list_for_reduce.Insert(node)
	fmt.Println("add reduce task ", node.Value)

	if c.wait_list_for_map.Next == c.wait_list_for_map &&
		c.mapping_list.Next == c.mapping_list {
		// waitmap和mapping都是空的
		// 则所有任务都到了waitreduce阶段
		c.cv.Broadcast()
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
	fmt.Println()
	return nil
}

func (c *Coordinator) RequestReduce(args *AskReduceArgs, reply *TaskInfo) error {
	c.mu.Lock()
	if c.mapping_list.Next != c.mapping_list {
		c.cv.Wait()
	}
	defer c.mu.Unlock()

	if c.wait_list_for_reduce.Next == c.wait_list_for_reduce {
		// wait_list_for_reduce 没有task了
		reply.Taskid = -1
		return rpc.ServerError("no reduce task")
	}
	node := c.wait_list_for_reduce.Next
	fmt.Println(node.Value)
	node.Release()
	*reply = *node.Value
	c.reducing_list.Insert(node)
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{count: 0, nReduce: nReduce}
	c.cv = sync.NewCond(&c.mu)
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
	for _, str := range files[1:] {
		c.wait_list_for_map.Insert(NewMapNode(str, c.count, c.nReduce))
		c.count += 1
	}
	c.count = 0

	ptr := c.wait_list_for_map.Next
	for {
		if ptr == c.wait_list_for_map {
			break
		}
		fmt.Println(ptr.Value)
		ptr = ptr.Next
	}

	c.server()
	return &c
}
