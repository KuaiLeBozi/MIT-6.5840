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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	args := AskTaskArgs{}
	reply := AskTaskReply{}
	ok := call("Coordinator.AskTask", &args, &reply) // map function
	if ok != true {
		return
	}
	if reply.Filename == "" {
		return
	}
	if reply.NReduce <= 0 {
		log.Fatalf("Invalid NReduce: %d", reply.NReduce)
	}
	for reply.ReduceID == -1 {
		content, err := os.ReadFile(reply.Filename)
		if err != nil {
			log.Fatalf("read %q: %v", reply.Filename, err)
		}
		kva := mapf(reply.Filename, string(content))

		buckets := make([][]KeyValue, reply.NReduce)
		for _, kv := range kva {
			r := ihash(kv.Key) % reply.NReduce
			buckets[r] = append(buckets[r], kv)
		}

		for r, bucket := range buckets {
			name := fmt.Sprintf("mr-%d-%d", reply.MapID, r)
			file, err := os.Create(name)
			if err != nil {
				log.Fatalf("create %q: %v", name, err)
			}

			encoder := json.NewEncoder(file)

			for _, kv := range bucket {
				if err := encoder.Encode(kv); err != nil {
					file.Close()
					log.Fatalf("write %q: %v", name, err)
				}
			}
			if err := file.Close(); err != nil {
				log.Fatalf("close %q: %v", name, err)
			}
		}

		args.State = 1
		args.MapID = reply.MapID //down is the for formation
		reply = AskTaskReply{}
		ok = call("Coordinator.AskTask", &args, &reply) //reduce function

		if ok != true {
			return
		}
	}
	if reply.ReduceID == -3 {
		return
	}
	for reply.ReduceID != -3 { // one reduce task
		intermediate := []KeyValue{}

		//pattern := fmt.Sprintf("mr-*-%d", reply.ReduceID)
		//files, err := filepath.Glob(pattern)
		//if err != nil {
		//	log.Fatalf("patter error for ReduceID %v", reply.ReduceID)
		//}
		//mr-*-%d could match for other files like "mr-out-1"

		for reply.ReduceID == -2 {
			time.Sleep(300 * time.Millisecond)
			ok := call("Coordinator.AskTask", &args, &reply) //reduce function
			if ok != true {
				return
			}
			if reply.ReduceID == -3 {
				return
			}
		}

		for mapID := 0; mapID < reply.Nmap; mapID++ {
			filename := fmt.Sprintf("mr-%d-%d", mapID, reply.ReduceID)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				err := decoder.Decode(&kv)
				if err == io.EOF {
					break
				}
				if err != nil {
					file.Close()
					log.Fatalf("decode %q: %v", filename, err)
				}

				intermediate = append(intermediate, kv)
			}
			if err := file.Close(); err != nil {
				log.Fatalf("Reduce close %q: %v", filename, err)
			}
		}
		sort.Sort(ByKey(intermediate))

		oname := fmt.Sprintf("mr-out-%d", reply.ReduceID)
		ofile, _ := os.Create(oname)

		i := 0
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

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()

		args.State = 2
		args.ReduceID = reply.ReduceID
		reply = AskTaskReply{}
		if ok := call("Coordinator.AskTask", &args, &reply); ok != true {
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
/*func CallTask() {

	// declare an argument structure.
	args := AskTaskArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := AskTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Filename %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}
}*/

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
