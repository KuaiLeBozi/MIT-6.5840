package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	mapStartTime    []time.Time
	reduceStartTime []time.Time

	files       []string
	nReduce     int
	nextMapID   int
	mapState    []int //0 for unsent, 1 for Map finished， 2 for send but not finished
	reduceState []int // 0 for unset, 1 for Reduce send, 2 for Reduce finished
	mapFinished bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Nmap = len(c.files)
	switch args.State {
	case 0:
		if len(c.files) > 0 {
			for mapid, mapstate := range c.mapState {
				if c.mapState[mapid] == 2 && time.Since(c.mapStartTime[mapid]) > 10*time.Second {
					c.mapState[mapid] = 0
					mapstate = 0
				}
				if mapstate == 0 {
					reply.ReduceID = -1 // map not finished
					reply.Filename = c.files[mapid]
					reply.MapID = mapid
					reply.NReduce = c.nReduce
					c.mapState[mapid] = 2
					c.mapStartTime[mapid] = time.Now()
					return nil
				}
			}
		}
		reply.ReduceID = -2
		// one major problem is that we cannot correctly deal with the sitiuation
		// if a worker is crushed!!!all the following
	case 1:
		c.mapState[args.MapID] = 1
		if c.mapFinished != true {
			for mapid, mapstate := range c.mapState {
				if c.mapState[mapid] == 2 && time.Since(c.mapStartTime[mapid]) > 10*time.Second {
					c.mapState[mapid] = 0
					mapstate = 0
				}
				if mapstate == 0 {
					reply.ReduceID = -1 // map not finished
					reply.Filename = c.files[mapid]
					reply.MapID = mapid
					reply.NReduce = c.nReduce
					c.mapState[mapid] = 2
					c.mapStartTime[mapid] = time.Now()
					return nil
				}

			}
			c.mapFinished = true
			for mapid, mapstate := range c.mapState {
				if c.mapState[mapid] == 2 && time.Since(c.mapStartTime[mapid]) > 10*time.Second {
					c.mapState[mapid] = 0
					mapstate = 0
				}
				if mapstate != 1 {
					c.mapFinished = false
					reply.ReduceID = -2
					return nil
				}
			}
		}

		for reduceID, reduceState := range c.reduceState {
			if c.reduceState[reduceID] == 1 && time.Since(c.reduceStartTime[reduceID]) > 10*time.Second {
				c.reduceState[reduceID] = 0
				reduceState = 0
			}
			if reduceState == 0 {
				reply.ReduceID = reduceID
				c.reduceState[reduceID] = 1
				c.reduceStartTime[reduceID] = time.Now()
				return nil
			}
		}
		reply.ReduceID = -3
		for reduceID, reduceState := range c.reduceState {
			if c.reduceState[reduceID] == 1 && time.Since(c.reduceStartTime[reduceID]) > 10*time.Second {
				c.reduceState[reduceID] = 0
				reduceState = 0
			}
			if reduceState != 2 {
				reply.ReduceID = -2
				break
			}
		}

	case 2:
		c.reduceState[args.ReduceID] = 2
		for reduceID, reduceState := range c.reduceState {
			if c.reduceState[reduceID] == 1 && time.Since(c.reduceStartTime[reduceID]) > 10*time.Second {
				c.reduceState[reduceID] = 0
				reduceState = 0
			}
			if reduceState == 0 {
				reply.ReduceID = reduceID
				c.reduceState[reduceID] = 1
				c.reduceStartTime[reduceID] = time.Now()
				return nil
			}
		}
		reply.ReduceID = -3 // -2 for no free reduce task, -3 for all finished

		for reduceID, reduceState := range c.reduceState {
			if c.reduceState[reduceID] == 1 && time.Since(c.reduceStartTime[reduceID]) > 10*time.Second {
				c.reduceState[reduceID] = 0
				reduceState = 0
			}
			if reduceState != 2 {
				reply.ReduceID = -2
				break
			}
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := true
	// Your code here.
	for _, reduceState := range c.reduceState {
		if reduceState != 2 {
			ret = false
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:           files,
		nReduce:         nReduce,
		mapState:        make([]int, len(files)),
		reduceState:     make([]int, nReduce),
		mapStartTime:    make([]time.Time, len(files)),
		reduceStartTime: make([]time.Time, nReduce),
	}

	// Your code here.

	c.server(sockname)
	return &c
}
