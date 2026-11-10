package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type AskTaskArgs struct {
	MapID    int
	State    int //0 for map start, 1 for  map finished, 2 for reduce finished
	ReduceID int
}

type AskTaskReply struct {
	NReduce  int
	MapID    int
	Filename string
	ReduceID int // -1 for map state, -2 for no free reduce task(should wait), -3 for all task finished(should return)
	Nmap     int
}

// Add your RPC definitions here.
