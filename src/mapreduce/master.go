package mapreduce

import "container/list"
import (
	"fmt"
	"log"
)


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func writeChannel(s string, c chan string) {
	c <- s;
}

func processWorkerReg(mr *MapReduce)  {
	for mr.alive {
		workerSockName := <- mr.registerChannel
		go writeChannel(workerSockName, mr.jobCh)
	}
}

func exeJob(mr *MapReduce, i int, workerSockName string, operation JobType) {
	arg := &DoJobArgs{}
	arg.Operation = operation;
	arg.JobNumber = i;
	arg.File = mr.file;

	if operation == Map {
		arg.NumOtherPhase = mr.nReduce
	} else {
		arg.NumOtherPhase = mr.nMap
	}
	var reply DoJobReply;
	ok := call(workerSockName, "Worker.DoJob", arg, &reply)

	if ok == false {
		log.Printf("%v:%v RPC error\n", operation, i)
	}
	log.Printf("%v:%v RPC Done\n", operation, i)

	go writeChannel(workerSockName, mr.jobCh)
	go writeChannel("", mr.completeCh)
}


func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	go processWorkerReg(mr)

	for i := 0; i < mr.nMap; i++ {
		workerSockName := <- mr.jobCh
		go exeJob(mr, i, workerSockName, Map)
	}

	// make sure all Map Job Completed
	for i := 0; i < mr.nMap; i++ {
		<- mr.completeCh
	}

	for i := 0; i < mr.nReduce; i++ {
		workerSockName := <- mr.jobCh
		go exeJob(mr, i,  workerSockName, Reduce)

	}

	// make sure all Reduce Job Completed
	for i := 0; i < mr.nReduce; i++ {
		<- mr.completeCh
	}
	return mr.KillWorkers()
}
