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

func writeChannelString(s string, c chan string) {
	c <- s;
}

func writeChannelBool(s bool, c chan bool) {
	c <- s;
}

func writeChannelJob(s *Job, c chan *Job) {
	c <- s;
}

func workerRegProcessor(mr *MapReduce)  {
	for mr.alive {
		workerSockName := <- mr.registerChannel
		go writeChannelString(workerSockName, mr.workerCh)
	}
}

func exeJob(job *Job) {
	arg := &DoJobArgs{}
	arg.Operation = job.operation;
	arg.JobNumber = job.index;
	arg.File = job.mr.file;

	if job.operation == Map {
		arg.NumOtherPhase = job.mr.nReduce
	} else {
		arg.NumOtherPhase = job.mr.nMap
	}
	var reply DoJobReply;

	workerSockName := <- job.mr.workerCh
	ok := call(workerSockName, "Worker.DoJob", arg, &reply)

	if ok {
		log.Printf("%s:%v RPC Done: ", job.operation, job.index)
		go writeChannelBool(true, job.mr.completeCh)

	} else {
		log.Printf("%s:%v RPC error\n", job.operation, job.index)
		go writeChannelJob(job, job.mr.jobCh)
	}

	go writeChannelString(workerSockName, job.mr.workerCh)
}

func jobExecutor(mr *MapReduce) {
	for mr.alive {
		job := <- mr.jobCh
		go exeJob(job)
	}
}

func processMapJobs(mr *MapReduce) {
	//produce Map Jobs
	for i := 0; i < mr.nMap; i++ {
		go writeChannelJob(&Job{Map, i, mr}, mr.jobCh)
	}

	// make sure all Map Job Completed
	for i := 0; i < mr.nMap; i++ {
		<- mr.completeCh
	}

	go writeChannelBool(true, mr.mapCompleteCh)

}

func processReduceJobs(mr *MapReduce)  {
	<- mr.mapCompleteCh

	for i := 0; i < mr.nReduce; i++ {
		go writeChannelJob(&Job{Reduce, i, mr}, mr.jobCh)
	}

	// make sure all Reduce Job Completed
	for i := 0; i < mr.nReduce; i++ {
		<- mr.completeCh
	}
	go writeChannelBool(true, mr.reduceCompleteCh)
}


func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	go workerRegProcessor(mr)
	go jobExecutor(mr)

	go processMapJobs(mr);
	go processReduceJobs(mr);

	<- mr.reduceCompleteCh

	return mr.KillWorkers()
}
