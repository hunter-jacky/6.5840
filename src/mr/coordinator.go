package mr

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const TIMER_INTERVAL = 10 * time.Second

var closeChOnce sync.Once

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	fileNum   int32         // number of files
	nReduce   int32         // number of reduce tasks
	phase     Phase         // current phase
	phaseLock chan struct{} // lock for phase

	mapTaskCh   chan string // map task channel
	mapTaskDone int32       // map task done number

	intermediate     map[int][]string // intermediate file name, class by reduceId
	intermediateLock chan struct{}    // lock for intermediate

	reduceTaskCh   chan int // reduce task channel
	reduceTaskDone int32    // reduce task done number

	report     map[int]chan interface{} // task report channel
	reportLock chan struct{}            // lock for report
}

func (c *Coordinator) GetTask(_ *TaskRequest, response *TaskResponse) error {
	response.NReduce = int(c.nReduce)
	response.Uid = int(time.Now().UnixMicro() + rand.Int63n(1000000))

	curPhace := actionWithLockAndReturn(c.phaseLock, func() interface{} {
		return c.phase
	}).(Phase)

	switch curPhace {
	case MapPhase:
		c.AssignMapTask(response)
	case ReducePhase:
		c.AssignReduceTask(response)
	case DonePhase:
		response.Type = DoneTask
	}
	// fmt.Println("get task request from worker", response.Type, response.Uid, response.FileName, response.ReduceID)
	return nil
}

func (c *Coordinator) ReportResult(report *ReportResult, _ *ReportResultResponse) error {
	// fmt.Println("1. report result from worker", report.Uid, report.Type)
	c.reportLock <- struct{}{}
	ch, ok := c.report[report.Uid]
	if !ok {
		return nil
	}
	<-c.reportLock
	// fmt.Println("2. report result from worker", report.Uid, report.Type)
	switch report.Type {
	case MapTask:
		actionWithLock(c.reportLock, func() {
			ch <- report.MapResult
		})
	case ReduceTask:
		actionWithLock(c.reportLock, func() {
			ch <- struct{}{}
		})
	}
	return nil
}

func (c *Coordinator) AssignMapTask(response *TaskResponse) {
	response.Type = MapTask
	// fmt.Println("assign map task", atomic.LoadInt32(&c.mapTaskDone), c.fileNum)
	if atomic.LoadInt32(&c.mapTaskDone) == c.fileNum {
		actionWithLock(c.phaseLock, func() {
			c.phase = ReducePhase
		})
		closeChOnce.Do(func() {
			close(c.mapTaskCh)
		})
		response.Type = WaitTask

		// for i := 0; i < int(c.nReduce); i++ {
		// 	c.intermediateLock <- struct{}{}
		// 	fmt.Println("reduce id:", i, "len fileNames:", len(c.intermediate[i]))
		// 	<-c.intermediateLock
		// }

		return
	}

	filename, ok := <-c.mapTaskCh
	if ok {
		response.Type = MapTask
		response.FileName = filename

		reportCh := make(chan interface{}, 1)
		actionWithLock(c.reportLock, func() {
			c.report[response.Uid] = reportCh
		})
		go func() {
			select {
			case result := <-reportCh:
				// the format of the intermediate file name is */mr-filename-Y
				resultStrs, ok := result.([]string)
				if !ok {
					log.Fatalf("report result from worker is not []string")
				}
				for _, fullpath := range resultStrs {
					filenames := strings.Split(filepath.Base(fullpath), "-")
					reduceId := filenames[len(filenames)-1]
					reduceIdInt, err := strconv.Atoi(reduceId)
					if err != nil {
						log.Fatalf("convert reduceId to int failed: %v", err)
					}
					actionWithLock(c.intermediateLock, func() {
						c.intermediate[reduceIdInt] = append(c.intermediate[reduceIdInt], fullpath)
					})
				}
				atomic.AddInt32(&c.mapTaskDone, 1)
				actionWithLock(c.reportLock, func() {
					delete(c.report, response.Uid)
				})

			case <-time.After(TIMER_INTERVAL):
				c.mapTaskCh <- filename
				actionWithLock(c.reportLock, func() {
					delete(c.report, response.Uid)
				})
			}
		}()
		return
	}
	response.Type = WaitTask
}
func (c *Coordinator) AssignReduceTask(response *TaskResponse) {
	response.Type = ReduceTask

	if atomic.LoadInt32(&c.reduceTaskDone) == c.nReduce {
		c.phaseLock <- struct{}{}
		c.phase = DonePhase
		<-c.phaseLock
		response.Type = DoneTask
		close(c.reduceTaskCh)
		return
	}

	reduceId, ok := <-c.reduceTaskCh
	if ok {
		response.ReduceID = reduceId
		actionWithLock(c.intermediateLock, func() {
			fileNames := make([]string, len(c.intermediate[reduceId]))
			copy(fileNames, c.intermediate[reduceId])
			// fmt.Println("assign reduce task", reduceId, "len fileNames:", len(fileNames), "intermediate:", len(c.intermediate[reduceId]))
			response.ReduceFileNames = fileNames
		})
		reportCh := make(chan interface{}, 1)

		actionWithLock(c.reportLock, func() {
			c.report[response.Uid] = reportCh
		})

		go func() {
			select {
			case <-reportCh:
				atomic.AddInt32(&c.reduceTaskDone, 1)
				actionWithLock(c.reportLock, func() {
					delete(c.report, response.Uid)
				})
			case <-time.After(TIMER_INTERVAL):
				c.reduceTaskCh <- reduceId
				actionWithLock(c.reportLock, func() {
					delete(c.report, response.Uid)
				})
			}
		}()
		return
	}
	response.Type = WaitTask
}

func actionWithLock(lock chan struct{}, action func()) {
	lock <- struct{}{}
	action()
	<-lock
}

func actionWithLockAndReturn(lock chan struct{}, action func() interface{}) interface{} {
	lock <- struct{}{}
	ret := action()
	<-lock
	return ret
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		if err := http.Serve(l, nil); err != nil {
			log.Fatalf("http.Serve error: %v", err)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.phaseLock <- struct{}{}
	defer func() {
		<-c.phaseLock
	}()
	return c.phase == DonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int32) *Coordinator {
	c := Coordinator{
		fileNum:   int32(len(files)),
		nReduce:   nReduce,
		phase:     MapPhase,
		phaseLock: make(chan struct{}, 1),

		mapTaskCh:   make(chan string, len(files)),
		mapTaskDone: 0,

		intermediate:     make(map[int][]string, nReduce),
		intermediateLock: make(chan struct{}, 1),

		reduceTaskCh:   make(chan int, nReduce),
		reduceTaskDone: 0,

		report:     make(map[int]chan interface{}, len(files)+int(nReduce)),
		reportLock: make(chan struct{}, 1),
	}

	for _, file := range files {
		c.mapTaskCh <- file
	}

	for i := 0; i < int(nReduce); i++ {
		c.reduceTaskCh <- i
		c.intermediate[i] = []string{}
	}

	c.server()
	return &c
}
