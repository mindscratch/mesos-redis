package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/mesosphere/RENDLER/go/src/github.com/mesosphere/rendler"
)

const (
	taskCPUs        = 0.1
	taskMem         = 32.0
	shutdownTimeout = time.Duration(30) * time.Second
)

const (
	crawlCommand  = "python crawl_executor.py"
	renderCommand = "python render_executor.py"
)

var (
	defaultFilter = &mesos.Filters{RefuseSeconds: proto.Float64(1)}
)

// maxTasksForOffer computes how many tasks can be launched using a given offer
func maxTasksForOffer(offer *mesos.Offer) int {
	count := 0

	var cpus, mem float64

	for _, resource := range offer.Resources {
		switch resource.GetName() {
		case "cpus":
			cpus += *resource.GetScalar().Value
		case "mem":
			mem += *resource.GetScalar().Value
		}
	}

	for cpus >= taskCPUs && mem >= taskMem {
		count++
		cpus -= taskCPUs
		mem -= taskMem
	}

	return count
}

// redisScheduler implements the Scheduler interface and stores
// the state needed for Rendler to function.
type redisScheduler struct {
	tasksCreated int
	tasksRunning int

	// This channel is closed when the program receives an interrupt,
	// signalling that the program should shut down.
	shutdown chan struct{}
	// This channel is closed after shutdown is closed, and only when all
	// outstanding tasks have been cleaned up
	done chan struct{}

	redisExecutor *mesos.ExecutorInfo
}

// newRedisScheduler creates a new scheduler for Rendler.
func newRedisScheduler() *redisScheduler {
	redisArtifacts := executorURIs()

	s := &redisScheduler{
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
		redisExecutor: &mesos.ExecutorInfo{
			ExecutorId: &mesos.ExecutorID{Value: proto.String("redis-executor")},
			Command: &mesos.CommandInfo{
				//Shell: proto.Bool(false),
				Value: proto.String("env|sort"),
				Uris:  redisArtifacts,
			},
			Name: proto.String("redis"),
		},
	}
	return s
}

func (s *redisScheduler) newTaskPrototype(offer *mesos.Offer) *mesos.TaskInfo {
	taskID := s.tasksCreated
	s.tasksCreated++
	return &mesos.TaskInfo{
		TaskId: &mesos.TaskID{
			Value: proto.String(fmt.Sprintf("RENDLER-%d", taskID)),
		},
		SlaveId: offer.SlaveId,
		Resources: []*mesos.Resource{
			mesosutil.NewScalarResource("cpus", taskCPUs),
			mesosutil.NewScalarResource("mem", taskMem),
		},
	}
}

func (s *redisScheduler) newRedisTask(url string, offer *mesos.Offer) *mesos.TaskInfo {
	task := s.newTaskPrototype(offer)
	task.Name = proto.String("REDIS_" + *task.TaskId.Value)
	task.Executor = s.redisExecutor
	task.Data = []byte(url)
	return task
}

//
// func (s *redisScheduler) newRenderTask(url string, offer *mesos.Offer) *mesos.TaskInfo {
// 	task := s.newTaskPrototype(offer)
// 	task.Name = proto.String("RENDER_" + *task.TaskId.Value)
// 	task.Executor = s.renderExecutor
// 	task.Data = []byte(url)
// 	return task
// }

func (s *redisScheduler) Registered(
	_ sched.SchedulerDriver,
	frameworkID *mesos.FrameworkID,
	masterInfo *mesos.MasterInfo) {
	log.Printf("Framework %s registered with master %s", frameworkID, masterInfo)
}

func (s *redisScheduler) Reregistered(_ sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Printf("Framework re-registered with master %s", masterInfo)
}

func (s *redisScheduler) Disconnected(sched.SchedulerDriver) {
	log.Println("Framework disconnected with master")
}

func (s *redisScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	log.Printf("Received %d resource offers", len(offers))
	for _, offer := range offers {
		select {
		case <-s.shutdown:
			log.Println("Shutting down: declining offer on [", offer.Hostname, "]")
			driver.DeclineOffer(offer.Id, defaultFilter)
			if s.tasksRunning == 0 {
				close(s.done)
			}
			continue
		default:
		}

		tasks := []*mesos.TaskInfo{}
		tasksToLaunch := maxTasksForOffer(offer)
		log.Printf("TASKS TO LAUNCH: %d\n", tasksToLaunch)
		// TODO REMOVE line below
		tasksToLaunch = 1
		for tasksToLaunch > 0 {
			task := s.newRedisTask("http://this-is-a-test.com", offer)
			tasks = append(tasks, task)
			log.Printf("launching task: %#v\n", task)
			tasksToLaunch--
			// 	if s.crawlQueue.Front() != nil {
			// 		url := s.crawlQueue.Front().Value.(string)
			// 		s.crawlQueue.Remove(s.crawlQueue.Front())
			// 		task := s.newCrawlTask(url, offer)
			// 		tasks = append(tasks, task)
			// 		tasksToLaunch--
			// 	}
			// 	if s.renderQueue.Front() != nil {
			// 		url := s.renderQueue.Front().Value.(string)
			// 		s.renderQueue.Remove(s.renderQueue.Front())
			// 		task := s.newRenderTask(url, offer)
			// 		tasks = append(tasks, task)
			// 		tasksToLaunch--
			// 	}
			// 	if s.crawlQueue.Front() == nil && s.renderQueue.Front() == nil {
			// 		break
			// 	}
		}

		if len(tasks) == 0 {
			driver.DeclineOffer(offer.Id, defaultFilter)
		} else {
			driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, defaultFilter)
		}
		// TODO remove sleep
		time.Sleep(10 * time.Second)
	}
}

func (s *redisScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Printf("Received task status [%s] for task [%s]", rendler.NameFor(status.State), *status.TaskId.Value)

	if *status.State == mesos.TaskState_TASK_RUNNING {
		s.tasksRunning++
	} else if rendler.IsTerminal(status.State) {
		s.tasksRunning--
		if s.tasksRunning == 0 {
			select {
			case <-s.shutdown:
				close(s.done)
			default:
			}
		}
	}
}

func (s *redisScheduler) FrameworkMessage(
	driver sched.SchedulerDriver,
	executorID *mesos.ExecutorID,
	slaveID *mesos.SlaveID,
	message string) {

	log.Printf("Getting a framework message: %#v\n", executorID)
	// switch *executorID.Value {
	// case *s.crawlExecutor.ExecutorId.Value:
	// 	log.Print("Received framework message from crawler")
	// 	var result rendler.CrawlResult
	// 	err := json.Unmarshal([]byte(message), &result)
	// 	if err != nil {
	// 		log.Printf("Error deserializing CrawlResult: [%s]", err)
	// 		return
	// 	}
	// 	for _, link := range result.Links {
	// 		edge := &rendler.Edge{From: result.URL, To: link}
	// 		log.Printf("Appending [%s] to crawl results", edge)
	// 		s.crawlResults = append(s.crawlResults, edge)
	//
	// 		if _, ok := s.processedURLs[link]; !ok {
	// 			log.Printf("Enqueueing [%s]", link)
	// 			s.crawlQueue.PushBack(link)
	// 			s.renderQueue.PushBack(link)
	// 			s.processedURLs[link] = struct{}{}
	// 		}
	// 	}

	// case *s.renderExecutor.ExecutorId.Value:
	// 	log.Printf("Received framework message from renderer")
	// 	var result rendler.RenderResult
	// 	err := json.Unmarshal([]byte(message), &result)
	// 	if err != nil {
	// 		log.Printf("Error deserializing RenderResult: [%s]", err)
	// 		return
	// 	}
	// 	log.Printf(
	// 		"Appending [%s] to render results",
	// 		rendler.Edge{From: result.URL, To: result.ImageURL},
	// 	)
	// 	s.renderResults[result.URL] = result.ImageURL
	//
	// default:
	// 	log.Printf("Received a framework message from some unknown source: %s", *executorID.Value)
	// }
}

func (s *redisScheduler) OfferRescinded(_ sched.SchedulerDriver, offerID *mesos.OfferID) {
	log.Printf("Offer %s rescinded", offerID)
}
func (s *redisScheduler) SlaveLost(_ sched.SchedulerDriver, slaveID *mesos.SlaveID) {
	log.Printf("Slave %s lost", slaveID)
}
func (s *redisScheduler) ExecutorLost(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, status int) {
	log.Printf("Executor %s on slave %s was lost", executorID, slaveID)
}

func (s *redisScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Printf("Receiving an error: %s", err)
}

func executorURIs() []*mesos.CommandInfo_URI {
	basePath, err := filepath.Abs(filepath.Dir(os.Args[0])) // + "/../..")
	if err != nil {
		log.Fatal("Failed to find the path to Redis")
	}
	baseURI := fmt.Sprintf("%s/", basePath)

	pathToURI := func(path string, extract bool) *mesos.CommandInfo_URI {
		return &mesos.CommandInfo_URI{
			Value:   &path,
			Extract: &extract,
		}
	}
	log.Printf("BASE URI: %s\n", baseURI)

	return []*mesos.CommandInfo_URI{
		//pathToURI(baseURI+"fake-redis.sh", false),
		//pathToURI(baseURI+"executor", false),
		pathToURI("/files/executor", false),
	}
}

func main() {
	master := flag.String("master", "127.0.1.1:5050", "Location of leading Mesos master")

	flag.Parse()

	scheduler := newRedisScheduler()
	driver, err := sched.NewMesosSchedulerDriver(sched.DriverConfig{
		Master: *master,
		Framework: &mesos.FrameworkInfo{
			Name: proto.String("REDIS"),
			User: proto.String("root"),
		},
		Scheduler: scheduler,
	})
	if err != nil {
		log.Printf("Unable to create scheduler driver: %s", err)
		return
	}

	// Catch interrupt
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		s := <-c
		if s != os.Interrupt {
			return
		}

		log.Println("REDIS is shutting down")
		close(scheduler.shutdown)

		select {
		case <-scheduler.done:
		case <-time.After(shutdownTimeout):
		}

		// we have shut down
		driver.Stop(false)
	}()

	if status, err := driver.Run(); err != nil {
		log.Printf("Framework stopped with status %s and error: %s\n", status.String(), err.Error())
	}
	log.Println("Exiting...")
}
