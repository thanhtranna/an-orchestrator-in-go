package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"

	"github.com/thanhtranna/an-orchestrator-in-go/stats"
	"github.com/thanhtranna/an-orchestrator-in-go/store"
	"github.com/thanhtranna/an-orchestrator-in-go/task"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        store.Store
	TaskCount int
	Stats     *stats.Stats
}

func New(name string, storeType store.DBType) *Worker {
	w := Worker{
		Name:  name,
		Queue: *queue.New(),
		Db:    store.NewTaskStore(name, storeType),
	}

	// s := store.NewTaskStore(name, storeType)
	// w.Db = s
	return &w
}

func (w *Worker) GetTasks() []*task.Task {
	taskList, err := w.Db.List()
	if err != nil {
		log.Printf("[worker] error getting list of tasks: %v\n", err)
		return nil
	}

	return taskList.([]*task.Task)
}

func (w *Worker) CollectStats() {
	for {
		log.Println("[worker] Collecting stats")
		w.Stats = stats.GetStats()
		w.TaskCount = w.Stats.TaskCount
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.runTask()
			if result.Error != nil {
				log.Printf("[worker] Error running task: %v\n", result.Error)
			}
		} else {
			log.Printf("[worker] No tasks to process currently.\n")
		}

		log.Println("[worker] Sleeping for 10 seconds.")
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) runTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("[worker] No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)
	err := w.Db.Put(taskQueued.ID.String(), &taskQueued)
	if err != nil {
		msg := fmt.Errorf("[worker] Error storing task %s: %v", taskQueued.ID.String(), err)
		log.Println(msg)
		return task.DockerResult{Error: msg}
	}

	result, err := w.Db.Get(taskQueued.ID.String())
	if err != nil {
		msg := fmt.Errorf("[worker] Error getting task %s from database: %v", taskQueued.ID.String(), err)
		log.Println(msg)
		return task.DockerResult{Error: msg}
	}

	taskPersisted := *result.(*task.Task)

	if taskPersisted.State == task.Completed {
		return w.StopTask(taskPersisted)
	}

	var dockerResult task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			if taskQueued.ContainerID != "" {
				dockerResult = w.StopTask(taskQueued)
				if dockerResult.Error != nil {
					log.Printf("%v\n", dockerResult.Error)
				}
			}
			dockerResult = w.StartTask(taskQueued)
		default:
			log.Printf("[worker] This is a mistake. taskPersisted: %v, taskQueued: %v\n", taskPersisted, taskQueued)
			dockerResult.Error = errors.New("we should not get here")
		}
	} else {
		err := fmt.Errorf("[worker] Invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
		dockerResult.Error = err
		return dockerResult
	}

	return dockerResult
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	result := d.Run()
	if result.Error != nil {
		log.Printf("[worker] Error running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		w.Db.Put(t.ID.String(), &t)
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db.Put(t.ID.String(), &t)

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("[worker] Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}

	removeResult := d.Remove(t.ContainerID)
	if removeResult.Error != nil {
		log.Printf("%v\n", removeResult.Error)
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db.Put(t.ID.String(), &t)
	log.Printf("[worker] Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)

	return result
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("[worker] Checking status of tasks")
		w.updateTasks()
		log.Println("[worker] Task updates completed")
		log.Println("[worker] Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	// for each task in the worker's datastore:
	// 1. call InspectTask method
	// 2. verify task is in running state
	// 3. if task is not in running state, or not running at all, mark task as `failed`
	tasks, err := w.Db.List()
	if err != nil {
		log.Printf("[worker] Error getting list of tasks: %v\n", err)
		return
	}

	for _, t := range tasks.([]*task.Task) {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				log.Printf("[worker] ERROR: %v\n", resp.Error)
			}

			if resp.Container == nil {
				log.Printf("[worker] No container for running task %s\n", t.ID)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}

			if resp.Container.State.Status == "exited" {
				log.Printf("[worker] Container for task %s in non-running state %s\n", t.ID, resp.Container.State.Status)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}

			// task is running, update exposed ports
			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
			w.Db.Put(t.ID.String(), t)
		}
	}
}
