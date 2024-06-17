package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/thanhtranna/an-orchestrator-in-go/manager"
	"github.com/thanhtranna/an-orchestrator-in-go/task"
	"github.com/thanhtranna/an-orchestrator-in-go/worker"
)

func main() {
	whost := os.Getenv("CUBE_HOST")
	wport, _ := strconv.Atoi(os.Getenv("CUBE_PORT"))

	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport, _ := strconv.Atoi(os.Getenv("CUBE_MANAGER_PORT"))

	fmt.Println("Starting Cube worker")
	db := make(map[uuid.UUID]*task.Task)
	w := worker.Worker{
		Queue: *queue.New(),
		Db:    db,
	}

	wapi := worker.Api{Address: whost, Port: wport, Worker: &w}
	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()

	go wapi.Start()

	fmt.Println("Starting Cube manager")

	workers := []string{fmt.Sprintf("%s:%d", whost, wport)}
	m := manager.New(workers)
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	mapi.Start()
}
