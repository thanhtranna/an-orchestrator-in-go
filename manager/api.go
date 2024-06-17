package manager

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

type ErrResponse struct {
	HTTPStatusCode int
	Message        string
}

type Api struct {
	Address string
	Port    int
	Manager *Manager
	Router  *chi.Mux
}

func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.StartTaskHandler)
		r.Get("/", a.GetTasksHandler)
		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", a.StopTaskHandler)
		})
	})
}

func (a *Api) Start() {
	a.initRouter()
	log.Println("[manager] Cube manager running with port:", a.Port)
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}
