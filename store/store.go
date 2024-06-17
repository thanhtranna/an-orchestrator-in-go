package store

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"

	"github.com/thanhtranna/an-orchestrator-in-go/task"
)

type DBType string

const (
	DBTypeMemory     DBType = "memory"
	DBTypePersistent DBType = "persistent"
)

type Store interface {
	Put(key string, value interface{}) error
	Get(key string) (interface{}, error)
	List() (interface{}, error)
	Count() (int, error)
}

func NewTaskStore(bucketName string, dbType DBType) Store {
	var ts Store
	var errTaskDb error
	switch dbType {
	case DBTypeMemory:
		ts = newInMemoryTaskStore()
	case DBTypePersistent:
		ts, errTaskDb = newTaskStore(fmt.Sprintf("%s.db", bucketName), 0600, bucketName)
	}
	if errTaskDb != nil {
		log.Fatalf("[manager] Unable to create task store: %v", errTaskDb)
	}

	return ts
}

type inMemoryTaskStore struct {
	Db map[string]*task.Task
}

func newInMemoryTaskStore() Store {
	return &inMemoryTaskStore{
		Db: make(map[string]*task.Task),
	}
}

func (i *inMemoryTaskStore) Put(key string, value interface{}) error {
	t, ok := value.(*task.Task)
	if !ok {
		return fmt.Errorf("[store] Value %v is not a task.Task type", value)
	}

	i.Db[key] = t
	return nil
}

func (i *inMemoryTaskStore) Get(key string) (interface{}, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("[store] Task with key %s does not exist", key)
	}

	return t, nil
}

func (i *inMemoryTaskStore) List() (interface{}, error) {
	var tasks []*task.Task
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (i *inMemoryTaskStore) Count() (int, error) {
	return len(i.Db), nil
}

type inMemoryTaskEventStore struct {
	Db map[string]*task.TaskEvent
}

func newInMemoryTaskEventStore() Store {
	return &inMemoryTaskEventStore{
		Db: make(map[string]*task.TaskEvent),
	}
}

func (i *inMemoryTaskEventStore) Put(key string, value interface{}) error {
	e, ok := value.(*task.TaskEvent)
	if !ok {
		return fmt.Errorf("[store] Value %v is not a task.TaskEvent type", value)
	}
	i.Db[key] = e
	return nil
}

func (i *inMemoryTaskEventStore) Get(key string) (interface{}, error) {
	e, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("[store] Task event with key %s does not exist", key)
	}

	return e, nil
}

func (i *inMemoryTaskEventStore) List() (interface{}, error) {
	var events []*task.TaskEvent
	for _, e := range i.Db {
		events = append(events, e)
	}
	return events, nil
}

func (i *inMemoryTaskEventStore) Count() (int, error) {
	return len(i.Db), nil
}

type TaskStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func newTaskStore(file string, mode os.FileMode, bucket string) (*TaskStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("[store] Unable to open %v", file)
	}
	t := TaskStore{
		DbFile:   file,
		FileMode: mode,
		Db:       db,
		Bucket:   bucket,
	}

	err = t.CreateBucket()
	if err != nil {
		log.Printf("[store] Bucket already exists, will use it instead of creating new one")
	}

	return &t, nil
}

func (t *TaskStore) Close() {
	t.Db.Close()
}

func (t *TaskStore) Count() (int, error) {
	taskCount := 0
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tasks"))
		b.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}

	return taskCount, nil

}

func (t *TaskStore) CreateBucket() error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(t.Bucket))
		if err != nil {
			return fmt.Errorf("[store] Create bucket %s: %s", t.Bucket, err)
		}
		return nil
	})

}

func (t *TaskStore) Put(key string, value interface{}) error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))

		buf, err := json.Marshal(value.(*task.Task))
		if err != nil {
			return err
		}

		err = b.Put([]byte(key), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (t *TaskStore) Get(key string) (interface{}, error) {
	var task task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		t := b.Get([]byte(key))
		if t == nil {
			return fmt.Errorf("[store] Task %v not found", key)
		}
		err := json.Unmarshal(t, &task)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &task, nil

}

func (t *TaskStore) List() (interface{}, error) {
	var tasks []*task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		b.ForEach(func(k, v []byte) error {
			var task task.Task
			err := json.Unmarshal(v, &task)
			if err != nil {
				return err
			}
			tasks = append(tasks, &task)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func NewTaskEventStore(name string, dbType DBType) Store {
	var ts Store
	var errTaskDb error
	switch dbType {
	case DBTypeMemory:
		ts = newInMemoryTaskEventStore()
	case DBTypePersistent:
		ts, errTaskDb = newEventStore(fmt.Sprintf("%s.db", name), 0600, name)
	}
	if errTaskDb != nil {
		log.Fatalf("[manager] Unable to create task event store: %v", errTaskDb)
	}

	return ts
}

type eventStore struct {
	DbFile   string
	FileMode os.FileMode
	Db       *bolt.DB
	Bucket   string
}

func newEventStore(file string, mode os.FileMode, bucket string) (Store, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("[store] Unable to open %v", file)
	}
	e := eventStore{
		DbFile:   file,
		FileMode: mode,
		Db:       db,
		Bucket:   bucket,
	}

	err = e.createBucket()
	if err != nil {
		log.Printf("[store] Bucket already exists, will use it instead of creating new one")
	}

	return &e, nil
}

func (e *eventStore) Close() {
	e.Db.Close()
}

func (e *eventStore) createBucket() error {
	return e.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(e.Bucket))
		if err != nil {
			return fmt.Errorf("[store] Create bucket %s: %s", e.Bucket, err)
		}

		return nil
	})
}
func (e *eventStore) Count() (int, error) {
	eventCount := 0
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.Bucket))
		b.ForEach(func(k, v []byte) error {
			eventCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}

	return eventCount, nil
}

func (e *eventStore) Put(key string, value interface{}) error {
	return e.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.Bucket))

		buf, err := json.Marshal(value.(*task.TaskEvent))
		if err != nil {
			return err
		}

		err = b.Put([]byte(key), buf)
		if err != nil {
			log.Printf("[store] Unable to save item %s", key)
			return err
		}
		return nil
	})
}

func (e *eventStore) Get(key string) (interface{}, error) {
	var event task.TaskEvent
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.Bucket))
		t := b.Get([]byte(key))
		if t == nil {
			return fmt.Errorf("[store] Event %v not found", key)
		}
		err := json.Unmarshal(t, &event)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &event, nil
}

func (e *eventStore) List() (interface{}, error) {
	var events []*task.TaskEvent
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.Bucket))
		b.ForEach(func(k, v []byte) error {
			var event task.TaskEvent
			err := json.Unmarshal(v, &event)
			if err != nil {
				return err
			}
			events = append(events, &event)
			return nil
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return events, nil
}
