package store

import (
	"sync"

	"github.com/Priyanshu-1729/mini-etcd/api"
)

type Store struct {
	mu       sync.RWMutex
	data     map[string]string
	watchers map[string][]chan api.Event
}

func New() *Store {
	return &Store{
		data:     make(map[string]string),
		watchers: make(map[string][]chan api.Event),
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.notify(key, api.Event{
		Type:  api.EventTypePut,
		Key:   key,
		Value: value,
	})
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	s.notify(key, api.Event{
		Type: api.EventTypeDelete,
		Key:  key,
	})
}

// Watch returns a channel that receives events for the given key
// The caller must call the returned cancel func to clean up
func (s *Store) Watch(key string) (<-chan api.Event, func()) {
	ch := make(chan api.Event, 10)
	s.mu.Lock()
	s.watchers[key] = append(s.watchers[key], ch)
	s.mu.Unlock()

	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		watchers := s.watchers[key]
		for i, w := range watchers {
			if w == ch {
				s.watchers[key] = append(watchers[:i], watchers[i+1:]...)
				close(ch)
				break
			}
		}
	}
	return ch, cancel
}

// notify must be called with s.mu held (write lock).
func (s *Store) notify(key string, event api.Event) {
	for _, ch := range s.watchers[key] {
		select {
		case ch <- event:
		default:
			// slow consumer  drop event rather than block
		}
	}
}