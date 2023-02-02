package db

import "sync"

// key-value database
type DB struct {
	data map[string]string
	mu   sync.Mutex
}

func New() *DB {
	return &DB{
		data: make(map[string]string),
	}
}

func (db *DB) Get(key string) string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.data[key]
}

func (db *DB) Put(key string, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value
}
