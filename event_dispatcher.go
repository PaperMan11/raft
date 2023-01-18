package raft

import (
	"reflect"
	"sync"
)

// eventDispatcher 负责管理指定事件的侦听器，并将事件通知分派给这些侦听器
type eventDispatcher struct {
	sync.RWMutex
	source    interface{}
	listeners map[string]eventListeners
}

// EventListener 是一个可以接收事件通知的函数。
type EventListener func(Event)

type eventListeners []EventListener

func newEventDispatcher(source interface{}) *eventDispatcher {
	return &eventDispatcher{
		source:    source,
		listeners: make(map[string]eventListeners),
	}
}

// AddEventListener adds a listener function for a given event type.
func (d *eventDispatcher) AddEventListener(typ string, listener EventListener) {
	d.Lock()
	defer d.Unlock()
	d.listeners[typ] = append(d.listeners[typ], listener)
}

// RemoveEventListener removes a listener function for a given event type.
func (d *eventDispatcher) RemoveEventListener(typ string, listener EventListener) {
	d.Lock()
	defer d.Unlock()

	// Grab a reference to the function pointer once.
	// 获取函数指针
	ptr := reflect.ValueOf(listener).Pointer()

	// Find listener by pointer and remove it.
	listeners := d.listeners[typ]
	for i, l := range listeners {
		if reflect.ValueOf(l).Pointer() == ptr {
			d.listeners[typ] = append(listeners[:i], listeners[i+1:]...)
		}
	}
}

// DispatchEvent dispatches an event.
//
// 执行所有绑定的事件函数
func (d *eventDispatcher) DispatchEvent(e Event) {
	d.RLock()
	defer d.RUnlock()

	// Automatically set the event source.
	if e, ok := e.(*event); ok {
		e.source = d.source
	}

	// Dispatch the event to all listeners.
	for _, l := range d.listeners[e.Type()] {
		l(e)
	}
}
