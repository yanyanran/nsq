package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1) // sync.WaitGroup.Add
	go func() {
		cb()
		w.Done() // sync.WaitGroup.Done
	}()
}
