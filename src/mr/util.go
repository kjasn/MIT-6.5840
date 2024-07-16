package mr

import "sync"

var INIT_ID = 1 

func generateID() int {
	mu := sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()
	ret := INIT_ID 
	INIT_ID ++
	return ret
}