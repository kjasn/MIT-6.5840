package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type RequestStatus int

const (
	RequestNotExist RequestStatus = iota
	// RequestProcessing
	RequestCompleted
)

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// requestRecord map[string]RequestStatus
	data map[string][]byte
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	log.Println("[SERVER INFO] start a get request")
	if kv.data[args.Key] == nil {
		reply.Value = ""
	} else {
		reply.Value = string(kv.data[args.Key])
	}

	reply.Status = RequestCompleted
	log.Println("[SERVER INFO] >>> Finished a Get Request~")
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// find if get the same request
	// if kv.requestRecord[args.OptionId] != RequestNotExist {
	// 	log.Println("[SERVER INFO] Server got the same request")
	// 	reply.Status = kv.requestRecord[args.OptionId]
	// 	return
	// }

	// put value
	// kv.requestRecord[args.OptionId] = RequestProcessing
	kv.data[args.Key] = []byte(args.Value)
	// kv.requestRecord[args.OptionId] = RequestCompleted
	// reply.Status = kv.requestRecord[args.OptionId]
	reply.Status = RequestCompleted
	log.Println("[SERVER INFO] >>> Finished a Put Request~")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if kv.requestRecord[args.OptionId] != RequestNotExist {
	// 	log.Println("[SERVER INFO] Server has got the same request")
	// 	reply.Status = kv.requestRecord[args.OptionId]
	// 	return
	// }

	// get old value to return
	// getReply := GetReply{}
	// kv.Get(&GetArgs{args.Key}, &getReply)
	// reply.Value = getReply.Value
	if val, ok := kv.data[args.Key]; ok {
		reply.Value = string(val)
		val = append(val, []byte(args.Value)...)
		kv.data[args.Key] = val
	} else {
		kv.data[args.Key] = []byte(args.Value)
	}

	// append value
	// kv.requestRecord[args.OptionId] = RequestProcessing
	// kv.data[args.Key] = append(kv.data[args.Key], []byte(args.Value)...)
	// kv.requestRecord[args.OptionId] = RequestCompleted
	// reply.Status = kv.requestRecord[args.OptionId]
	// ==============================================
	reply.Status = RequestCompleted
	log.Println("[SERVER INFO] >>> Finished a Append Request~")
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.data = make(map[string][]byte)
	// kv.requestRecord = make(map[string]RequestStatus)

	return kv
}
