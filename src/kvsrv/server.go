package kvsrv

import (
	"log"
	"strings"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// requestRecord map[int64]string
	requestRecord sync.Map
	data          map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Type == DeleteCache {
		// delete(kv.requestRecord, args.OptionId)
		kv.requestRecord.Delete(args.OptionId)
		return
	}
	// find if get the same request
	// if record, ok := kv.requestRecord[args.OptionId]; ok {
	if record, ok := kv.requestRecord.Load(args.OptionId); ok {
		reply.Value = record.(string)
		return
	}

	// set old value
	oldValue := kv.data[args.Key]
	reply.Value = oldValue
	// kv.requestRecord[args.OptionId] = oldValue
	// put value
	kv.data[args.Key] = args.Value

	kv.requestRecord.Store(args.OptionId, oldValue)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Type == DeleteCache {
		// delete(kv.requestRecord, args.OptionId)
		kv.requestRecord.Delete(args.OptionId)
		return
	}

	// if record, ok := kv.requestRecord[args.OptionId]; ok {
	if record, ok := kv.requestRecord.Load(args.OptionId); ok {
		reply.Value = record.(string)
		return
	}

	var builder strings.Builder
	builder.WriteString(kv.data[args.Key])
	builder.WriteString(args.Value)

	oldValue := kv.data[args.Key]
	// kv.requestRecord[args.OptionId] = oldValue
	reply.Value = oldValue
	kv.data[args.Key] = builder.String()

	kv.requestRecord.Store(args.OptionId, oldValue)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.data = make(map[string]string)
	// kv.requestRecord = make(map[int64]string)
	kv.requestRecord = sync.Map{}

	return kv
}
