package kvsrv

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.mu = sync.Mutex{}
	ck.requestId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args, reply := GetArgs{Key: key}, GetReply{Status: RequestNotExist}
	for reply.Status != RequestCompleted {
		ck.server.Call("KVServer.Get", &args, &reply)
		if reply.Status != RequestCompleted {
			log.Println("[CLIENT INFO] Fail to get key from server, retry in 1s ...")
			time.Sleep(1 * time.Second)
		}
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		// OptionId: op + "_" + strconv.Itoa(ck.generateId()),
	}
	reply := PutAppendReply{Status: RequestNotExist}
	for reply.Status != RequestCompleted {
		ck.server.Call("KVServer."+op, &args, &reply)
		if reply.Status != RequestCompleted {
			log.Printf("[CLIENT INFO] Fail to %v key to server, retry in 1s ...", op)
			time.Sleep(1 * time.Second)
		}
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) generateId() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestId++
	return ck.requestId
}
