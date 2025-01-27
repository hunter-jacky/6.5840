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

type Buffer struct {
	actionID int
	oldValue string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	db     map[string]string
	buffer map[int64]Buffer
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ActionID == kv.buffer[args.ClientID].actionID {
		reply.Value = kv.buffer[args.ClientID].oldValue
		return
	}

	kv.db[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ActionID == kv.buffer[args.ClientID].actionID {
		reply.Value = kv.buffer[args.ClientID].oldValue
		return
	}

	reply.Value = kv.db[args.Key]
	kv.db[args.Key] += args.Value

	kv.buffer[args.ClientID] = Buffer{args.ActionID, reply.Value}
}
func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.buffer = make(map[int64]Buffer)

	return kv
}
