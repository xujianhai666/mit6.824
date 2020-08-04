package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	_ErrClosed = "closed"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big

	// Your definitions here.
	term      int
	storeLock sync.Mutex
	store     map[string]string

	persister *raft.Persister
	closing   chan struct{}
	closed    chan struct{}

	depLock          sync.Mutex
	deduplicate      map[int32]int32
	storeDepLock     sync.Mutex
	storeDeduplicate map[int32]int32

	notifyLock sync.Mutex
	notifies   map[string]chan struct{}
}

func (kv *KVServer) run() {
	defer close(kv.closed)
	for {
		select {
		case <-kv.closing:
			kv.rf.Kill()
			kv.snap()
			return
		case applied, ok := <-kv.applyCh:
			if !ok {
				return
			}
			if !applied.CommandValid {
				DPrintf("not valid info")
				continue
			}
			var pa ApplyArgs
			switch cmd := applied.Command.(type) {
			case *ApplyArgs:
				pa = *cmd
			case ApplyArgs:
				pa = cmd
			}

			switch pa.Op {
			case "Put":
				kv.storeLock.Lock()
				kv.store[pa.Key] = pa.Value
				kv.storeLock.Unlock()

				kv.notifyLock.Lock()
				key := fmt.Sprintf("%d-%d", pa.ClientId, pa.Count)
				notify, ok := kv.notifies[key]
				if ok {
					notify <- struct{}{}
					delete(kv.notifies, key)
				}
				kv.notifyLock.Unlock()
			case "Append":
				DPrintf("[me: %v] run append %v", kv.me, pa)

				kv.storeDepLock.Lock()
				if _, ok := kv.storeDeduplicate[pa.ClientId]; !ok {
					kv.storeDeduplicate[pa.ClientId] = 0
				}
				if kv.storeDeduplicate[pa.ClientId] < pa.Count {
					kv.storeDeduplicate[pa.ClientId] = pa.Count
					DPrintf("[me %v ]apply PutAppend args: %+v at %v", kv.me, pa.Value, applied.CommandIndex)

					kv.storeLock.Lock()
					if _, ok := kv.store[pa.Key]; !ok {
						kv.store[pa.Key] = ""
					}
					kv.store[pa.Key] = kv.store[pa.Key] + pa.Value

					kv.notifyLock.Lock()
					key := fmt.Sprintf("%d-%d", pa.ClientId, pa.Count)
					notify, ok := kv.notifies[key]
					if ok {
						notify <- struct{}{}
						delete(kv.notifies, key)
					}
					kv.notifyLock.Unlock()

					kv.storeLock.Unlock()
				}
				kv.storeDepLock.Unlock()
			}
		}
	}
}

func (kv *KVServer) snap() {
	state := kv.persister.ReadRaftState()

	snap := &kv.store
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snap)
	data := w.Bytes()
	kv.persister.SaveStateAndSnapshot(state, data)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = _ErrClosed
		return
	}
	DPrintf("Get args: %+v on %v", args, kv.me)

	term, isLeader := kv.rf.GetState()
	if isLeader {

		kv.mu.Lock()
		if term > kv.term { // maybe we should not need that
			time.Sleep(200 * time.Millisecond)
			kv.term = term
		}
		kv.mu.Unlock()

		kv.storeLock.Lock()
		v, ok := kv.store[args.Key]
		kv.storeLock.Unlock()
		if !ok {
			reply.Err = ErrNoKey
			return
		}
		reply.Value = v
		return
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = _ErrClosed
		return
	}

	kv.depLock.Lock()
	if _, ok := kv.deduplicate[args.ClientId]; !ok {
		kv.deduplicate[args.ClientId] = 0
	}
	if kv.deduplicate[args.ClientId] >= args.Count {
		// just return, mark success to client
		kv.depLock.Unlock()
		//kv.mu.Unlock()
		return
	}
	kv.depLock.Unlock()

	cmd := &ApplyArgs{
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
		ClientId: args.ClientId,
		Count:    args.Count,
	}
	DPrintf("[me %v] PutAppend args: %+v", kv.me, args)
	key := fmt.Sprintf("%d-%d", args.ClientId, args.Count)

	kv.notifyLock.Lock()
	notify := make(chan struct{})
	kv.notifies[key] = notify
	kv.notifyLock.Unlock()

	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		kv.notifyLock.Lock()
		delete(kv.notifies, key)
		kv.notifyLock.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	if index == -1 {
		kv.notifyLock.Lock()
		delete(kv.notifies, key)
		kv.notifyLock.Unlock()
		reply.Err = ErrFail
	}

	kv.mu.Lock()
	if term != kv.term {
		kv.term = term
	}
	kv.mu.Unlock()

	kv.depLock.Lock()
	kv.deduplicate[args.ClientId] = args.Count
	kv.depLock.Unlock()

	DPrintf("wait notify")
	<-notify
	kv.notifyLock.Lock()
	delete(kv.notifies, key)
	kv.notifyLock.Unlock()
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	close(kv.closing)
	<-kv.closed
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(raft.AppendEntriesRequest{})
	labgob.Register(raft.AppendEntriesResponse{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(ApplyArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxraftstate
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = map[string]string{}
	kv.closing = make(chan struct{})
	kv.closed = make(chan struct{})
	kv.storeDeduplicate = make(map[int32]int32)
	kv.deduplicate = make(map[int32]int32)
	kv.notifies = make(map[string]chan struct{})

	go kv.run()
	// You may need initialization code here.

	return kv
}
