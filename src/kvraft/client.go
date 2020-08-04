package kvraft

import (
	"sync/atomic"
	"time"

	"../labrpc"
)
import "crypto/rand"
import "math/big"

var identity int32

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader  int32 // cached leader info
	epoch   int32 // identity Clerk
	counter int32 // identity request
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.epoch = atomic.AddInt32(&identity, 1)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// Get return current value of key
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	leader := atomic.LoadInt32(&ck.leader)
	args := &GetArgs{
		Key:      key,
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := &GetReply{}
			index := (i + int(leader)) % len(ck.servers)
			s := ck.servers[index]
			ok := s.Call("KVServer.Get", args, reply)
			if !ok {
				DPrintf("network from %v false", index)
				continue
			}
			if reply.Err != "" {
				DPrintf("reply from :%v err: %v", index, reply.Err)
				continue
			}
			DPrintf("reply from :%v success", index)
			atomic.StoreInt32(&ck.leader, int32(index))
			return reply.Value
		}
		time.Sleep(200 * time.Millisecond)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// PutAppend put or append value to kvServer
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	leader := atomic.LoadInt32(&ck.leader)
	count := atomic.AddInt32(&ck.counter, 1)
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ClientId: ck.epoch,
		Count:    count,
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			index := (i + int(leader)) % len(ck.servers)
			s := ck.servers[index]
			reply := &PutAppendReply{}
			ok := s.Call("KVServer.PutAppend", args, reply)
			if !ok {
				DPrintf("KVServer.PutAppend from %v failed", index)
				continue
			}
			if reply.Err != "" {
				DPrintf("KVServer.PutAppend from %v failed err: %v", index, reply.Err)
				continue
			}
			if reply.Err == ErrFail {
				i = i - 1
				DPrintf("fail to req : %v", index)
			}

			DPrintf("KVServer.PutAppend success for %v", index)
			atomic.StoreInt32(&ck.leader, int32(index))
			return
		}
		DPrintf("iter failed")
		time.Sleep(100 * time.Millisecond)
	}
}

// Put key & value to kvServer
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's list value, create if not exist
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
