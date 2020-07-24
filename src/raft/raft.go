package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int32
}

type _Role int32

const (
	_Unknown _Role = iota
	_Leader
	_Follower
	_Candidate
)

const (
	_ElectionTimeout      = 240 * time.Millisecond
	_DeltaElectionTimeout = 200 * time.Millisecond
	_HeartbeatTimeout     = 120 * time.Millisecond
	_NetworkTimeout       = 60 * time.Millisecond
)

var (
	electRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	electLock sync.Mutex
)

// 落后的follower怎么发现新的leader, 等leader发送信息
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	closeCh   chan struct{}       // close by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state
	currentTerm  int32         // latest Term server has seen
	votedFor     int           // candidateId that received vote in current Term (or null if none)
	log          []LogEntry    // log entries
	stateMachine chan ApplyMsg // message store
	isLeader     int32         // check identity
	roleCh       chan _Role    // role change notification
	stateLock    sync.Mutex    // protect state related info modification
	role         _Role         // current role

	executeLock sync.Mutex // use lock to keep fifo, reduce code len

	// Volatile state on all servers:
	commitIndex int32 // index of highest log entry known to be committed
	lastApplied int32 // index of highest log entry applied to state machine

	lastHeartbeat time.Time

	// leader info
	nextIndex  []int32 // for each server, index of the next log entry to send to that server
	matchIndex []int32 // for each server, index of highest log entry known to be replicated on server
}

// PROBLEM: voteFor change,
func (rf *Raft) run() {
	rf.init()

	go func() {
		rf.stateLock.Lock()
		if rf.role == _Unknown {
			DPrintf("become candidate: [me %v] initially", rf.me)
			rf.roleCh <- _Candidate
		}
		rf.stateLock.Unlock()
	}()

	for {
		if rf.killed() {
			return
		}

		select {
		case <-rf.closeCh:
			DPrintf("raft: %v closed, so exist", rf.me)
			return
		case role, ok := <-rf.roleCh:
			if !ok {
				return
			}
			switch role {
			case _Leader:
				DPrintf("raft: [me %v] now is leader", rf.me)
				go rf.becomeLeader()
			case _Follower:
				DPrintf("raft: [me %v] now is follower", rf.me)
				go rf.becomeFollower()
			case _Candidate:
				DPrintf("raft: [me %v] now is candidate", rf.me)
				go rf.becomeCandidate()
			}
		}
	}
}

func (rf *Raft) init() {
	rf.closeCh = make(chan struct{})
	rf.roleCh = make(chan _Role)
	// custom structure
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
}

func (rf *Raft) becomeLeader() {
	rf.stateLock.Lock()
	if rf.role == _Leader || rf.killed() {
		rf.stateLock.Unlock()
		return
	}
	rf.role = _Leader
	atomic.StoreInt32(&rf.isLeader, 1)
	rf.stateLock.Unlock()
	rf.nextIndex = make([]int32, len(rf.peers))
	rf.matchIndex = make([]int32, len(rf.peers))

	for {
		if rf.killed() {
			return
		}

		rf.stateLock.Lock()
		if atomic.LoadInt32(&rf.isLeader) != 1 {
			rf.stateLock.Unlock()
			return
		}
		rf.stateLock.Unlock()

		startT := time.Now()
		alive := rf.heartbeat()
		if !alive {
			return
		}
		ticks := time.Since(startT)
		time.Sleep(_HeartbeatTimeout - ticks)
	}
}

func (rf *Raft) becomeFollower() {
	rf.stateLock.Lock()
	rf.role = _Follower
	atomic.StoreInt32(&rf.isLeader, 0)
	rf.lastHeartbeat = time.Now()
	rf.stateLock.Unlock()
	go rf.monitorLeader()
}

// becomeCandidate turn to be candidate, try to elect to be leader.
// WARNING: elect a new leader within five seconds of the failure of the old leader, raft mentions
// election timeouts in the range of 150 to 300 milliseconds.  tester limits you to 10 heartbeats per second.
func (rf *Raft) becomeCandidate() {
	rf.stateLock.Lock()
	rf.role = _Candidate
	atomic.StoreInt32(&rf.isLeader, 0)
	rf.stateLock.Unlock()
	var (
		maxTerm = int32(0)
	)

	for {
		if rf.killed() {
			DPrintf("killed, [me %v] exist becomeCandidate", rf.me)
			return
		}

		rf.stateLock.Lock()
		if rf.role != _Candidate {
			DPrintf("nonCandidate role: %v, [me %v] exist becomeCandidate", rf.role, rf.me)
			rf.stateLock.Unlock()
			return
		}
		//rf.stateLock.Unlock()

		// 选举超时、心跳、voter 超时的问题: 另外两个超时?
		electTimeout := _ElectionTimeout + time.Duration(rand.Intn(int(_DeltaElectionTimeout)))
		DPrintf("[me %v] elect timeout: %v\n", rf.me, electTimeout)

		rf.currentTerm = rf.currentTerm + 1
		if maxTerm > rf.currentTerm {
			rf.currentTerm = maxTerm
		}
		rf.votedFor = rf.me

		lastTerm := int32(0)
		lastLogIndex := int32(0)
		if len(rf.log) > 1 {
			lastLogIndex = int32(len(rf.log) - 1)
			lastTerm = rf.log[lastLogIndex].Term
		}
		req := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastTerm,
		}
		rf.stateLock.Unlock()

		count := int32(1)
		granted := int32(1)
		quorum := int32(len(rf.peers)/2 + 1)
		maxTerm := rf.currentTerm // pick maxTerm from peer for elect next round
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			//DPrintf("prepare send vote for peer: %v\n", i)
			go func(idx int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(idx, req, reply)
				if !ok {
					if req.Term != rf.currentTerm {
						DPrintf("sendRequestVote [me %v] Term != rf.currentTerm", rf.me)
						return
					}

					if atomic.LoadInt32(&rf.isLeader) == 1 {
						DPrintf("sendRequestVote [me %v] rf.isLeader==1", rf.me)
						return
					}

					atomic.AddInt32(&count, 1)
					if count-granted >= quorum {
						DPrintf("sendRequestVote [me %v] get not granted >= quorum", rf.me)
						// elect failed, so back to origin
						rf.votedFor = -1
						return
					}
					DPrintf("sendRequestVote [me %v] return false", rf.me)
					return
				}

				rf.stateLock.Lock()
				defer rf.stateLock.Unlock()

				if req.Term != rf.currentTerm {
					DPrintf("sendRequestVote [me %v] Term != rf.currentTerm", rf.me)
					return
				}

				if atomic.LoadInt32(&rf.isLeader) == 1 {
					DPrintf("sendRequestVote [me %v] rf.isLeader==1", rf.me)
					return
				}

				atomic.AddInt32(&count, 1)
				if reply.VoteGranted {
					atomic.AddInt32(&granted, 1)
				}

				if granted >= quorum {
					DPrintf("sendRequestVote [me %v] get granted >= quorum", rf.me)
					rf.roleCh <- _Leader
					return
				}
				if count-granted >= quorum {
					DPrintf("sendRequestVote [me %v] get not granted >= quorum", rf.me)
					// elect failed, so back to origin
					rf.votedFor = -1
					return
				}
				if maxTerm < reply.Term {
					maxTerm = reply.Term
				}
				DPrintf("sendRequestVote [me %v] finish count: %v granted:%v", rf.me, count, granted)
			}(i)
		}
		time.Sleep(electTimeout)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.currentTerm), atomic.LoadInt32(&rf.isLeader) == 1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32 // candidate’s Term
	CandidateId  int   // candidate requesting vote
	LastLogIndex int32 // index of candidate’s last log entry
	LastLogTerm  int32 // Term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32 // currentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.executeLock.Lock()
	defer rf.executeLock.Unlock()

	DPrintf("[ReceiveRequestVote] [me %v] from [peer %v] start", rf.me, args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		DPrintf("[ReceiveRequestVote] [me %v] Term <= currentTerm, return", rf.me)
		return
	}

	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := int32(0)
		lastLogTerm := int32(0)
		if len(rf.log) > 1 {
			lastLogIndex = int32(len(rf.log) - 1)
			lastLogTerm = rf.log[lastLogIndex].Term
		}

		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			DPrintf("[ReceiveRequestVote] [me %v] index is oldest, return", rf.me)
			return
		}

		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		if rf.role != _Follower {
			DPrintf("[ReceiveRequestVote] [me %v] become follower", rf.me)
			rf.roleCh <- _Follower
		}

		reply.VoteGranted = true
		reply.Term = args.Term
		// [WARNING] 一旦授权，应该重置超时
		rf.lastHeartbeat = time.Now()
		DPrintf("[ReceiveRequestVote] [me %v] granted vote", rf.me)
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ctx := context.Background()
	ctx, cancelF := context.WithTimeout(ctx, _NetworkTimeout)
	defer cancelF()
	waitCh := make(chan struct{})
	var ok bool
	go func() {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		waitCh <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return false
	case <-waitCh:
		return ok
	}
}

type AppendEntriesRequest struct {
	Term         int32      // leader’s Term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int32      // index of log entry immediately preceding new ones
	PrevLogTerm  int32      // Term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int32      // leader’s commitIndex
}

type AppendEntriesResponse struct {
	Term    int32 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and PrevLogTerm
}

func (rf *Raft) heartbeat() bool {
	// heartbeat
	preLogIndex := int32(0)
	preLogTerm := int32(0)
	if len(rf.log) > 1 {
		preLogIndex = int32(len(rf.log) - 1)
		preLogTerm = rf.log[preLogIndex].Term
	}
	appendReq := &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  preLogTerm,
		//Entries      : nil,
		LeaderCommit: rf.commitIndex,
	}

	count := int32(1)
	success := int32(1)
	quorum := int32(len(rf.peers)/2 + 1)
	waitCh := make(chan struct{})
	for i, _ := range rf.peers {
		go func(idx int) {
			if idx == rf.me {
				return
			}
			appendReply := &AppendEntriesResponse{}
			ok := rf.sendAppendEntries(idx, appendReq, appendReply)
			atomic.AddInt32(&count, 1)
			if ok {
				atomic.AddInt32(&success, 1)
			}
			rf.stateLock.Lock()
			if success >= quorum {
				rf.stateLock.Unlock()
				waitCh <- struct{}{}
				return
			}
			if count-success >= quorum {
				rf.stateLock.Unlock()
				waitCh <- struct{}{}
				return
			}
			rf.stateLock.Unlock()
			//rf.peers[idx].Call("Raft.AppendEntries", appendReq, appendReply)

		}(i)
	}
	// wait
	<-waitCh
	return success >= quorum
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ctx := context.Background()
	ctx, cancelF := context.WithTimeout(ctx, _NetworkTimeout)
	defer cancelF()
	waitCh := make(chan struct{})
	var ok bool
	go func() {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		waitCh <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return false
	case <-waitCh:
		return ok
	}
}

func (rf *Raft) monitorLeader() {
	for {
		DPrintf("[monitorLeader] [me %v] monitor leader", rf.me)
		if rf.killed() {
			return
		}
		rf.stateLock.Lock()
		if rf.role != _Follower {
			DPrintf("[monitorLeader] [me %v] exit due to role: %v", rf.me, rf.role)
			rf.stateLock.Unlock()
			return
		}
		if time.Since(rf.lastHeartbeat) > _ElectionTimeout {
			DPrintf("lastHeartbeat [me %v] is timeout, so change to candidate", rf.me)
			rf.stateLock.Unlock()
			rf.roleCh <- _Candidate
			return
		}
		rf.stateLock.Unlock()
		time.Sleep(_ElectionTimeout)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	rf.executeLock.Lock()
	defer rf.executeLock.Unlock()

	DPrintf("[ReceiveAppendEntries] [me %v] from [peer: %v]", rf.me, args.LeaderId)
	reply.Success = false
	rf.stateLock.Lock()
	// use defer
	reply.Term = rf.currentTerm

	if rf.role == _Unknown {
		DPrintf("[ReceiveAppendEntries] [me %v] is changing role", rf.me)
		rf.stateLock.Unlock()
		return
	}

	if args.Term < rf.currentTerm {
		DPrintf("[ReceiveAppendEntries] [me %v] args Term is lower, exist", rf.me)
		rf.stateLock.Unlock()
		return
	}

	if rf.role == _Leader {
		switch {
		case args.Term == rf.currentTerm:
			DPrintf("[ReceiveAppendEntries] [me %v] term conflict", rf.me)
			rf.role = _Unknown
			rf.stateLock.Unlock()
			rf.roleCh <- _Follower
			return
		case args.Term > rf.currentTerm:
			DPrintf("[ReceiveAppendEntries] [me %v] role is leader, receive high term, so convert to follower", rf.me)
			// how to check whether new leader
			rf.role = _Unknown
			rf.stateLock.Unlock()
			rf.roleCh <- _Follower
			return
		}
	}

	if rf.role == _Candidate {
		DPrintf("[ReceiveAppendEntries] [me %v] role is %v, so convert to follower", rf.me, rf.role)
		rf.stateLock.Unlock()
		// how to check whether new leader
		rf.roleCh <- _Follower
		return
	}

	if int32(len(rf.log))-1 < args.PrevLogIndex {
		DPrintf("[ReceiveAppendEntries] [me %v] log is lower, exist", rf.me)
		rf.stateLock.Unlock()
		return
	}

	entry := rf.log[args.PrevLogIndex]
	if entry.Term != args.PrevLogTerm {
		DPrintf("[ReceiveAppendEntries] [me %v] Term is not equal, exist", rf.me)
		rf.log = rf.log[:args.PrevLogIndex-1]
	}

	if len(args.Entries) == 0 {
		DPrintf("[ReceiveAppendEntries] [me %v] entries is 0, just heartbeat", rf.me)
		rf.lastHeartbeat = time.Now()
		rf.stateLock.Unlock()
		return
	}

	for _, e := range args.Entries {
		rf.log = append(rf.log, LogEntry{
			Command: e.Command,
			Term:    e.Term,
		})
	}
	if args.LeaderCommit > rf.commitIndex {
		commit := args.LeaderCommit
		if commit > int32(len(rf.log)-1) {
			commit = int32(len(rf.log) - 1)
		}
		rf.commitIndex = commit
	}

	rf.stateLock.Unlock()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("start command")
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// 如何将请求发过去? 后面实现下
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.closeCh)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.stateMachine = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.run()
	DPrintf("Success make")
	return rf
}
