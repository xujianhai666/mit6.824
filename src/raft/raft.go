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
	"bytes"
	"context"
	"math/rand"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"time"
	_ "net/http/pprof"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

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
type _Reason int32

const (
	_Unknown _Role = iota
	_Leader
	_Follower
	_Candidate

	_SUCCESS _Reason = iota
	_Voted
	_TermLarge
	_IndexLower
	_TIMEOUT
)

const (
	_ElectionTimeout      = 200 * time.Millisecond
	_DeltaElectionTimeout = 200 * time.Millisecond
	_HeartbeatTimeout     = 120 * time.Millisecond
	_NetworkTimeout       = 20 * time.Millisecond
)

var (
	electRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	electLock sync.Mutex
)

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
	stateLock    sync.RWMutex  // protect state related info modification
	role         _Role         // current role

	// Volatile state on all servers:
	commitIndex   int32 // index of highest log entry known to be committed
	lastApplied   int32 // index of highest log entry applied to state machine
	lastHeartbeat time.Time
	executeLock   sync.Mutex // use lock to keep fifo, reduce code len
	epoch         int32
	epochCh       chan struct{}

	// leader info
	nextIndex  []int32 // for each server, index of the next log entry to send to that server
	matchIndex []int32 // for each server, index of highest log entry known to be replicated on server

}

type StateInfo struct {
	CurrentTerm int32
	CommitIndex int32
	LastApplied int32
	Logs        []LogEntry
}

func (rf *Raft) run() {

	go func() {
		rf.stateLock.Lock()
		if rf.role == _Unknown {
			DPrintf("become candidate: [me %v] initially", rf.me)
			atomic.AddInt32(&rf.epoch, 1)
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
			rf.stateLock.Lock()
			atomic.AddInt32(&rf.epoch, 1)
			DPrintf("raft: %v closed, so exist", rf.me)
			rf.stateLock.Unlock()
			return
		case role, ok := <-rf.roleCh:
			if !ok {
				return
			}
			switch role {
			case _Leader:
				rf.stateLock.Lock()
				//DPrintf("raft: [me %v] now is leader with state: %#v", rf.me, rf.log)
				DPrintf("raft: [me %v] now is leader", rf.me)
				rf.stateLock.Unlock()
				go rf.becomeLeader()
			case _Follower:
				rf.stateLock.Lock()
				//DPrintf("raft: [me %v] now is follower %#v", rf.me, rf.log)
				DPrintf("raft: [me %v] now is follower", rf.me)
				rf.stateLock.Unlock()
				go rf.becomeFollower()
			case _Candidate:
				rf.stateLock.Lock()
				//DPrintf("raft: [me %v] now is candidate %#v", rf.me, rf.log)
				DPrintf("raft: [me %v] now is candidate", rf.me, )
				rf.stateLock.Unlock()
				go rf.becomeCandidate()
			}
		}
	}
}

func (rf *Raft) init() {
	//rf.stateLock.Lock()
	rf.closeCh = make(chan struct{})
	rf.roleCh = make(chan _Role)
	// custom structure
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	//rf.stateLock.Unlock()
}

func (rf *Raft) becomeLeader() {
	rf.stateLock.Lock()
	if rf.role == _Leader || rf.killed() {
		rf.stateLock.Unlock()
		return
	}
	rf.role = _Leader
	atomic.StoreInt32(&rf.isLeader, 1)

	rf.nextIndex = make([]int32, len(rf.peers))  // (initialized to leader last log index + 1)
	rf.matchIndex = make([]int32, len(rf.peers)) // (initialized to 0, increases monotonically)

	lastLogIndex := int32(0)
	if len(rf.log) > 1 {
		lastLogIndex = int32(len(rf.log) - 1)
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
	}

	term := rf.currentTerm
	rf.stateLock.Unlock()
	for {
		if rf.killed() {
			return
		}

		rf.stateLock.Lock()
		if atomic.LoadInt32(&rf.isLeader) != 1 || term != rf.currentTerm {
			rf.stateLock.Unlock()
			return
		}
		lasthb := rf.lastHeartbeat
		rf.stateLock.Unlock()

		if time.Since(lasthb) >= _HeartbeatTimeout {
			now := time.Now()
			alive := rf.heartbeat()
			if !alive {
				return
			}
			rf.stateLock.Lock()
			rf.lastHeartbeat = now
			rf.stateLock.Unlock()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) becomeFollower() {
	rf.stateLock.Lock()
	if rf.role == _Follower || rf.killed() {
		rf.stateLock.Unlock()
		return
	}
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
	if rf.role == _Candidate || rf.killed() {
		rf.stateLock.Unlock()
		return
	}
	rf.role = _Candidate
	atomic.StoreInt32(&rf.isLeader, 0)
	rf.stateLock.Unlock()
	var (
		maxTerm = int32(0)
		maxLog  = int32(len(rf.log) - 1)
	)

	for {
		if rf.killed() {
			rf.stateLock.Lock()
			DPrintf("killed, [me %v] exist becomeCandidate", rf.me)
			rf.stateLock.Unlock()
			return
		}

		rf.stateLock.Lock()
		if rf.role != _Candidate {
			DPrintf("nonCandidate role: %v, [me %v] exist becomeCandidate", rf.role, rf.me)
			rf.stateLock.Unlock()
			return
		}

		rf.currentTerm = rf.currentTerm + 1
		//if maxTerm >= rf.currentTerm {
		//	rf.currentTerm = maxTerm
		//}
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

		count := int32(1)
		granted := int32(1)
		quorum := int32(len(rf.peers)/2 + 1)
		maxTerm = rf.currentTerm // pick maxTerm from peer for elect next round
		rf.stateLock.Unlock()
		wg := sync.WaitGroup{}
		wg.Add(len(rf.peers) - 1)
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(idx int) {
				defer wg.Done()
				reply := &RequestVoteReply{}
				//DPrintf("[me: %v] send request with term: %v", rf.me, rf.currentTerm)
				ok := rf.sendRequestVote(idx, req, reply)
				if !ok {
					rf.stateLock.Lock()
					if req.Term != rf.currentTerm {
						DPrintf("sendRequestVote [me %v] Term != rf.currentTerm", rf.me)
						rf.stateLock.Unlock()
						return
					}

					if atomic.LoadInt32(&rf.isLeader) == 1 {
						DPrintf("sendRequestVote [me %v] rf.isLeader==1", rf.me)
						rf.stateLock.Unlock()
						return
					}

					atomic.AddInt32(&count, 1)
					failed := count-granted >= quorum
					if failed {
						DPrintf("sendRequestVote [me %v] get not granted: %v >= quorum", rf.me, count-granted)
						// elect failed, so back to origin
						rf.votedFor = -1
						rf.stateLock.Unlock()
						return
					}
					//DPrintf("sendRequestVote [me %v] return false", rf.me)
					rf.stateLock.Unlock()
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
				if ok && reply.VoteGranted {
					atomic.AddInt32(&granted, 1)
				} else {
					if maxLog < reply.LastLog {
						maxLog = reply.LastLog
					}
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
					maxTerm = reply.Term + 1
				}
				DPrintf("sendRequestVote [me %v] finish count: %v granted:%v", rf.me, count, granted)
			}(i)
		}
		wg.Wait()

		if rf.killed() {
			rf.stateLock.Lock()
			DPrintf("killed, [me %v] exist becomeCandidate", rf.me)
			rf.stateLock.Unlock()
			return
		}

		rf.stateLock.Lock()
		if rf.role != _Candidate {
			DPrintf("nonCandidate role: %v, [me %v] exist becomeCandidate", rf.role, rf.me)
			rf.stateLock.Unlock()
			return
		}
		if maxTerm >= rf.currentTerm {
			rf.currentTerm = maxTerm
		}
		electTimeout := _ElectionTimeout + time.Duration(rand.Intn(int(_DeltaElectionTimeout)))
		if maxLog > int32(len(rf.log)-1) {
			DPrintf("maxLog triggered\n")
			electTimeout = 2 * _ElectionTimeout
		}
		DPrintf("[me %v] elect timeout: %v with term: %v\n", rf.me, electTimeout, rf.currentTerm)
		rf.stateLock.Unlock()
		time.Sleep(electTimeout)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	return int(rf.currentTerm), atomic.LoadInt32(&rf.isLeader) == 1
}

func (rf *Raft) IsLeader() bool {
	return atomic.LoadInt32(&rf.isLeader) == 1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	state := &StateInfo{
		CurrentTerm: rf.currentTerm,
		CommitIndex: rf.commitIndex,
		LastApplied: rf.lastApplied,
		Logs:        rf.log,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//DPrintf("rf [me %v] save stateInfo: %#v", rf.me, state)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	state := &StateInfo{}

	if err := d.Decode(state); err != nil {
		panic(err)
	}
	rf.currentTerm = state.CurrentTerm
	rf.commitIndex = state.CommitIndex
	rf.lastApplied = state.LastApplied
	rf.log = state.Logs
	DPrintf("rf [me %v] read stateInfo: %#v", rf.me, state)
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
	LastLog     int32 // last log index
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.executeLock.Lock()
	defer rf.executeLock.Unlock()

	//DPrintf("[ReceiveRequestVote] [me %v] from [peer %v] start", rf.me, args.CandidateId)
	rf.stateLock.Lock()
	DPrintf("[ReceiveRequestVote] [me %v] log: %v term: %v from [peer %v] start", rf.me, len(rf.log), rf.currentTerm, args)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	reply.LastLog = int32(len(rf.log) - 1)
	if args.Term < rf.currentTerm {
		DPrintf("[ReceiveRequestVote] [me %v] from %v Term :%v <= currentTerm: %v, return", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.stateLock.Unlock()
		return
	}

	defer rf.stateLock.Unlock()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := int32(0)
		lastLogTerm := int32(0)
		if len(rf.log) > 1 {
			lastLogIndex = int32(len(rf.log) - 1)
			lastLogTerm = rf.log[lastLogIndex].Term
		}

		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			rf.votedFor = -1
			rf.lastHeartbeat = time.Now()
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

	LogIndexHint int32
}

func (rf *Raft) heartbeat() bool {
	// heartbeat
	rf.stateLock.Lock()
	preLogIndex := int32(0)
	preLogTerm := int32(0)
	if len(rf.log) > 1 {
		preLogIndex = int32(len(rf.log) - 1)
		preLogTerm = rf.log[preLogIndex].Term
	}
	appendReq := AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  preLogTerm,
		//Entries      : nil,
		LeaderCommit: rf.commitIndex,
	}
	rf.stateLock.Unlock()

	return rf.quorumHeartbeat(appendReq)
}

func (rf *Raft) quorumHeartbeat(appendReq AppendEntriesRequest) bool {
	count := int32(1)
	success := int32(1)
	quorum := int32(len(rf.peers)/2 + 1)
	waitCh := make(chan struct{}, len(rf.peers))
	for i, _ := range rf.peers {
		go func(idx int, appendReq AppendEntriesRequest) {

			for {

				if idx == rf.me {
					return
				}

				if !rf.IsLeader() {
					waitCh <- struct{}{}
					return
				}

				appendReply := &AppendEntriesResponse{}

				r := &AppendEntriesRequest{
					Term:         appendReq.Term,
					LeaderId:     appendReq.LeaderId,
					PrevLogIndex: appendReq.PrevLogIndex,
					PrevLogTerm:  appendReq.PrevLogTerm,
					Entries:      appendReq.Entries,
					LeaderCommit: appendReq.LeaderCommit,
				}
				ok := rf.sendAppendEntries(idx, r, appendReply)
				if !ok {
					rf.stateLock.Lock()
					atomic.AddInt32(&count, 1)
					rf.stateLock.Unlock()
					break
				}

				if appendReply.Success {
					rf.stateLock.Lock()
					atomic.AddInt32(&count, 1)
					atomic.AddInt32(&success, 1)

					// 目前使用 同步顺序发送, 所以delta=1
					rf.matchIndex[idx] = appendReq.PrevLogIndex
					rf.nextIndex[idx] = appendReq.PrevLogIndex + 1

					// update majority agreement
					matchIndexes := make([]int, len(rf.peers))
					for i, _ := range rf.matchIndex {
						matchIndexes[i] = int(rf.matchIndex[i])
					}
					sort.Ints(matchIndexes)
					i := (len(rf.peers) + 1) / 2
					majority := matchIndexes[i-1]

					// channel notify
					if int32(majority) > rf.commitIndex {
						rf.commitIndex = int32(majority)
						// trigger apply
						for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
							rf.stateMachine <- ApplyMsg{
								CommandValid: true,
								Command:      rf.log[i].Command,
								CommandIndex: int(i),
							}
						}
						rf.persist()
					}
					rf.stateLock.Unlock()
					break
				}

				rf.stateLock.Lock()
				if rf.currentTerm < appendReply.Term {
					DPrintf("quorumHeartbeat leader %v term is lower", rf.me)
					rf.stateLock.Unlock()
					rf.roleCh <- _Follower
					waitCh <- struct{}{}
					return
				}

				// TODO(zro.xu): term pointer is better
				DPrintf("quorumHeartbeat resend from %v to %v indicator: %v", rf.me, idx, appendReply.LogIndexHint)
				rf.nextIndex[idx] = appendReply.LogIndexHint + 1

				appendReq.PrevLogTerm = rf.log[appendReply.LogIndexHint].Term
				appendReq.PrevLogIndex = appendReply.LogIndexHint
				appendReq.LeaderCommit = rf.commitIndex
				appendReq.Entries = rf.log[appendReply.LogIndexHint+1:]
				rf.stateLock.Unlock()
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
		}(i, appendReq)
	}
	<-waitCh
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	return atomic.LoadInt32(&success) >= quorum
}

func (rf *Raft) quorumSendAppendEntries(req AppendEntriesRequest) bool {
	count := int32(1)
	success := int32(1)
	quorum := int32(len(rf.peers)/2 + 1)
	waitCh := make(chan struct{}, len(rf.peers))
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers))
	//start := time.Now()
	for i, _ := range rf.peers {
		go func(idx int, appendReq AppendEntriesRequest) {
			defer wg.Done()
			for { // retry fro log inconsistency

				if idx == rf.me {
					return
				}

				if !rf.IsLeader() {
					DPrintf("not leader, existed")
					waitCh <- struct{}{}
					return
				}

				appendReply := &AppendEntriesResponse{}
				ok := rf.sendAppendEntries(idx, &appendReq, appendReply)

				if !ok {
					rf.stateLock.Lock()
					atomic.AddInt32(&count, 1)
					rf.stateLock.Unlock()
					break
				}
				if appendReply.Success {
					rf.stateLock.Lock()

					// 目前使用 同步顺序发送, 所以delta=1
					rf.matchIndex[idx] = int32(len(rf.log)) - 1
					rf.nextIndex[idx] = int32(len(rf.log))

					// update majority agreement
					matchIndexes := make([]int, len(rf.peers))
					for i, _ := range rf.matchIndex {
						matchIndexes[i] = int(rf.matchIndex[i])
						DPrintf("match %v index: %v for end index: %v\n", i, matchIndexes[i], len(rf.log)-1)
					}
					sort.Ints(matchIndexes)
					i := (len(rf.peers) + 1) / 2
					majority := matchIndexes[i-1]

					// channel notify
					if int32(majority) > rf.commitIndex {
						rf.commitIndex = int32(majority)
						// trigger apply
						for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
							rf.stateMachine <- ApplyMsg{
								CommandValid: true,
								Command:      rf.log[i].Command,
								CommandIndex: int(i),
							}
						}
						rf.lastApplied = rf.commitIndex
						rf.persist()
						// 发送 commitIndex 变更事件, 这里不需要
					}
					// 等待 commitIndex 到最新值
					rf.stateLock.Unlock()
					for {
						rf.stateLock.Lock()
						if rf.commitIndex == int32(len(rf.log)-1) {
							rf.stateLock.Unlock()
							break
						}
						rf.stateLock.Unlock()
						time.Sleep(10 * time.Millisecond)
					}

					rf.stateLock.Lock()
					atomic.AddInt32(&count, 1)
					atomic.AddInt32(&success, 1)
					rf.stateLock.Unlock()
					break
				}
				// not success reason as below:
				// Reply false if term < currentTerm, exit
				// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
				// turn index
				rf.stateLock.Lock()
				if rf.currentTerm < appendReply.Term {
					DPrintf("quorumSendAppendEntries leader %v term is lower", rf.me)
					rf.stateLock.Unlock()
					rf.roleCh <- _Follower
					waitCh <- struct{}{}
					return
				}

				DPrintf("quorumSendAppendEntries resend from %v to %v indicator: %v to end: %v",
					rf.me, idx, appendReply.LogIndexHint, len(rf.log)-1)
				rf.nextIndex[idx] = appendReply.LogIndexHint + 1
				appendReq.PrevLogTerm = rf.log[appendReply.LogIndexHint].Term
				appendReq.PrevLogIndex = appendReply.LogIndexHint
				appendReq.LeaderCommit = rf.commitIndex
				appendReq.Entries = rf.log[appendReply.LogIndexHint+1:]
				rf.stateLock.Unlock()
			}

			// 应该是个 请求处理的锁.
			rf.stateLock.Lock()
			if count-success >= quorum {
				// 失败的话，就不是leader了
				rf.stateLock.Unlock()
				rf.roleCh <- _Follower
				waitCh <- struct{}{}
				return
			}
			if success >= quorum {
				rf.stateLock.Unlock()
				waitCh <- struct{}{}
				return
			}
			rf.stateLock.Unlock()
		}(i, req)
	}
	<-waitCh
	wg.Wait()
	//gap := time.Since(start)
	//if gap < _HeartbeatTimeout {
	//time.Sleep(_HeartbeatTimeout)
	//}
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
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
		electTimeout := _ElectionTimeout + time.Duration(rand.Intn(int(_DeltaElectionTimeout)))
		rf.stateLock.Unlock()
		time.Sleep(electTimeout)
	}
}

// follower append 直接 commit, 然后apply
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	rf.executeLock.Lock()
	defer rf.executeLock.Unlock()

	//DPrintf("[ReceiveAppendEntries] [me %v] from [peer: %v]", rf.me, args.LeaderId)
	reply.Success = false
	rf.stateLock.Lock()
	// use defer
	reply.Term = rf.currentTerm
	reply.LogIndexHint = -1

	if rf.role == _Unknown {
		DPrintf("[ReceiveAppendEntries] [me %v] is changing role", rf.me)
		reply.Success = true
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
			reply.Success = true
			rf.stateLock.Unlock()
			rf.roleCh <- _Follower
			return
		}
	}

	if rf.role == _Candidate {
		DPrintf("[ReceiveAppendEntries] [me %v] role is %v, so convert to follower", rf.me, rf.role)
		rf.stateLock.Unlock()
		// how to check whether new leader
		reply.Success = true
		rf.roleCh <- _Follower
		return
	}

	if int32(len(rf.log))-1 < args.PrevLogIndex || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		DPrintf("[ReceiveAppendEntries] [me %v] log %v is lower %v, exist", rf.me, int32(len(rf.log))-1, args.PrevLogIndex)
		rf.log = rf.log[:rf.commitIndex+1]
		reply.LogIndexHint = rf.commitIndex
		rf.lastHeartbeat = time.Now()
		rf.stateLock.Unlock()
		return
	}

	if len(args.Entries) > 0 {
		DPrintf("add entries from preIndex: %v", args.PrevLogIndex)
		rf.log = rf.log[:args.PrevLogIndex+1]
		for _, e := range args.Entries {
			rf.log = append(rf.log, LogEntry{
				Command: e.Command,
				Term:    e.Term,
			})
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		commit := args.LeaderCommit
		if commit > int32(len(rf.log)-1) {
			commit = int32(len(rf.log) - 1)
		}
		rf.commitIndex = commit
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.stateMachine <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: int(i),
		}
		rf.lastApplied = i
	}
	//DPrintf("[me: %v]persist for appendEntries", rf.me)
	rf.persist()
	//rf.lastApplied = rf.commitIndex
	rf.lastHeartbeat = time.Now()
	reply.Success = true
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
	index := -1
	term := int32(-1)
	isLeader := true

	_, isLeader = rf.GetState()
	if !isLeader {
		return index, int(term), false
	}

	// TODO: take concurrent out-of-order commit to account
	rf.stateLock.Lock()
	preLogIndex := int32(0)
	preLogTerm := int32(0)
	if len(rf.log) > 1 {
		preLogIndex = int32(len(rf.log) - 1)
		preLogTerm = rf.log[preLogIndex].Term
	}
	term = rf.currentTerm
	newEntry := LogEntry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, newEntry)
	//rf.persist()
	rf.matchIndex[rf.me] = int32(len(rf.log) - 1)
	entries := []LogEntry{newEntry}
	appendReq := &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex, // change
		PrevLogTerm:  preLogTerm,  // change
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.stateLock.Unlock()

	quorumAck := rf.quorumSendAppendEntries(*appendReq)
	if !quorumAck {
		return int(preLogIndex) + 1, int(term), true
	}

	// Your code here (2B).
	return int(preLogIndex) + 1, int(term), isLeader
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
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	rf.persist()
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
	rf.init()
	rf.readPersist(persister.ReadRaftState())
	go rf.run()
	DPrintf("Success make")
	return rf
}

func init() {
	runtime.SetBlockProfileRate(500 * 1000 * 1000)
	runtime.SetMutexProfileFraction(500 * 1000 * 1000)

	http.ListenAndServe(":8088", nil)
}
