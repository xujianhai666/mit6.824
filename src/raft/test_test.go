package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"testing"
)
import "fmt"
import "time"
import "math/rand"
import "sync/atomic"
import "sync"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): initial election")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the Term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("Term is %v, but should be at least 1", term1)
	}

	// does the leader+Term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: Term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
}

func TestReElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	DPrintf("prepare round-0 disconnect: %v", leader1)
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader.
	DPrintf("prepare round-0 recover: %v", leader1)
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no leader should
	// be elected.
	DPrintf("prepare round-1 disconnect: %v %v", leader2, (leader2+1)%servers)
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	DPrintf("recover round-1 phase 1 connect: %v", (leader2+1)%servers)
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	DPrintf("recover round-1 phase 2 connect: %v", leader2)
	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}

func TestBasicAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}

//
// check, based on counting bytes of RPCs, that
// each command is sent to each peer just once.
//
func TestRPCBytes2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): RPC byte count")

	cfg.one(99, servers, false)
	bytes0 := cfg.bytesTotal()

	iters := 10
	var sent int64 = 0
	for index := 2; index < iters+2; index++ {
		cmd := randstring(5000)
		xindex := cfg.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}

func TestFailAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): agreement despite follower disconnection")

	cfg.one(101, servers, false)

	// disconnect one follower from the network.
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	cfg.one(103, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, servers-1, false)
	cfg.one(105, servers-1, false)

	// re-connect
	cfg.connect((leader + 1) % servers)

	// the full set of servers should preserve
	// previous agreements, and be able to agree
	// on new commands.
	cfg.one(106, servers, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, servers, true)

	cfg.end()
}

func TestFailNoAgree2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): no agreement if too many followers disconnect")

	cfg.one(10, servers, false)
	DPrintf("pass 10 command")

	// 3 of 5 followers disconnect
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.rafts[leader].Start(20)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}
	DPrintf("pass 20 command")

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}
	DPrintf("pass no majority")

	// repair
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}
	DPrintf("pass 30")

	cfg.one(1000, servers, true)
	DPrintf("pass 1000")

	cfg.end()
}

func TestConcurrentStarts2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): concurrent Start()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// Term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("Term changed too often")
	}

	cfg.end()
}

func TestRejoin2B(t *testing.T) {
	//Debug = 0
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): rejoin of partitioned leader")

	cfg.one(101, servers, true)

	// leader network failure
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// make old leader try to agree on some entries
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)

	// new leader commits, also for index=2
	cfg.one(103, 2, true)

	// new leader network failure
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// old leader connected again
	cfg.connect(leader1)

	//Debug = 1
	//DPrintf("start 104")
	cfg.one(104, 2, true)

	// all together now
	cfg.connect(leader2)

	cfg.one(105, servers, true)

	cfg.end()
}

func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")

	t.Log("success a random: round-0\n")
	cfg.one(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	t.Log("failed lots of command: round-1\n")
	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	t.Log("success lots of command: round-2\n")
	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	t.Log("fail lots of command: round-3\n")
	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// lots of successful commands to new group.
	t.Log("success lots of command: round-4\n")
	for i := 0; i < 50; i++ {
		t.Logf("success lots of command: round-4 for i:%v\n", i)
		cfg.one(rand.Int(), 3, true)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	t.Log("success a command: round-5\n")
	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestCount2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): RPC counts aren't too high")

	// 写网络
	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so Term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// Term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// Term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		// iter + 1
		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) over %v for %v entries\n", total2-total1, (iters+1+3)*3, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("Term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	cfg.end()
}

func TestPersist12C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): basic persistence")

	cfg.one(11, servers, true)
	DPrintf("ONE 11 successs")

	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, servers, true)

	DPrintf("ONE SUCCESS")
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	DPrintf("disconnect SUCCESS")
	cfg.start1(leader1)
	DPrintf("START1 SUCCESS")
	cfg.connect(leader1)
	DPrintf("connect SUCCESS")

	cfg.one(13, servers, true)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1, true)
	cfg.start1(leader2)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1, true)
	cfg.start1(i3)
	cfg.connect(i3)

	cfg.one(16, servers, true)

	cfg.end()
}

func TestPersist22C(t *testing.T) {
	//Debug = 0
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers, true)  // 11
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2, true)  // 12
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		//atomic.StoreInt32(&Debug, 1)
		DPrintf("start with : %v", 10 + index)
		cfg.start1((leader1 + 1) % servers)
		cfg.start1((leader1 + 2) % servers)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		cfg.start1((leader1 + 3) % servers)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2, true)  // 13, 就三个
		index++
		DPrintf("end with : %v", 10 + index)

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestPersist32C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): partitioned leader and one follower crash, leader restarts")

	cfg.one(101, 3, true)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % servers)

	cfg.one(102, 2, true)

	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.start1((leader + 0) % servers)
	cfg.connect((leader + 0) % servers)

	cfg.one(103, 2, true)

	cfg.start1((leader + 1) % servers)
	cfg.connect((leader + 1) % servers)

	cfg.one(104, servers, true)

	cfg.end()
}

//
// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new Term may try to finish replicating log entries that
// haven't been committed yet.
//
func TestFigure82C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8")

	index := 0
	cfg.one(rand.Int(), 1, true)
	index = index + 1

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if cfg.rafts[i] != nil {
				// entries 需要保持一致. 后退的兼容性
				_, _, ok := cfg.rafts[i].Start(rand.Int())
				if ok {
					DPrintf("success for index : %v", index)
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.rafts[s] == nil {
				cfg.start1(s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestUnreliableAgree2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	cfg.begin("Test (2C): unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(x, y int) {
				defer wg.Done()
				cfg.one((100*x)+y, 1, true)
			}(iters, j)
		}
		cfg.one(iters, 1, true)
	}

	cfg.setunreliable(false)

	wg.Wait()

	cfg.one(100, servers, true)

	cfg.end()
}

func TestFigure8Unreliable2C(t *testing.T) {
	//f, err := os.Create("output")
	//if err != nil {
	//	panic(err)
	//}
	//pprof.StartCPUProfile(f)
	//
	//
	//tf, err := os.Create("trace")
	//if err != nil {
	//	panic(err)
	//}
	//trace.Start(tf)
	//
	//defer func() {
	//	trace.Stop()
	//}()
	//
	//defer func() {
	//	pprof.StopCPUProfile()
	//}()

	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8 (unreliable)")

	cfg.one(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			cfg.setlongreordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := cfg.rafts[i].Start(rand.Int() % 10000)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		// 1/10 少睡, 不然多睡 
		if (rand.Int() % 1000) < 100 {
			// 500 取余
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		// 一半的概率阻断 leader的网络
		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		// 低于半数, 尝试恢复一台
		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}

	// 尝试成功
	DPrintf("final try")
	//cfg.one(rand.Int()%10000, servers, true)
	cfg.one(1919, servers, true)

	cfg.end()
}

func internalChurn(t *testing.T, unreliable bool) {

	servers := 5
	cfg := make_config(t, servers, unreliable)
	defer cfg.cleanup()

	if unreliable {
		cfg.begin("Test (2C): unreliable churn")
	} else {
		cfg.begin("Test (2C): churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				cfg.start1(i)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				cfg.crash1(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), servers, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}

func TestReliableChurn2C(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn2C(t *testing.T) {
	//f, err := os.Create("output")
	//if err != nil {
	//	panic(err)
	//}
	//pprof.StartCPUProfile(f)
	//
	//tf, err := os.Create("trace")
	//if err != nil {
	//	panic(err)
	//}
	//trace.Start(tf)
	//defer func() {
	//
	//	trace.Stop()
	//	pprof.StopCPUProfile()
	//	f.Close()
	//	tf.Close()
	//}()

	internalChurn(t, true)
}
