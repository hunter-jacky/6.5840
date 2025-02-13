package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// custom state
	electionTimer *time.Timer
	leaderId      int
	state         State
	stopElectCh   chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) getTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
	Valid       bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CandidateId < 0 || args.CandidateId >= len(rf.peers) {
		return
	}
	reply.Valid = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == Candidate && rf.stopElectCh != nil {
			rf.stopElectCh <- struct{}{}
		}
		rf.state = Follower
	}

	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.updateElectionTimer()
		log.Default().Printf("server: %d, vote for server: %d, my term: %d, his term: %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	} else {
		reply.VoteGranted = false
		log.Default().Printf("server: %d, reject vote for server: %d, my term: %d, his term: %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// PrevLogIndex int
	// PrevLogTerm  int
	// Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries 前置检查
func (rf *Raft) checkAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LeaderId < 0 || args.LeaderId >= len(rf.peers) {
		reply.Term = rf.currentTerm
		reply.Success = false
		log.Default().Printf("server: %d, invalid leader id: %d", rf.me, args.LeaderId)
		return false
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		log.Default().Printf("server: %d, receive heartbeat from old leader: %d, my term: %d, leader term: %d, my state: %d", rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.state)
		return false
	}

	if args.Term > rf.currentTerm {
		log.Default().Printf("server: %d, receive heartbeat from new leader: %d, my term: %d, leader term: %d, my state: %d", rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.state)
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		rf.state = Follower
	}
	if args.Term == rf.currentTerm {
		if rf.state == Candidate && rf.stopElectCh != nil {
			rf.stopElectCh <- struct{}{}
		}
		rf.state = Follower
	}
	// reset the election timer
	rf.updateElectionTimer()
	reply.Term = rf.currentTerm
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if !rf.checkAppendEntries(args, reply) {
		return
	}
	// TODO: check if the log is consistent

	// TODO: append the entries to the log

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	HEARTBEAT_INTERVAL      = 100 * time.Millisecond
	ELECTION_TIMEOUT_BASE   = 500 * time.Millisecond
	VOTE_GRANT_TIMEOUT_BASE = 500 * time.Millisecond
)

// the heartbeat interval, follower should begin an election
// if it doesn't receive any message from the leader within this time.
func getElectionTimeoutDuration() time.Duration {
	return ELECTION_TIMEOUT_BASE + time.Duration(rand.Int63()%int64(ELECTION_TIMEOUT_BASE))
}

// the candidate should begin next election
// if it doesn't win the election within this time, and not receive any message from the leader.
func getVoteGrantTimeoutDuration() time.Duration {
	return VOTE_GRANT_TIMEOUT_BASE + time.Duration(rand.Int63()%int64(VOTE_GRANT_TIMEOUT_BASE))
}

func (rf *Raft) updateElectionTimer() {
	rf.electionTimer.Reset(getElectionTimeoutDuration())
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(i, args, reply)
		}(i)
	}
}

func (rf *Raft) doLeader() {
	for rf.getState() == Leader {
		log.Default().Printf("server: %d, send heartbeat, term: %d", rf.me, rf.getTerm())
		rf.sendHeartbeat()
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func (rf *Raft) doFollower() {
	rf.updateElectionTimer()
	<-rf.electionTimer.C
	if rf.getState() == Follower {
		rf.actionWithLock(func() {
			rf.state = Candidate
		})
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		switch rf.getState() {
		case Leader:
			rf.doLeader()
		case Follower:
			rf.doFollower()
		case Candidate:
			rf.doCandidate()
		}
	}
}

func (rf *Raft) actionWithLock(f func()) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f()
}

type ElectionResult int

const (
	Win ElectionResult = iota
	Lose
	TimeOut
)

func (rf *Raft) doCandidate() {
	for rf.getState() == Candidate {
		rf.election()
	}
}

// the election process, determine the final state of the server, if state is
// - Leader, the server has won the election
// - Follower, the server has lost the election
// - Candidate, the server has not received enough votes, timeout
// election may be stopped when meet a new leader or a bigger term
func (rf *Raft) election() {
	// create the channel only at the beginning of the election to avoid a situation
	// where the channel repeatedly sends signals for multiple reasons before receiving the signal, causing it to block
	// multiple reasons: receive the vote with a bigger term, receive the heartbeat from new leader
	rf.stopElectCh = make(chan struct{}, 1)
	defer rf.actionWithLock(func() {
		rf.stopElectCh = nil
	})

	electResCh := make(chan ElectionResult, 1)
	go rf.electionOnce(electResCh)

	select {
	case result := <-electResCh:
		switch result {
		case Win:
			log.Default().Printf("server: %d, Election won", rf.me)
			rf.actionWithLock(func() {
				rf.state = Leader
				rf.leaderId = rf.me
			})
		case Lose:
			log.Default().Printf("server: %d, Election lost", rf.me)
			rf.actionWithLock(func() {
				rf.state = Follower
			})
		case TimeOut:
			// stay in the candidate state, restart the election
			log.Default().Printf("server: %d, vote timeout, restart the election", rf.me)
		}
	case <-rf.stopElectCh:
		// another server has won the election, state has been changed
		log.Default().Printf("server: %d, stop election", rf.me)
	}
}

// wait for the votes
func (rf *Raft) electionOnce(resCh chan ElectionResult) {
	rf.mu.Lock()
	rf.currentTerm++
	log.Default().Printf("server: %d, Election timeout, start the election, new term: %d", rf.me, rf.currentTerm)
	rf.votedFor = rf.me

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	voteCh := make(chan bool, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			voteCh <- true
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(i, args, reply)
			if reply.Valid {
				voteCh <- reply.VoteGranted
				log.Default().Printf("server: %d, receive vote from server: %d, reply: %+v", rf.me, i, reply)
			}
		}(i)
	}

	// check if the candidate has won the election
	log.Default().Printf("server: %d, Wait for the votes, term: %d", rf.me, rf.getTerm())
	votes := 0
	count := 0
	for {
		select {
		case <-time.After(getVoteGrantTimeoutDuration()):
			resCh <- TimeOut
			return
		case vote := <-voteCh:
			count++
			if vote {
				votes++
			}
			log.Default().Printf("server: %d, Vote received, votes: %d, count: %d", rf.me, votes, count)
			if votes > len(rf.peers)/2 {
				resCh <- Win
				return
			}
			if count-votes > len(rf.peers)/2 {
				resCh <- Lose
				return
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetOutput(ioutil.Discard)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimer = time.NewTimer(getElectionTimeoutDuration())
	rf.leaderId = -1
	rf.state = Follower
	rf.stopElectCh = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
