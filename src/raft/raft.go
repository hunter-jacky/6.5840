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

	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// custom state
	applyCh       chan ApplyMsg
	applyMu       sync.Mutex
	applyCond     *sync.Cond
	electionTimer *time.Timer
	leaderId      int
	state         State
	stopElectCh   chan struct{}
	candidateTerm int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.CurrentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) getTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.CurrentTerm
}

func (rf *Raft) getState() State {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
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
	// logPrintf("server: %d, persist, term: %d", rf.me, rf.currentTerm)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil || e.Encode(rf.VotedFor) != nil || e.Encode(rf.Logs) != nil {
		log.Fatalf("server: %d, persist error", rf.me)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry = make([]LogEntry, 0)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatalf("server: %d, read persist error", rf.me)
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = logs
		logPrintf("server: %d, read persist, term: %d, votedFor: %d, len(logs): %d", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Logs))
		rf.mu.Unlock()
	}
}

// // restore previously persisted state.
// func (rf *Raft) readPersist2(data []byte) {
// 	if data == nil || len(data) < 1 { // bootstrap without any state?
// 		return
// 	}
// 	// Your code here (3C).
// 	// Example:
// 	r := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(r)
// 	var currentTerm int
// 	var votedFor int
// 	var logs []LogEntry = make([]LogEntry, 0)
// 	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
// 		log.Fatalf("server: %d, read persist error", rf.me)
// 	} else {
// 		logPrintf("server: %d, read persist22222, term: %d, votedFor: %d, len(logs): %d", rf.me, currentTerm, votedFor, len(logs))
// 	}
// }

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// logPrintf("server: %d, receive a command, term: %d", rf.me, rf.currentTerm)

	if rf.state != Leader {
		return -1, -1, false
	}

	rf.Logs = append(rf.Logs, LogEntry{Term: rf.CurrentTerm, Command: command})
	rf.persist()
	rf.nextIndex[rf.me] = len(rf.Logs)

	logPrintf("server: %d, #--------------# receive a command, %s, append to logs, term: %d, len(logs): %d", rf.me, getCommandValue(command), rf.CurrentTerm, len(rf.Logs))
	return len(rf.Logs) - 1, rf.CurrentTerm, true
}

func getCommandValue(cmd interface{}) string {
	switch v := cmd.(type) {
	case int:
		return strconv.Itoa(v)
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return "unknown type"
	}
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

func (rf *Raft) doLeader() {
	// check and init
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	logPrintf("server: %d, do leader, term: %d", rf.me, rf.CurrentTerm)
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.Logs)
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	// send heartbeat
	go rf.sendHeartbeat()

	// update the commit index
	go rf.updateCommitIndex()

	// send log entries
	for !rf.killed() && rf.getState() == Leader {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if !rf.shouldSendEntries(i) {
				continue
			}
			args := rf.prepareArgs(i, true)
			// the args may be nil when the state is changed
			if args == nil {
				return
			}
			go rf.sendAppendEntriesToOne(i, args)
		}
		time.Sleep(SLEEP_INTERVAL)
	}
}

func (rf *Raft) prepareArgs(i int, withEntry bool) *AppendEntriesArgs {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	if rf.state != Leader {
		return nil
	}
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm:  rf.Logs[rf.nextIndex[i]-1].Term,
		Entries:      []LogEntry{},
	}
	if withEntry {
		entries := make([]LogEntry, len(rf.Logs[rf.nextIndex[i]:]))
		copy(entries, rf.Logs[rf.nextIndex[i]:])
		args.Entries = entries

		logPrintf("server: %d, send entries to server: %d, term: %d, len(entries): %d, nextIndex: %d, commit index: %d", rf.me, i, rf.CurrentTerm, len(args.Entries), rf.nextIndex[i], rf.commitIndex)
	} else {
		logPrintf("server: %d, send heartbeat to server: %d, term: %d, nextIndex: %d, commit index: %d", rf.me, i, rf.CurrentTerm, rf.nextIndex[i], rf.commitIndex)
	}
	return args
}

func (rf *Raft) sendAppendEntriesToOne(i int, args *AppendEntriesArgs) {
	if args == nil && rf.getState() != Leader {
		return
	}

	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(i, args, reply) {
		return
	}

	// resolve the reply
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// state and term may be changed by other reply when receive the current reply
	if !rf.killed() && rf.state == Leader && rf.CurrentTerm == args.Term {
		if len(args.Entries) > 0 {
			logPrintf("server: %d, receive reply from server: %d, term: %d, reply.success: %v, reply.term: %d, state: %d", rf.me, i, rf.CurrentTerm, reply.Success, reply.Term, rf.state)
		} else {
			logPrintf("server: %d, receive heartbeat reply from server: %d, term: %d, reply.success: %v, reply.term: %d, state: %d", rf.me, i, rf.CurrentTerm, reply.Success, reply.Term, rf.state)
		}

		if reply.Success {
			// The message sent later may return earlier, causing the value of nextIndex[i] to be updated to a larger value.
			// At this time, the message sent earlier should not be updated directly.
			newIndex := args.PrevLogIndex + len(args.Entries) + 1
			if newIndex > rf.nextIndex[i] {
				rf.nextIndex[i] = newIndex
				rf.matchIndex[i] = rf.nextIndex[i] - 1
			}
		} else {
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.persist()
				rf.state = Follower
				logPrintf("server: %d, receive reply from server: %d, term: %d, reply.term: %d, become follower", rf.me, i, rf.CurrentTerm, reply.Term)
				rf.leaderId = -1
			} else {
				rf.nextIndex[i]--
				if reply.XLen-1 < args.PrevLogIndex {
					rf.nextIndex[i] = reply.XLen
				} else {
					findXTerm := false
					for j := args.PrevLogIndex; j >= 0; j-- {
						if rf.Logs[j].Term == reply.XTerm {
							rf.nextIndex[i] = j
							findXTerm = true
							break
						}
					}
					if !findXTerm {
						rf.nextIndex[i] = reply.XIndex
					}
				}

			}
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	for !rf.killed() && rf.getState() == Leader {
		rf.mu.RLock()
		logPrintf("server: %d, send heartbeat, term: %d, nextIndex: %+v, matchIndex: %+v, logs length: %d, commit index: %d", rf.me, rf.CurrentTerm, rf.nextIndex, rf.matchIndex, len(rf.Logs), rf.commitIndex)
		rf.mu.RUnlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			args := rf.prepareArgs(i, false)
			if args == nil {
				return
			}
			go rf.sendAppendEntriesToOne(i, args)
		}
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func (rf *Raft) updateCommitIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		commitIndex := rf.commitIndex
		for i := len(rf.Logs) - 1; i > rf.commitIndex; i-- {
			if rf.Logs[i].Term != rf.CurrentTerm {
				break
			}
			cnt := 1
			for j := range rf.peers {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= i {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				commitIndex = i
				break
			}
		}
		if commitIndex > rf.commitIndex {
			rf.commitIndex = commitIndex
			rf.applyCond.Broadcast()
		}
		rf.mu.Unlock()
		time.Sleep(SLEEP_INTERVAL)
	}
}

func (rf *Raft) shouldSendEntries(i int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.nextIndex[i] < len(rf.Logs)
}

func (rf *Raft) doFollower() {
	rf.mu.Lock()
	logPrintf("server: %d, do follower, term: %d", rf.me, rf.CurrentTerm)
	rf.candidateTerm = -1
	rf.mu.Unlock()

	rf.updateElectionTimer()
	<-rf.electionTimer.C

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	// check if the election timer is reset during the waiting,
	// otherwise there may be election behavior in the presence of a leader
	if rf.electionTimer.Stop() {
		rf.updateElectionTimer()
		return
	}

	logPrintf("server: %d, election time ring, term: %d", rf.me, rf.CurrentTerm)
	if rf.state == Follower {
		rf.state = Candidate
		rf.CurrentTerm++
		rf.candidateTerm = rf.CurrentTerm
		rf.VotedFor = rf.me
		logPrintf("server: %d, convert to candidate, update term, vote for self, term: %d", rf.me, rf.CurrentTerm)
	}
}

func (rf *Raft) doCandidate() {
	logPrintf("server: %d, do candidate, term: %d", rf.me, rf.getTerm())
	for !rf.killed() && rf.getState() == Candidate {
		rf.election()
	}
}

func (rf *Raft) ticker() {
	go rf.applyEntryProcess()

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

func (rf *Raft) isUpdatedCommitIndex() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.lastApplied < rf.commitIndex
}

func (rf *Raft) applyEntryProcess() {
	for !rf.killed() {
		rf.applyMu.Lock()
		for !rf.isUpdatedCommitIndex() {
			rf.applyCond.Wait()
		}
		rf.applyMu.Unlock()

		rf.mu.Lock()
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		logPrintf("server: %d, apply entry, term: %d, index: %d, cmd: %s", rf.me, rf.CurrentTerm, rf.lastApplied, getCommandValue(msg.Command))
		rf.mu.Unlock()
		rf.applyCh <- msg
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
	initLogSetting()

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.applyMu = sync.Mutex{}
	rf.applyCond = sync.NewCond(&rf.applyMu)
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs = make([]LogEntry, 0)
	rf.Logs = append(rf.Logs, LogEntry{Term: -1})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimer = time.NewTimer(getElectionTimeoutDuration())
	rf.leaderId = -1
	rf.state = Follower
	rf.stopElectCh = nil
	rf.candidateTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
