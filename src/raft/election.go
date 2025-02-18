package raft

import (
	"math/rand"
	"time"
)

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
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()

	logPrintf("server: %d, receive vote request from server: %d, args.term: %d, args.LastLogIndex: %d, args.LastLogTerm: %d", rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	if args.CandidateId < 0 || args.CandidateId >= len(rf.peers) {
		return
	}

	// leader 也可能会受到投票请求，例如，
	// 1. T1: 当server1作为候选者开始进行选举，server2向server1投赞成票，
	// 2. T2：server1未收集到决定性的票数，仍然在等待收集其他票数。同时，server2因投票超时成为候选者，
	// （此时，server1集票阶段的超时时间 > server2选举超时时间）
	// 3. T3：server1收到了足够票数，成为了leader，同时server1收到server2的投票请求。
	// 如果server1和server2的log数据一样，由于server2的term在给server1投票时已经更新到与server1一样，
	// 在server2成为候选者后，term已经比server1大。此时如果按照term和log去判断，会使得server1给server2投票.
	// 4. T4：此时距离T3很近，server2未听到server1的heartbeat。并且server2收集到足够票数，成为leader，此时会导致两个leader的产生。

	// 避免上述情况的方法：
	// 1. leader丢弃收到的投票请求
	// 2. 成为leader后第一时间发送心跳。但是在候选者身份下选举成为leader改变状态后，
	// 到以leader身份进行工作时，仍然会有一段时间的延迟。此期间可能会被其他的投票请求抢到锁。
	// 或者是先收到的投票请求，被抢了锁。然后收到决定性投票，但是在改变身份成为leader前，需要等待锁释放。
	// 此时需要在拿到锁之后判断自己的term是否被改变，即是否给更高term的候选者投了票。如果被改变则回归到follower状态。
	// 3. 尽可能保证大部分情况下，集票阶段的超时时间 < 选举超时时间

	if rf.state == Leader {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		logPrintf("server: %d, reject vote for server: %d, I'm leader", rf.me, args.CandidateId)
		return
	}

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		logPrintf("server: %d, reject vote for server: %d for have smaller term, his term: %d, our term: %d", rf.me, args.CandidateId, args.Term, rf.CurrentTerm)
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		if rf.state == Candidate && rf.stopElectCh != nil {
			rf.stopElectCh <- struct{}{}
		}
		rf.state = Follower
	}

	reply.Term = rf.CurrentTerm
	ok, reason := rf.shouldVote(args)
	if ok {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.persist()
		rf.updateElectionTimer()
		logPrintf("server: %d, vote for server: %d, args.term: %d, args.LastLogIndex: %d, args.LastLogTerm: %d", rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	} else {
		reply.VoteGranted = false
		logPrintf("server: %d, reject vote for server: %d, %s , args.term: %d, args.LastLogIndex: %d, args.LastLogTerm: %d", rf.me, args.CandidateId, reason, args.Term, args.LastLogIndex, args.LastLogTerm)
	}
}

func (rf *Raft) shouldVote(args *RequestVoteArgs) (bool, string) {
	if args.Term < rf.CurrentTerm {
		return false, "candidate's term is smaller than receiver's term"
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		// the candidate's log is at least as up-to-date as receiver's log
		if rf.Logs[len(rf.Logs)-1].Term < args.LastLogTerm || (rf.Logs[len(rf.Logs)-1].Term == args.LastLogTerm && len(rf.Logs)-1 <= args.LastLogIndex) {
			return true, ""
		}
		return false, "candidate's log is not up-to-date"
	}
	return false, "already voted"
}

// the heartbeat interval, follower should begin an election
// if it doesn't receive any message from the leader within this time.
func getElectionTimeoutDuration() time.Duration {
	return ELECTION_TIMEOUT + time.Duration(rand.Int63()%int64(ELECTION_RAND_SCOPE))
}

// the candidate should begin next election
// if it doesn't win the election within this time, and not receive any message from the leader.
func getVoteGrantTimeoutDuration() time.Duration {
	return VOTE_GRANT_TIMEOUT + time.Duration(rand.Int63()%int64(VOTE_RAND_SCOPE))
}

func (rf *Raft) updateElectionTimer() {
	rf.electionTimer.Reset(getElectionTimeoutDuration())
}

// check if the server is in the candidate state and has correct term
func (rf *Raft) checkCandidateStateAfterLock() bool {
	return rf.state == Candidate && rf.CurrentTerm == rf.candidateTerm
}

// the election process, determine the final state of the server, if state is
// - Leader, the server has won the election
// - Follower, the server has lost the election
// - Candidate, the server has not received enough votes, timeout
// election may be stopped when meet a new leader or a bigger term
func (rf *Raft) election() {
	rf.mu.Lock()
	if !rf.checkCandidateStateAfterLock() {
		rf.mu.Unlock()
		return
	}

	rf.persist()

	// create the channel(stopElectCh) only at the beginning of the election to avoid a situation
	// where the channel repeatedly sends signals for multiple reasons before receiving the signal, causing it to block
	// multiple reasons: receive the vote with a bigger term, receive the heartbeat from new leader
	rf.stopElectCh = make(chan struct{}, 1)
	rf.mu.Unlock()

	defer func() {
		rf.mu.Lock()
		rf.stopElectCh = nil
		rf.mu.Unlock()
	}()

	electResCh := make(chan ElectionResult, 1) // to receive the result of the election
	cancelCh := make(chan struct{}, 1)         // to cancel the election
	go rf.electionOnce(electResCh, cancelCh)

	select {
	case result := <-electResCh:
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !rf.checkCandidateStateAfterLock() {
			// state可能会改变，变成leader是错误的，不应该。
			// 变成follower可能是其他candidate二次选举，把更大的term同步给当前server的时候导致状态回退成follower
			// 这种情况下，更可能是通过stopElectCh来停止选举的，来到这个分支的原因是已经选举出结果，但是被卡在了上面的锁上
			switch rf.state {
			case Candidate:
				// 此时term被更新，是因为已经给其他term更高的server投了票，因此需要回退到follower状态
				logPrintf("server: %d, term has been updated, back to follower", rf.me)
			case Follower:
				logPrintf("server: %d, become follower not by stopElectCh", rf.me)
			case Leader:
				panic("server: %d, become leader before the election end")
			}
			rf.state = Follower
			return
		}

		switch result {
		case Win:
			logPrintf("server: %d, Election won", rf.me)
			rf.state = Leader
			rf.leaderId = rf.me
		case Lose:
			logPrintf("server: %d, Election lost", rf.me)
			rf.state = Follower
		case TimeOut:
			// stay in the candidate state, restart the election
			logPrintf("server: %d, vote timeout, restart the election", rf.me)
			rf.state = Candidate
			rf.CurrentTerm++
			rf.candidateTerm = rf.CurrentTerm
			rf.VotedFor = rf.me
		}
	case <-rf.stopElectCh:
		// another server has won the election, state has been changed
		cancelCh <- struct{}{}
		logPrintf("server: %d, stop election", rf.me)

	}
}

// wait for the votes
func (rf *Raft) electionOnce(resCh chan<- ElectionResult, cancelCh <-chan struct{}) {
	rf.mu.RLock()
	if !rf.checkCandidateStateAfterLock() {
		rf.mu.RUnlock()
		resCh <- Lose
		return
	}

	logPrintf("server: %d, start the election, new Term: %d", rf.me, rf.CurrentTerm)

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Logs) - 1,
		LastLogTerm:  rf.Logs[len(rf.Logs)-1].Term,
	}
	rf.mu.RUnlock()

	voteCh := make(chan bool, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			voteCh <- true
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			logPrintf("server: %d, send vote request to server: %d, term: %d", rf.me, i, args.Term)
			if rf.sendRequestVote(i, args, reply) {
				voteCh <- reply.VoteGranted
				logPrintf("server: %d, receive vote from server: %d, reply: %+v", rf.me, i, reply)
			}
		}(i)
	}

	// check if the candidate has won the election
	logPrintf("server: %d, Wait for the votes, term: %d", rf.me, rf.getTerm())
	votes := 0
	count := 0
	for !rf.killed() {
		select {
		case <-time.After(getVoteGrantTimeoutDuration()):
			resCh <- TimeOut
			return
		case vote := <-voteCh:
			count++
			if vote {
				votes++
			}
			logPrintf("server: %d, Vote received, votes: %d, count: %d", rf.me, votes, count)
			if votes > len(rf.peers)/2 {
				resCh <- Win
				return
			}
			if count-votes > len(rf.peers)/2 {
				resCh <- Lose
				return
			}
		case <-cancelCh:
			resCh <- Lose
			return
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
