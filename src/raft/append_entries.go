package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

// AppendEntries 前置检查
func (rf *Raft) checkAppendEntriesAfterLock(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if args.LeaderId < 0 || args.LeaderId >= len(rf.peers) {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		logPrintf("server: %d, AppendEntries, invalid leader id: %d", rf.me, args.LeaderId)
		return false
	}
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		logPrintf("server: %d, receive entry or heartbeat from old leader: %d, my term: %d, leader term: %d, my state: %d", rf.me, args.LeaderId, rf.CurrentTerm, args.Term, rf.state)
		return false
	}

	if args.Term > rf.CurrentTerm {
		logPrintf("server: %d, receive entry or heartbeat from new leader: %d, my term: %d, leader term: %d, my state: %d", rf.me, args.LeaderId, rf.CurrentTerm, args.Term, rf.state)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		rf.leaderId = args.LeaderId
		if rf.state == Candidate && rf.stopElectCh != nil {
			rf.stopElectCh <- struct{}{}
		}
		rf.state = Follower
	} else if args.Term == rf.CurrentTerm {
		if rf.state == Candidate && rf.stopElectCh != nil {
			rf.stopElectCh <- struct{}{}
		}
		rf.state = Follower
	}
	// reset the election timer
	rf.updateElectionTimer()
	reply.Term = rf.CurrentTerm
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	if !rf.checkAppendEntriesAfterLock(args, reply) {
		return
	}
	if len(args.Entries) > 0 {
		logPrintf("server: %d, receive entries from leader: %d, term: %d, len(entries): %d, PrevLogIndex: %d, lastIncludedIndex: %d, len(log): %d, leaderCommit: %d", rf.me, args.LeaderId, args.Term, len(args.Entries), args.PrevLogIndex, rf.LastIncludedIndex, len(rf.Logs), args.LeaderCommit)
	} else {
		logPrintf("server: %d, receive heartbeat from leader: %d, term: %d, leaderCommit: %d", rf.me, args.LeaderId, args.Term, args.LeaderCommit)
	}
	// check if the log is consistent
	// if current log doesn't have the prevLogIndex, return false
	if args.PrevLogIndex >= rf.getIndexAfterLock(len(rf.Logs)) {
		reply.Success = false
		reply.XLen = rf.getIndexAfterLock(len(rf.Logs))
		reply.XTerm = -1
		reply.XIndex = -1
		logPrintf("server: %d, prevLogIndex >= my log len, my log fake len: %d, prevLogIndex: %d", rf.me, rf.getIndexAfterLock(len(rf.Logs)), args.PrevLogIndex)
		return
	}
	// if there is a conflict at the prevLogIndex position, return false
	if rf.getTermByIndexAfterLock(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.XLen = rf.getIndexAfterLock(len(rf.Logs))
		reply.XTerm = rf.getTermByIndexAfterLock(args.PrevLogIndex)
		reply.XIndex = 1
		for i := args.PrevLogIndex; i > rf.LastIncludedIndex; i-- {
			if rf.getTermByIndexAfterLock(i) != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
		}

		logPrintf("server: %d, log is inconsistent, my log term at PrevLogIndex: %d, args.prevLogTerm: %d", rf.me, rf.getTermByIndexAfterLock(args.PrevLogIndex), args.PrevLogTerm)
		return
	}

	change := false
	// append the entries to the log
	for i := 0; i < len(args.Entries); i++ {
		// if the log doesn't have current entry, append the rest of the entries
		if args.PrevLogIndex+i+1 == rf.getIndexAfterLock(len(rf.Logs)) {
			rf.Logs = append(rf.Logs, args.Entries[i:]...)
			rf.persist()
			change = true
			break
		} else if args.PrevLogIndex+i+1 > rf.getIndexAfterLock(len(rf.Logs)) {
			panic("log is inconsistent")
		}
		// if there is a conflict, truncate the log and append the rest of the entries
		if rf.getTermByIndexAfterLock(args.PrevLogIndex+i+1) != args.Entries[i].Term {
			rf.Logs = rf.Logs[:rf.getRealIndexAfterLock(args.PrevLogIndex+i+1)]
			rf.Logs = append(rf.Logs, args.Entries[i:]...)
			rf.persist()
			change = true
			break
		}
		// if the log has the current entry, do nothing
	}

	// 当前log可能存在污染，本次只能保证PrevLogIndex+len(entries)之前的log是一致的，之后的其他log可能是不一致的
	oldCommitIndex := rf.commitIndex
	curCheckInedx := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit >= curCheckInedx {
		rf.commitIndex = curCheckInedx
	} else {
		rf.commitIndex = args.LeaderCommit
	}
	if rf.commitIndex > oldCommitIndex {
		rf.applyCond.Broadcast()
	}

	reply.Success = true
	if len(args.Entries) > 0 {
		if change {
			logPrintf("server: %d, append entries from leader: %d, log changed. term: %d, PrevLogIndex: %d, len(entries): %d, lastIncludedIndex: %d, len(log): %d, commitIndex: %d, success", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries), rf.LastIncludedIndex, len(rf.Logs), rf.commitIndex)
		} else {
			logPrintf("server: %d, append entries from leader: %d, term: %d, PrevLogIndex: %d, len(entries): %d, lastIncludedIndex: %d, len(log): %d, commitIndex: %d, success", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries), rf.LastIncludedIndex, len(rf.Logs), rf.commitIndex)
		}
	} else {
		logPrintf("server: %d, finished heartbeat from leader: %d, term: %d, PrevLogIndex: %d, lastIncludedIndex: %d, len(log): %d, commitIndex: %d, success", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, rf.LastIncludedIndex, len(rf.Logs), rf.commitIndex)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
