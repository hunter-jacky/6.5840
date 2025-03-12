package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()

	if args.LeaderId < 0 || args.LeaderId >= len(rf.peers) {
		reply.Term = rf.CurrentTerm
		logPrintf("server: %d, InstallSnapshot, invalid leader id: %d", rf.me, args.LeaderId)
		return
	}

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		logPrintf("server: %d, receive snapshot from old leader: %d, my term: %d, leader term: %d, my state: %d", rf.me, args.LeaderId, rf.CurrentTerm, args.Term, rf.state)
		return
	}

	if args.Term > rf.CurrentTerm {
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

	rf.updateElectionTimer()
	reply.Term = rf.CurrentTerm

	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		logPrintf("server: %d, receive snapshot from leader: %d, but my snapshot is newer, my LastIncludedIndex: %d, leader LastIncludedIndex: %d", rf.me, args.LeaderId, rf.LastIncludedIndex, args.LastIncludedIndex)
		return
	}

	if args.LastIncludedIndex <= rf.getIndexAfterLock(len(rf.Logs)-1) {
		logs := make([]LogEntry, len(rf.Logs)-rf.getRealIndexAfterLock(args.LastIncludedIndex))
		copy(logs, rf.Logs[rf.getRealIndexAfterLock(rf.LastIncludedIndex):])
		rf.Logs = logs
	} else {
		rf.Logs = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}
	rf.snapshot = args.Data
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	if rf.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.LastIncludedIndex
	}

	rf.persist()
	logPrintf("server: %d, receive snapshot from leader: %d, my LastIncludedIndex: %d, leader LastIncludedIndex: %d, len(log): %d", rf.me, args.LeaderId, rf.LastIncludedIndex, args.LastIncludedIndex, len(rf.Logs))
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
