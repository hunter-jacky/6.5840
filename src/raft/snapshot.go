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

// func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	rf.persist()

// 	if args.Term < rf.CurrentTerm {
// 		reply.Term = rf.CurrentTerm
// 		return
// 	}

// 	if args.Term > rf.CurrentTerm {
// 		rf.CurrentTerm = args.Term
// 		rf.VotedFor = -1
// 		rf.persist()
// 		rf.leaderId = args.LeaderId
// 		if rf.state == Candidate && rf.stopElectCh != nil {
// 			rf.stopElectCh <- struct{}{}
// 		}
// 		rf.state = Follower
// 	}

// 	if args.Term == rf.CurrentTerm {
// 		if rf.state == Candidate && rf.stopElectCh != nil {
// 			rf.stopElectCh <- struct{}{}
// 		}
// 		rf.state = Follower
// 	}

// 	rf.updateElectionTimer()
// 	reply.Term = rf.CurrentTerm

// 	if args.LastIncludedIndex <= rf.snapshot.LastIncludedIndex {
// 		return
// 	}

// 	rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
// 	rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
// 	rf.snapshot.Data = args.Data

// 	rf.log = rf.log[rf.getRelativeIndex(args.LastIncludedIndex):]
// 	rf.persist()
// }
