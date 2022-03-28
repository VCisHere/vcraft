package vcraft

import (
	"sort"
	"time"
	"vcraft/util"
)

func (rf *Raft) updateCommitIndex() {
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.lastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers) / 2]

	if newCommitIndex > rf.commitIndex {
		if rf.log[newCommitIndex].Term == rf.currentTerm {
			rf.commitIndex = newCommitIndex
		}
	}
}

func (rf *Raft) applyLogLoop() {
	noMore := false
	for rf.isAlive() {
		if noMore {
			time.Sleep(10 * time.Millisecond)
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedIndex := rf.lastApplied
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[appliedIndex].Term,
				}
				rf.applyCh <- appliedMsg
				util.DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
				noMore = false
			}
		}()
	}
}