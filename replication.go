package vcraft

import (
	"time"
	"vcraft/util"
)

type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) (err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	util.DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] commitIndex[%d] Logs[%v]",
		rf.me, rf.leaderId, args.Term, rf.currentTerm, RoleText(rf.role), rf.lastIndex(), rf.commitIndex, args.Logs)

	defer func() {
		util.DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] Success[%v] commitIndex[%d] log[%v]",
			rf.me, rf.leaderId, args.Term, rf.currentTerm, RoleText(rf.role), rf.lastIndex(), reply.Success, rf.commitIndex, len(rf.log))
	}()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	if args.PrevLogIndex < rf.lastIndex() {
		reply.ConflictIndex = 0
		return
	}

	// prevLogIndex位置没有日志的case
	if args.PrevLogIndex > rf.lastIndex() {
		reply.ConflictIndex = rf.lastIndex() + 1
		return
	}

	// prevLogIndex位置有日志，那么判断term必须相同，否则false
	if args.PrevLogIndex > -1 {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			for index := 0; index <= args.PrevLogIndex; index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
				if rf.log[index].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
	}

	// 保存日志
	for i, logEntry := range args.Logs {
		index := args.PrevLogIndex + 1 + i
		logPos := index
		if index > rf.lastIndex() { // 超出现有日志长度，继续追加
			rf.log = append(rf.log, logEntry)
		} else { // 重叠部分
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}

	// 更新提交下标
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastIndex()
		}
	}

	reply.Success = true

	return
}

func (rf *Raft) appendEntriesLoop() {
	for rf.isAlive() {
		time.Sleep(10 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有Leader才向外广播心跳
			if rf.role != Leader {
				return
			}

			now := time.Now()
			timeout := 100 * time.Millisecond
			deltaTime := now.Sub(rf.lastBroadcastTime)
			if deltaTime < timeout {
				return
			}
			rf.lastBroadcastTime = time.Now()
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				rf.doAppendEntries(peerId)
			}
		}()

	}
}

func (rf *Raft) doAppendEntries(peerId int) {
	args := AppendEntriesArgs{
		LeaderId: rf.me,
		Term: rf.currentTerm,
		Logs: make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[peerId] - 1,
	}

	if args.PrevLogIndex > -1 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	} else {
		args.PrevLogTerm = 0
	}

	args.Logs = append(args.Logs, rf.log[rf.nextIndex[peerId]:]...)

	go func() {
		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			util.DPrintf("RaftNode[%d] appendEntries starts, currentTerm[%d] peer[%d] logIndex=[%d] args.Logs[%d] commitIndex[%d]",
				rf.me, rf.currentTerm, peerId, rf.lastIndex(), len(args.Logs), rf.commitIndex)
			defer func() {
				util.DPrintf("RaftNode[%d] appendEntries ends, currentTerm[%d] peer[%d] logIndex=[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.commitIndex)
			}()

			// 如果不是rpc前的Leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}

			// 变成Follower
			if reply.Term > rf.currentTerm {
				rf.role = Follower
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				return
			}

			if reply.Success {
				// 同步日志成功
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Logs) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				rf.updateCommitIndex()
			} else {
				if reply.ConflictTerm != -1 {
					// Follower的prevLogIndex位置term冲突了
					// 我们找Leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用Follower的conflictIndex
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index >= 0; index-- {
						if rf.log[index].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					if conflictTermIndex != -1 { // Leader log出现了这个term，那么从这里prevLogIndex之前的最晚出现位置尝试同步
						rf.nextIndex[peerId] = conflictTermIndex
					} else {
						rf.nextIndex[peerId] = reply.ConflictIndex // 用Follower首次出现term的index作为同步开始
					}
				} else {
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
			}
		}
	}()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
