package vcraft

import (
	"math/rand"
	"time"
	"vcraft/util"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) (err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	util.DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		util.DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] Term[%d] currentTerm[%d] VoteGranted[%v]",
			rf.me, args.CandidateId, args.Term, rf.currentTerm, reply.VoteGranted)
	}()

	// 任期不如我大，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.leaderId = -1
	}

	// 每个任期，只能投票给1人
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// Candidate的日志必须比我的新
		// 1, 最后一条log，任期大的更新
		// 2，任期相同, 更长的log则更新
		lastLogTerm := rf.lastTerm()
		lastLogIndex := rf.lastIndex()
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 为其他人投票，那么重置自己的下次投票时间
		}
	}
	return
}

func (rf *Raft) electionLoop() {
	for rf.isAlive() {
		time.Sleep(10 * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration(200 + rand.Int31n(150)) * time.Millisecond
			deltaTime := now.Sub(rf.lastActiveTime)

			// Follower -> Candidate
			if rf.role == Follower && deltaTime >= timeout {
				rf.role = Candidate
				util.DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
			}

			if rf.role == Candidate && deltaTime >= timeout {
				rf.lastActiveTime = now // 重置下次选举时间
				rf.currentTerm += 1 // 发起新任期
				rf.votedFor = rf.me // 该任期投了自己

				rf.mu.Unlock()

				maxTerm, voteCount := rf.holdVote()

				rf.mu.Lock()

				rf.summaryVote(maxTerm, voteCount)
			}
		}()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) holdVote() (maxTerm int, voteCount int) {
	// 并发RPC请求vote
	type VoteResult struct {
		peerId int
		resp   *RequestVoteReply
	}

	// 请求投票
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}

	voteCount = 1   // 收到投票个数（先给自己投1票）
	finishCount := 1 // 收到应答个数
	voteResultChan := make(chan *VoteResult, len(rf.peers))
	maxTerm = 0
	voteEnd := false

	util.DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]",
		rf.me, args.Term, args.LastLogIndex, args.LastLogTerm)

	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}
		go func(id int) {
			resp := RequestVoteReply{}
			if ok := rf.sendRequestVote(id, &args, &resp); ok {
				voteResultChan <- &VoteResult{peerId: id, resp: &resp}
			} else {
				voteResultChan <- &VoteResult{peerId: id, resp: nil}
			}
		}(peerId)
	}

	for {
		if voteEnd {
			break
		}
		select {
		case voteResult := <-voteResultChan:
			finishCount++
			if voteResult.resp != nil {
				if voteResult.resp.VoteGranted {
					voteCount++
				}
				if voteResult.resp.Term > maxTerm {
					maxTerm = voteResult.resp.Term
				}
			}
			// 得到大多数vote后，立即离开
			if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
				voteEnd = true
			}
		}
	}
	return
}

func (rf *Raft) summaryVote(maxTerm int, voteCount int) {
	defer func() {
		util.DPrintf("RaftNode[%d] RequestVote ends, voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]",
			rf.me, voteCount, RoleText(rf.role), maxTerm, rf.currentTerm)
	}()

	// 如果角色改变了，则忽略本轮投票结果
	if rf.role != Candidate {
		return
	}

	// 发现了更高的任期，切回Follower
	if maxTerm > rf.currentTerm {
		rf.role = Follower
		rf.leaderId = -1
		rf.currentTerm = maxTerm
		rf.votedFor = -1
		return
	}

	// 赢得大多数选票，则成为Leader
	if voteCount > len(rf.peers) / 2 {
		rf.role = Leader
		rf.leaderId = rf.me
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastIndex() + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i] = -1
		}
		rf.lastBroadcastTime = time.Unix(0, 0)
		return
	}
}
