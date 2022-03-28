package vcraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"vcraft/util"
)

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

type ApplyMsg struct {
	CommandValid      bool

	// 向application层提交日志
	Command           interface{}
	CommandIndex      int
	CommandTerm       int
}

type Raft struct {
	mu sync.           Mutex
	alive              int32
	me                 int
	peers              []*ClientEnd

	// 持久化状态
	currentTerm       int        // 见过的最大任期
	votedFor          int        // 记录在currentTerm任期投票给谁了
	log               []LogEntry // 操作日志

	// 易失状态
	commitIndex       int        // 已知的最大已提交索引
	lastApplied       int        // 当前应用到状态机的索引

	// 易失状态
	nextIndex         []int      //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex        []int      // 每个follower的log同步进度（初始为0），和nextIndex强关联

	role						 // 身份
	leaderId          int        // leader的id
	lastActiveTime    time.Time  // 上次活跃时间（刷新时机：收到Leader心跳、给其他Candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time  // 作为Leader，上次的广播时间

	applyCh chan ApplyMsg        // 应用层的提交队列
}

func NewRaft(addrs []string, me int) (rf *Raft, applyChan chan ApplyMsg, err error) {
	rf = &Raft{}

	rf.me = me
	rf.role = Follower
	rf.alive = 1
	rf.leaderId = -1
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.lastActiveTime = time.Now()
	rf.applyCh = make(chan ApplyMsg, 1)
	applyChan = rf.applyCh

	// peers
	rf.initRpcPeers(addrs)
	// me
	go rf.initRpcServer()
	// election逻辑
	go rf.electionLoop()
	// leader逻辑
	go rf.appendEntriesLoop()
	// apply逻辑
	go rf.applyLogLoop()

	util.DPrintf("RaftNode[%d] 启动", me)
	return
}

func (rf *Raft) isAlive() bool {
	z := atomic.LoadInt32(&rf.alive)
	return z == 1
}

func (rf *Raft) lastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastTerm() (lastLogTerm int) {
	if len(rf.log) != 0 {
		return rf.log[len(rf.log) - 1].Term
	} else {
		return 0
	}
}

func SleepSeconds(rf *Raft) {
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.alive = 0
		log.Printf("RaftNode[%d] Sleep", rf.me)
		time.Sleep(13 * time.Second)
		log.Printf("RaftNode[%d] Wakeup", rf.me)
		rf.alive = 1
	}()
}

func PrintState(rf *Raft) {
	if rf != nil {
		log.Printf("[%d] %s", rf.me, RoleText(rf.role))
	}
}

func PrintLogs(rf *Raft) {
	if rf != nil {
		log.Printf("[%d] %v", rf.me, rf.log)
	}
}

func (rf *Raft) Push(command interface{}) (int, int) {
	index := -1
	term := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 只有Leader才能写入
	if rf.role != Leader {
		return -1, -1
	}

	log.Printf("[%d] %s", rf.me, command.(string))

	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = rf.lastIndex()
	term = rf.currentTerm

	util.DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)

	return index, term
}