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
	"fmt"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	MAXSERVERCOUNT = 5
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term (or null if none)
	votedFor int
	// state a Raft server must maintain.
	// currentState
	state int
	// voteCount
	voteCount     int
	heartbeatChan chan bool
	leaderChan    chan bool
	commitCh      chan bool

	// index of highest log entry known to be committed
	commitIndex int
	// index of highest log entry applied to state machine
	lastApplied int

	log []LogEntry

	// leader properties , next index will send to follower server
	nextIndex []int
	// for each server , index of highest log entry known to be replicated on server
	matchIndex []int
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// ----------------- heartbeat req start--------------------
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	NextIndex int
	Success   bool
}

func (rf *Raft) commitLog() {
	commit := false
	for n := rf.commitIndex + 1; n <= rf.getLastIndex(); n++ {
		count := 1
		for i := range rf.peers {
			if rf.me != i && rf.matchIndex[i] >= n && rf.log[n].Term == rf.currentTerm {
				count += 1
			}
		}
		if count >= len(rf.peers)>>1 {
			rf.commitIndex = n
			commit = true
		}
	}
	if commit {
		rf.commitCh <- true
	}
}

func (rf *Raft) appendEntries2Follower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.commitLog()

	var reply AppendEntriesReply
	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			var args AppendEntriesArgs
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[args.PrevLogIndex+1:]
			go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
				rf.sendAppendEntries(server, args, &reply)
			}(i, args, reply)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == LEADER {
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				rf.becameFollower(reply.Term)
			} else {
				rf.nextIndex[server] = reply.NextIndex
			}
		} else {
			// 如果发送了日志,并且成功更新下nextIndex 和 matchIndex
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		}
	}
	return ok
}
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeatChan <- true

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	if args.Term > rf.currentTerm {
		rf.becameFollower(args.Term)
	}

	// 日志条目小于leader的nextIndex
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	// 当前server的index 小于等于prevLogIndex
	if args.PrevLogIndex >= 0 {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.NextIndex = rf.decreaseNextIndex(args.PrevLogIndex)
			return
		}
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
	} else {
		reply.Success = false
	}
	reply.NextIndex = rf.getLastIndex() + 1
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.getLastIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLastIndex()
		}
		rf.commitCh <- true
	}
}

func (rf *Raft) decreaseNextIndex(prevLogIndex int) int {
	nextIndex := 0
	// 找到前一个与prevLogTerm不相同的term的最后一个位置（每次朝前一个term）
	// eg leader 1 1 1 4 4 5 5 6 6 6
	// follower  1 1 1 2 2 2 3 3 3 3 3 3
	// 第一次会到2这个term
	for i := prevLogIndex - 1; i >= 0; i-- {
		if rf.log[i].Term != rf.log[prevLogIndex].Term {
			nextIndex = i + 1
			break
		}
	}
	return nextIndex
}

//---------------------heartbeat req end---------------------------

func (rf *Raft) becameFollower(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
}

// Start add command
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.state == LEADER

	if isLeader {
		var logEntry LogEntry
		logEntry.Term = rf.currentTerm
		logEntry.Command = command
		logEntry.Index = rf.getLastIndex() + 1
		rf.log = append(rf.log, logEntry)
		index = logEntry.Index
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case FOLLOWER:
			// 1. get leader heartbeat 2.timeout start election
			select {
			case <-rf.heartbeatChan:
				//log.Println(fmt.Sprintf("%v,get heartbeat", rf.me))
			case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
			}
		case CANDIDATE:
			rf.startElection()
			select {
			case <-rf.heartbeatChan:
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			case isLeader := <-rf.leaderChan:
				// init nextIndex

				if isLeader {
					//log.Println(fmt.Sprintf("server : %v,start to heartbeat",rf.me))
					rf.initNextIndex()
					rf.appendEntries2Follower()
				}
			case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
				//log.Println(fmt.Sprintf("candidate: %v time-out ",rf.me))
			}
		case LEADER:
			//log.Println(fmt.Sprintf("current leader:%v", rf.me))
			time.Sleep(time.Millisecond * 50)
			rf.appendEntries2Follower()
		}

	}
}

func (rf *Raft) initNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := range rf.nextIndex {
		rf.nextIndex[server] = rf.getLastIndex() + 1
	}
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.commitCh:
			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				var msg ApplyMsg
				msg.CommandIndex = i
				msg.Command = rf.log[i].Command
				applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}
}

//-------- candidate start
type RequestVoteArgs struct {
	// candidate's  term
	Term int
	// candidate requesting vote
	CandidateId int

	LastLogIndex  int
	LastTermIndex int
}

type RequestVoteReply struct {
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.voteCount = 1
	rf.mu.Unlock()
	rf.sendElection2Other()
}

func (rf *Raft) sendElection2Other() {
	args := RequestVoteArgs{
		CandidateId:   rf.me,
		Term:          rf.currentTerm,
		LastTermIndex: rf.getLastTerm(),
		LastLogIndex:  rf.getLastIndex(),
	}
	var reply RequestVoteReply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == CANDIDATE {
			go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
				//log.Println(fmt.Sprintf("server start election: %v,send to server:%v", rf.me, server))
				rf.sendRequestVote(server, args, &reply)
			}(i, args, reply)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.VoteGranted && rf.state == CANDIDATE {
			//log.Println(fmt.Sprintf("candidate:%v,server repy true:%v", rf.me, server))
			rf.voteCount++
			if rf.voteCount > len(rf.peers)>>1 {
				log.Println(fmt.Sprintf("leader has been elected : [%v]", rf.me))
				rf.state = LEADER
				rf.leaderChan <- true
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.becameFollower(reply.Term)
			}
		}
	}
	return ok
}

func (rf *Raft) checkLog(candidateLastLogIndex int, candidateTermIndex int) bool {
	// 任期大
	if candidateTermIndex > rf.getLastTerm() {
		return true
	}
	if candidateTermIndex == rf.getLastTerm() && candidateLastLogIndex >= rf.getLastIndex() {
		return true
	}
	return false
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// rejoin
	if args.Term > rf.currentTerm {
		rf.becameFollower(args.Term)
	}
	checkLog := rf.checkLog(args.LastLogIndex, args.LastTermIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && checkLog {
		//log.Println(fmt.Sprintf("server:%v set votedFor:%v", rf.me, args.CandidateId))
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
	} else {
		//log.Println(fmt.Sprintf("server:%v return false :%v", rf.me, args.CandidateId))
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}

}

// --------- candidate end
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.heartbeatChan = make(chan bool, MAXSERVERCOUNT)
	rf.leaderChan = make(chan bool, MAXSERVERCOUNT)
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.log = append(rf.log, LogEntry{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply(applyCh)
	return rf
}
