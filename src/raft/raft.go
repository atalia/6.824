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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	term    int
	index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// service
	applyCh chan ApplyMsg

	// persistent
	currentTerm int
	votedFor    int // -1 for nil
	logs        []LogEntry

	// volatile for all
	commitIndex   int //start at -1
	lastApplied   int
	leader        int
	lastHeartbeat time.Time

	// volatile for leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.leader == rf.me

	return term, isleader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(logs) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A, 2B).
	if args.term < rf.currentTerm || (args.term == rf.currentTerm && rf.votedFor == -1) {
		reply.term = rf.currentTerm
		reply.voteGranted = false
		return
	}

	// 日志比较
	lastLogTerm := -1
	lastLogIndex := -1

	if len(rf.logs) > 0 {
		logEntry := rf.logs[len(rf.logs)-1]
		lastLogTerm = logEntry.term
		lastLogIndex = logEntry.index
	}

	if args.lastLogTerm < lastLogTerm || args.lastLogTerm == args.lastLogTerm && args.lastLogIndex < lastLogIndex {
		reply.term = rf.currentTerm
		reply.voteGranted = false
		return
	}

	// 接受
	rf.votedFor = args.candidateId
	rf.currentTerm = args.term

	reply.voteGranted = true
	reply.term = rf.currentTerm

	if !rf.killed() {
		rf.lastHeartbeat = time.Now()
	}
	rf.persist()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesRequest struct {
	term         int
	leaderId     int
	prevLogIndex int
	preLogTerm   int
	entries      []LogEntry

	leaderCommit int
}

type AppendEntriesReply struct {
	term    int
	success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.term < rf.currentTerm {
		reply.success = false
		reply.term = rf.currentTerm
		return
	}
	// 此时变follower
	rf.leader = args.leaderId
	rf.votedFor = -1

	if !rf.killed() {
		rf.lastHeartbeat = time.Now()
	}

	offset := -1

	if len(rf.logs) > 0 {
		// 防止snapshot发生过截断
		firstLogEntry := rf.logs[0]
		offset = args.prevLogIndex - firstLogEntry.index
		// 日志不匹配
		if rf.logs[offset].term != args.preLogTerm {
			reply.success = false
			reply.term = rf.currentTerm
			return
		}
	}

	// 匹配后开始拼接新的日志
	if args.entries != nil && len(args.entries) > 0 {
		rf.logs = append(rf.logs[offset+1:], args.entries...)
		rf.persist()
	}

	// apply service
	if rf.commitIndex < args.leaderCommit {

		lastLogEntryIndex := len(rf.logs) - 1

		commitIndex := args.leaderCommit
		if commitIndex < lastLogEntryIndex {
			commitIndex = lastLogEntryIndex
		}

		startCommitIndex := 0
		if rf.commitIndex > startCommitIndex {
			startCommitIndex = rf.commitIndex
		}

		for _, logEntry := range rf.logs[startCommitIndex:rf.commitIndex] {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.index,
			}
		}

		rf.commitIndex = commitIndex
	}

	reply.success = true
	reply.term = rf.currentTerm

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leader = -1
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.lastHeartbeat = time.Now()
	
	majority := int(len(peers) / 2) 

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// timeout
	electionTimeout := time.Duration(rand.Int31n(40)+60) * 10 * time.Millisecond
	boardcastTimeout := 100 * time.Millisecond
	
	appendEntries := func (){
		for{
			time.Sleep(boardcastTimeout)

			if !rf.killed() {
				// LEADER
				if term, isleader := rf.GetState(); isleader {
					wg := sync.WaitGroup{}
					for idx, _ := range rf.peers {
						wg.Add(1)
						// 客户端并发发送
						go func(idx int) {
							if idx != rf.me {
								req := &AppendEntriesRequest{
									term:         term,
									leaderId:     me,
									prevLogIndex: -1,
									preLogTerm:   -1,
									entries:      nil,
									leaderCommit: -1,
								}
								reply := &AppendEntriesReply{}
								if ok := rf.sendAppendEntries(idx, req, reply); ok {
									//TODO DO WITH REPLAY
								}
							}
							wg.Done()
						}(idx)
					}
					wg.Wait()
				}else{
					return
				}
			} else{
				return
			}
		}
		
	}

	// heartbeat
	go func() {
		for {
			time.Sleep(10 * time.Millisecond)

			// CANDIDATE
			if time.Now().Sub(rf.lastHeartbeat) > electionTimeout {
				vote := 1
				rf.currentTerm += 1
				wg := sync.WaitGroup{}

				for idx, _ := range rf.peers {
					wg.Add(1)
					go func(idx int) {
						if idx != rf.me {
							req := &RequestVoteArgs{
								term:         rf.currentTerm,
								candidateId:  rf.me,
								lastLogIndex: -1,
								lastLogTerm:  -1,
							}
							reply := &RequestVoteReply{}
							if ok := rf.sendRequestVote(idx, req, reply); ok {
								if reply.voteGranted {
									vote += 1
								} else {
									if reply.term > rf.currentTerm {
										rf.currentTerm = reply.term
									}
								}
							}
						}
						wg.Done()
					}(idx)
				}
				wg.Wait()

				if vote > majority{
					rf.leader = me
					appendEntries()
				}
			}
		}
	}()

	return rf
}
