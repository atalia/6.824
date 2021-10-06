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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"log"
	// "io/ioutil"
	"../labgob"
	"../labrpc"
)

var (
	randG *rand.Rand
	electionTimeoutSet map[time.Duration]struct{}
)

func init(){
	seed := time.Now().UnixNano()
	source := rand.NewSource(seed)
	randG = rand.New(source)
	electionTimeoutSet = make(map[time.Duration]struct{})
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	// log.SetOutput(ioutil.Discard)
}

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
	Term    int
	Index   int
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

	electionTimeout  time.Duration
	boardcastTimeout time.Duration

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
	// log.Printf("%v persist currentTerm=%v votedFor=%v logs=%v\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.electionTimeout)
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
	var electionTimeout time.Duration
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&electionTimeout) != nil{
		log.Printf("%v readPersist error\n", rf.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("%v readPersist currentTerm=%v votedFor=%v logs=%v\n", rf.me, currentTerm, votedFor, logs)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		if logs == nil {
			rf.logs = make([]LogEntry, 0)
		} else {
			rf.logs = logs
		}
		log.Printf("%v electionTimeout %v", rf.me, electionTimeout)
		rf.electionTimeout = electionTimeout
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	// log.Printf("%v receive RequestVote %+v \n", rf.me, args)
	

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	// term 比自身小或者 一样的term时，已经投票了
	// bugfix: args.Term == rf.currentTerm 时，有可能rf.votedFor = -1 ,因为之前已经投过票了。投票只会投给term高于自己的term的，等于也不行
	// 等于的时候，如果判断votedfor = -1 有一种错误在于，选举成功后，votedfor有可能在appendentries term升上去时，置为-1，这时候在发生term投票，就会脑裂
	if args.Term <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf("%v reject vote! votedFor=%v currentTerm=%v, requests=%+v\n", rf.me, rf.votedFor, rf.currentTerm, args)
		return
	}

	// term 比自身大时
	rf.currentTerm = args.Term
	rf.leader = -1

	rf.persist()

	// 日志比较
	lastLogTerm := -1
	lastLogIndex := -1

	if len(rf.logs) > 0 {
		logEntry := rf.logs[len(rf.logs)-1]
		lastLogTerm = logEntry.Term
		lastLogIndex = logEntry.Index
	}

	// term 高，但是日志短
	if args.LastLogTerm < lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf("%v reject vote request! lastLogTerm=%v , lastLogIndex=%v, request=%+v\n", rf.me,lastLogTerm, lastLogIndex, args)
		return
	}
	// 优化：接受时，才更新心跳时间
	rf.lastHeartbeat = time.Now()
	// 接受
	rf.votedFor = args.CandidateId
	//rf.currentTerm = args.Term

	rf.persist()

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	log.Printf("%v vote for %v at Term %v", rf.me, args.CandidateId, reply.Term)
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	// 更新最后一次心跳时间
	if rf.killed() {
		return
	}

	log.Printf("follower %v receive AppendEntries %+v\n", rf.me, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Println("server ", rf.me, "logs ", rf.logs)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		log.Printf("%v current term %v receive outdate AppendEntries Term %v\n", rf.me, rf.currentTerm, args.Term)
		return
	}

	rf.lastHeartbeat = time.Now()

	// 此时变follower
	rf.leader = args.LeaderId
	rf.votedFor = -1
	// bugfix 如果自身term 小，那么需要更新term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	rf.persist()
	// log.Println("follower ", rf.me, "logs ", rf.logs)

	offset := -1

	// bug: 如果log为空，而args.PrevLogIndex > -1？
	if args.PrevLogIndex > -1 && len(rf.logs) == 0{
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	
	if args.PrevLogIndex > -1 && len(rf.logs) > 0 {
		// 防止snapshot发生过截断
		firstLogEntry := rf.logs[0]

		// 相对位置
		offset = args.PrevLogIndex - firstLogEntry.Index

		// 日志不匹配
		// log.Printf("follower %v receive PrevLogIndex = %v PrevLogTerm = %v, offset = %v, rf.logs %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, offset, rf.logs)
		if offset < -1 || len(rf.logs) - 1 < offset || offset > 0 && rf.logs[offset].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			// log.Printf("follower %v log not match, PrevLogIndex %v PrevLogTerm %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			// log.Printf("follower %v log %v\n", rf.me, rf.logs)
			return
		}

	}
	
	// 匹配后开始拼接新的日志
	if args.Entries != nil && len(args.Entries) > 0 {
		// log.Printf("follower %v logs %v PrevLogIndex = %v PrevLogTerm = %v offset= %v \n", rf.me, rf.logs, args.PrevLogIndex, args.PrevLogTerm, offset)

		if offset + 1 > cap(rf.logs) {
			// log.Printf("follower %v offset greater than cap", rf.me)
			rf.logs = append(rf.logs[:], args.Entries...)
		} else {
			rf.logs = append(rf.logs[:offset+1], args.Entries...)
		}
		// log.Printf("follower %v logs %v\n", rf.me, rf.logs)
		rf.persist()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	// log.Println("follower ", rf.me, " AppendEntries finish...")
	rf.commitIndex = args.LeaderCommit
	log.Printf("follower %v logs %v\n", rf.me, rf.logs)
	rf.CommitLog()
}

func (rf *Raft) CommitLog() {
	// log.Println("server ", rf.me, " commit logs ")
	if len(rf.logs) == 0 || rf.commitIndex == rf.lastApplied { // no update
		return
	} else { // update service

		lastLogEntry := rf.logs[len(rf.logs)-1]

		// find last log entry can commit
		commitIndexEnd := lastLogEntry.Index

		if commitIndexEnd > rf.commitIndex {
			commitIndexEnd = rf.commitIndex
		}

		for pos := rf.findLogEntryPositionWithIndex(rf.lastApplied + 1); pos >= 0 && rf.lastApplied < commitIndexEnd; pos++ {

			logEntry := rf.logs[pos]

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index + 1,
			}
			rf.lastApplied = rf.lastApplied + 1
			// log.Printf("server %v commit log %v ok\n", rf.me, logEntry)
		}
		log.Printf("server %v commit logs.id %v ok\n", rf.me, rf.lastApplied)
	}
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
	if rf.killed() {
		return index, term, false
	}

	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.logs) > 0 {
		lastLogEntry := rf.logs[len(rf.logs)-1]

		// index = rf.matchIndex[rf.me]
		index = lastLogEntry.Index + 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	} else {
		index = 0
	}
	rf.logs = append(rf.logs[:len(rf.logs)], LogEntry{
		Term:    rf.currentTerm,
		Index:   index,
		Command: command,
	})
	rf.persist()
	log.Printf("leader %v logs %v\n", rf.me, rf.logs)

	return index + 1, term, isLeader
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

func (rf *Raft) appendEntries() {
	for {
		time.Sleep(rf.boardcastTimeout)
		go func() {
			if !rf.killed() {
				// LEADER
				if term, isleader := rf.GetState(); isleader {
					rf.lastHeartbeat = time.Now()
					wg := sync.WaitGroup{}
					logs := rf.logs
					currentTerm := rf.currentTerm
					for peer, _ := range rf.peers {
						wg.Add(1)
						// Follower并发发送
						go func(peer int) {
							if peer != rf.me {
								nextIndex := rf.nextIndex[peer]
								prevLogIndex := -1
								prevLogTerm := -1
								if nextIndex > 0 {
									if nextIndex - 1 >= len(logs) {
										return
									}
									prevLogEntry := logs[nextIndex-1]
									prevLogIndex = prevLogEntry.Index
									prevLogTerm = prevLogEntry.Term
								}

								entries := make([]LogEntry, 0)
								if nextIndex >= 0 {
									entries = append(entries, logs[nextIndex:]...)
								}

								req := &AppendEntriesRequest{
									Term:         term,
									LeaderId:     rf.me,
									PrevLogIndex: prevLogIndex,
									PrevLogTerm:  prevLogTerm,
									Entries:      entries,
									LeaderCommit: rf.commitIndex,
								}
								// log.Printf("leader %v send to %v %v\n", me, peer, req)
								reply := &AppendEntriesReply{}
								if ok := rf.sendAppendEntries(peer, req, reply); ok {
									if reply.Success {
										// 心跳
										if req.Entries == nil || len(req.Entries) == 0 {
											// log.Printf("%v receive heartbeat from %v\n", peer, rf.me)
										} else {
											// 常规append
											lastLogEntry := req.Entries[len(req.Entries)-1]
											// 相对位置
											if index := findLogEntryPositionWithIndex(logs, lastLogEntry.Index); index > rf.matchIndex[peer] {
												rf.matchIndex[peer] = index
												rf.nextIndex[peer] = rf.matchIndex[peer] + 1
												// log.Printf("leader %v matchIndex %v nextIndex %v\n", rf.me, rf.matchIndex, rf.nextIndex)
												// 更新commitID
												// Figure 8 注意只能提交当前term的log
												tmp := append(make([]int, 0, len(rf.matchIndex)), rf.matchIndex...)
												sort.Ints(tmp)
												commitIndex := tmp[len(tmp)/2]
												rf.mu.Lock()
												defer rf.mu.Unlock()
												commitTerm := -1
												// matchIndex 为ref 时，如果响应超时了，matchIdx可能比上下文的log长度大
												if commitIdx := findLogEntryPositionWithIndex(logs, commitIndex) ; commitIdx >= 0 && commitIdx < len(logs) {
													commitLogEntry := logs[commitIdx]
													commitTerm = commitLogEntry.Term
												}
												currentTerm := rf.currentTerm
												if commitIndex > rf.commitIndex && commitTerm == currentTerm{
													rf.commitIndex = commitIndex
													log.Printf("leader %v update commitIndex %v\n", rf.me, rf.commitIndex)
													rf.CommitLog()
												}
											}
										}

									} else {
										// term 非最大即非leader
										if reply.Term > currentTerm {
											log.Printf("outdate: leader %v term %v is outdate(peer %v reply.Term %v)", rf.me, currentTerm, peer, reply.Term)
											if reply.Term > rf.currentTerm{
												rf.currentTerm = reply.Term
												rf.leader = -1
												rf.persist()
											}
											return
										}
										if len(req.Entries) > 0 {
											// log 问题
											// bug 记录，如果发现不匹配，说明第一个log就不匹配，而不是最后一个log不匹配。
											//lastLogEntry := req.Entries[len(req.Entries) - 1]
											firstLogEntry := req.Entries[0]
											// 相对位置
											// log.Printf("leader %v log %v\n", rf.me, rf.logs)
											// log.Printf("leader %v peer %v nextIndex %v Error\n", rf.me, peer, rf.nextIndex[peer])
											if firstLogEntry.Index < len(logs) {
												rf.nextIndex[peer] = findPriorTermFirstLogEntry(logs, firstLogEntry.Index)
											}

											// log.Printf("leader %v peer %v nextIndex change to %v\n", rf.me, peer, rf.nextIndex[peer])
										}

									}
								}
							}
							wg.Done()
						}(peer)
					}
					wg.Wait()
				} else { // 不是leader 不需要发送
					return
				}
			} else { // server killed 不需要发送
				return
			}
		}()
	}
}

func (rf *Raft) requestVote() {
	if !rf.killed() && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
		// log.Println(rf.me, " since lastHeartbeat ", time.Since(rf.lastHeartbeat))
		rf.lastHeartbeat = time.Now()

		majority := int(len(rf.peers) / 2)

		rf.mu.Lock()
		rf.votedFor = rf.me
		vote := 1
		rf.currentTerm += 1
		currentTerm := rf.currentTerm
		rf.persist()

		rf.mu.Unlock()

		lastLogIndex := -1
		lastLogTerm := -1
		if rf.logs != nil && len(rf.logs) > 0 {
			lastLogEntry := rf.logs[len(rf.logs)-1]
			lastLogIndex = lastLogEntry.Index
			lastLogTerm = lastLogEntry.Term
		}

		req := &RequestVoteArgs{
			Term:         currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		// waitGroup 如果出现RPC超时会造成无限选举. 所以为了提高效率，这里需要在确定结果的时候及时反馈，不需要等所有的客户端返回结果
		waitCh := make(chan struct{})
		
		var do int32 = 0

		go func(){
			time.Sleep(10 * time.Second)
			waitCh <- struct{}{}
		}()
		for idx := range rf.peers {
			go func(idx int) {
				if idx != rf.me {
					log.Printf("candidate %v requestVote to %v req %+v\n", rf.me, idx, req)
					reply := &RequestVoteReply{}
					if ok := rf.sendRequestVote(idx, req, reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.VoteGranted {
							if reply.Term == currentTerm {
								vote += 1
								if vote == majority + 1 {
									waitCh <- struct{}{}		
								}
							}
							// log.Printf("%v get granted from %v Term %v\n" , rf.me, idx, reply.Term)
						} else {
							if reply.Term > rf.currentTerm {
								// log.Printf("%v Term %v lower than %v Term %v\n", rf.me, rf.currentTerm, idx, reply.Term)
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								// 不是最大的term绝对不能成为leader	
								rf.persist()
								waitCh <- struct{}{}
							}
							
						}
					}
				}
			}(idx)
		}

		
		<- waitCh
		// rf.voteFor = -1 is necessary?
		// log.Printf("%v get total %v vote\n", rf.me, vote)
		// bug：如果上述rpc发生了延迟，rf.currentTerm可能已经升上去了，出现过期的vote。那就会脑裂。
		
		if vote > majority && rf.currentTerm == currentTerm && atomic.CompareAndSwapInt32(&do, 0, 1){
			log.Printf("%v get total %v vote major %v Term %v\n", rf.me, vote, majority, currentTerm)
			if rf.nextIndex == nil {
				rf.nextIndex = make([]int, len(rf.peers))
			}
			for idx := range rf.nextIndex {
				nextIndex := 0
				if len(rf.logs) > 0 {
					lastLogEntry := rf.logs[len(rf.logs)-1]
					nextIndex = lastLogEntry.Index
				}
				rf.nextIndex[idx] = nextIndex
			}

			if rf.matchIndex == nil {
				rf.matchIndex = make([]int, len(rf.peers))
				for idx := range rf.matchIndex {
					rf.matchIndex[idx] = -1
				}
				// rf.matchIndex[rf.me] = - 1
				if len(rf.logs) > 0 {
					rf.matchIndex[rf.me] = rf.logs[len(rf.logs)-1].Index
				}
			}
			rf.leader = rf.me
			rf.votedFor = -1
			rf.persist()
			// log.Printf("%v become leader Term %v\n", rf.me, rf.currentTerm)
			go rf.appendEntries()
		}
	}
}

func (rf *Raft) findLogEntryPositionWithIndex(idx int) int {
	return findLogEntryPositionWithIndex(rf.logs, idx)
}

// 返回全局IDX log在logs中的相对位置（防止snapshot截断）
func findLogEntryPositionWithIndex(entries []LogEntry, idx int) int {
	if entries == nil || len(entries) == 0 {
		return -1
	}

	firstLogEntry := entries[0]
	return idx - firstLogEntry.Index
}

// 寻找idx logEntry 上一个term 第一个log idx
func findPriorTermFirstLogEntry(entries []LogEntry, idx int) int {
	pos := findLogEntryPositionWithIndex(entries, idx)
	if pos == -1 {
		return 0
	}
	// 当前idx的LogEntry
	term := entries[pos].Term

	for pos = pos - 1; pos >= 0 && entries[pos].Term == term; pos-- {

	}

	if pos < 0 {
		// TODO snapshot
		return 0
	}

	// 先前term 的LogEntry
	term = entries[pos].Term
	for ; pos >= 0 && entries[pos].Term == term; pos-- {

	}
	pos = pos + 1

	return pos

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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.leader = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.logs = make([]LogEntry, 0)
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// electionTimeout = 300 ms - 800ms
	//rf.electionTimeout = time.Duration(rf.me*2%30+50) * 10 * time.Millisecond
	if rf.electionTimeout == 0{
		timeout := time.Duration(randG.Intn(50) + 30) * 10 * time.Millisecond
		for {
			if _, ok := electionTimeoutSet[timeout]; !ok {
				electionTimeoutSet[timeout] = struct{}{}
				break
			}
			timeout = time.Duration(randG.Intn(50) + 30) * 10 * time.Millisecond
		}
		rf.electionTimeout = timeout
		rf.persist()
	}
	
	// log.Println(rf.me, " election Timeout ", electionTimeout)
	// boardcastTimeout = 100 ms
	rf.boardcastTimeout = 100 * time.Millisecond
	log.Printf("%v electionTimeout %v", rf.me, rf.electionTimeout)
	// heartbeat
	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			go rf.requestVote()
		}
	}()

	return rf
}
