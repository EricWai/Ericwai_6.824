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
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Sucess      bool
	ApdState    AppendEntriesState
	UpNextIndex int
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type rfState int
type VoteState int
type AppendEntriesState int

var HeartBeatsTimeout = 35 * time.Millisecond

const (
	Normal VoteState = iota
	Killed
	Expire
	Voted
)

const (
	Follower rfState = iota
	Candidate
	Leader
)

const (
	ApdNormal AppendEntriesState = iota
	ApdOutOfDate
	ApdKilled
	ApdCommitted
	MisMatch
)

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	//Persistent state
	currentTerm int
	voteFor     int
	log         []LogEntry

	//Volatile state
	commitIndex int
	lastApplied int
	state       rfState
	overTime    time.Duration
	tkr         *time.Ticker
	//Volatile state for leaders
	nextIndex  []int
	matchIndex []int

	applyChan chan ApplyMsg

	lastIncludeIndex int
	lastIncludeTerm  int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) getLastIndex() int {
	return rf.lastIncludeIndex + len(rf.log)
}

func (rf *Raft) getIndexLog(index int) LogEntry {
	if index <= rf.lastIncludeIndex {
		return LogEntry{}
	} else {
		return rf.log[index-rf.lastIncludeIndex-1]
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
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
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(rf.xxx)
	//e.Encode(rf.yyy)
	//data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeTerm)
	e.Encode(rf.lastIncludeIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	return
}

// restore previously persisted state.
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
	var voteFor int
	var log []LogEntry
	var lastIncludeTerm int
	var lastIncludeIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeTerm) != nil ||
		d.Decode(&lastIncludeIndex) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
		rf.lastIncludeTerm = lastIncludeTerm
		rf.lastIncludeIndex = lastIncludeIndex
	}
	return
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}
	newLogs := make([]LogEntry, 0)
	for i := index - rf.lastIncludeIndex; i < rf.getLastIndex()-rf.lastIncludeIndex; i++ {
		newLogs = append(newLogs, rf.log[i])
	}
	rf.lastIncludeTerm = rf.getIndexLog(index).Term
	rf.lastIncludeIndex = index
	rf.log = newLogs
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	VoteState   VoteState
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Term [%d]: %d recieve the request from %d\n", args.Term, rf.me, args.CandidateId)
	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	rf.resetTicker()
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}
	if rf.voteFor == -1 {
		currentLogIndex := rf.getLastIndex()
		currentLogTerm := 0
		if len(rf.log) > 0 {
			currentLogTerm = rf.log[len(rf.log)-1].Term
		} else {
			currentLogTerm = rf.lastIncludeTerm
		}
		//fmt.Printf("%v ca Term [%v] Index %v\n", args.CandidateId, args.LastLogTerm, args.LastLogIndex)
		//fmt.Printf("%v my Term [%v] Index %v\n", rf.me, currentLogTerm, currentLogIndex)
		if args.LastLogTerm > currentLogTerm || (args.LastLogTerm == currentLogTerm && args.LastLogIndex >= currentLogIndex) {
			rf.voteFor = args.CandidateId
			rf.persist()
			reply.VoteState = Normal
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			//fmt.Printf("Term [%d]: %d vote to %d\n", rf.currentTerm, rf.me, args.CandidateId)
			return
		} else {
			reply.VoteState = Expire
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else {
		reply.VoteState = Voted
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() || rf.state == Leader {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return false
	}
	if rf.killed() || rf.state == Leader {
		return false
	}
	rf.resetTicker()
	switch reply.VoteState {
	case Expire:
		rf.state = Follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			rf.persist()
		}
	case Normal, Voted:
		if reply.VoteGranted && reply.Term == rf.currentTerm {
			*voteNum++
			//fmt.Printf("Term [%d]: %d get %d votes!!!!!!!!!!!!!!!!!!!!!!!!\n", rf.currentTerm, rf.me, *voteNum)
		}
		if *voteNum > len(rf.peers)/2 {
			//fmt.Printf("Term [%d]: %d became the Leader\n", rf.currentTerm, rf.me)
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.getLastIndex() + 1
			}
			rf.startHeartBeats()
			rf.resetTicker()
		}
	case Killed:
		return false
	}
	return ok
}
func (rf *Raft) getIndexTerm(index int) int {
	if index == rf.lastIncludeIndex {
		return rf.lastIncludeTerm
	} else if index > rf.lastIncludeIndex && index <= rf.getLastIndex() {
		return rf.getIndexLog(index).Term
	} else {
		return -1
	}
}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.ApdState = ApdKilled
		reply.Term = -1
		reply.Sucess = false
		return
	}
	rf.resetTicker()
	if args.Term < rf.currentTerm {
		reply.ApdState = ApdOutOfDate
		reply.Term = rf.currentTerm
		reply.Sucess = false
		return
	}

	if rf.lastApplied > args.PrevLogIndex {
		reply.ApdState = ApdCommitted
		reply.Term = rf.currentTerm
		reply.Sucess = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}

	if args.PrevLogIndex > 0 && (rf.getLastIndex() < args.PrevLogIndex || rf.getIndexTerm(args.PrevLogIndex) != args.PrevLogTerm) {
		reply.ApdState = MisMatch
		reply.Term = rf.currentTerm
		reply.Sucess = false
		//fmt.Println("rf.getLastIndex", rf.getLastIndex(), "args.PrevLogIndex", args.PrevLogIndex, "rf.lastApplied", rf.lastApplied)
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}
	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderId
	rf.state = Follower

	reply.ApdState = ApdNormal
	reply.Term = rf.currentTerm
	reply.Sucess = true

	if args.Entries != nil {
		rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludeIndex]
		//fmt.Printf("Term [%v]: %v %v append %v\n", rf.currentTerm, rf.me, rf.log, args.Entries)
		rf.log = append(rf.log, args.Entries...)
	}

	rf.persist()

	for rf.commitIndex < args.LeaderCommit && rf.commitIndex < rf.getLastIndex() {
		rf.commitIndex++
		//fmt.Printf("Term [%v]: %v commit %v\n", rf.currentTerm, rf.me, rf.commitIndex)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.getIndexLog(rf.commitIndex).Command,
			CommandIndex: rf.commitIndex,
		}
		rf.mu.Unlock()
		rf.applyChan <- applyMsg
		rf.mu.Lock()
		rf.lastApplied = rf.commitIndex
		//fmt.Printf("Term [%v]: %v applied %v\n", rf.currentTerm, rf.me, rf.lastApplied)
	}
	return
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) {
	if rf.killed() {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntry", args, reply)
	}
	switch reply.ApdState {
	case ApdKilled:
		return
	case ApdNormal:
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Sucess && reply.Term == rf.currentTerm && *appendNum <= len(rf.peers)/2 {
			//fmt.Printf("Term [%v]: %v know %v sucess append\n", rf.currentTerm, rf.me, server)
			*appendNum++
		}
		if rf.nextIndex[server] > rf.getLastIndex()+1 {
			return
		}
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		if *appendNum > len(rf.peers)/2 {
			*appendNum = 0
			if len(rf.log) == 0 || rf.log[len(rf.log)-1].Term != rf.currentTerm {
				return
			}

			//for rf.commitIndex < len(rf.log) {
			for rf.commitIndex < args.PrevLogIndex+len(args.Entries) {
				rf.commitIndex++
				//fmt.Printf("Term [%v]: %v commit %v\n", rf.currentTerm, rf.me, rf.commitIndex)
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.getIndexLog(rf.commitIndex).Command,
					CommandIndex: rf.commitIndex,
				}
				rf.mu.Unlock()
				rf.applyChan <- applyMsg
				rf.mu.Lock()
				rf.lastApplied = rf.commitIndex
				//fmt.Printf("Term [%v]: %v applied %v\n", rf.currentTerm, rf.me, rf.lastApplied)
			}
		}
		return
	case MisMatch, ApdCommitted:
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.voteFor = -1
			rf.resetTicker()
			rf.persist()
			return
		}
		rf.nextIndex[server] = reply.UpNextIndex
	case ApdOutOfDate:
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.state = Follower
		rf.voteFor = -1
		rf.resetTicker()
		rf.currentTerm = reply.Term
		rf.persist()
	}
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Term [%d]: %v want to InstallSnapshot\n", rf.currentTerm, rf.me)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.voteFor = -1
	rf.persist()
	rf.resetTicker()
	reply.Term = rf.currentTerm
	if args.LastIncludeIndex <= rf.lastIncludeIndex {
		return
	}
	newlog := make([]LogEntry, 0)
	for i := args.LastIncludeIndex - rf.lastIncludeIndex; i < rf.getLastIndex()-rf.lastIncludeIndex; i++ {
		newlog = append(newlog, rf.log[i])
	}
	rf.log = newlog
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	if args.LastIncludeIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludeIndex
	}
	if args.LastIncludeIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludeIndex
	}
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludeIndex,
		SnapshotTerm:  args.LastIncludeTerm,
	}
	rf.applyChan <- msg
	//fmt.Printf("Term [%d]: %v sucess InstallSnapshot to %v\n", rf.currentTerm, rf.me, rf.lastIncludeIndex)
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok == true {
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.resetTicker()
			rf.voteFor = -1
			rf.persist()
			return
		}
		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1
		return
	}
	return
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}

	isLeader = true
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	//fmt.Printf("Term [%v]: %v %v append %v\n", rf.currentTerm, rf.me, rf.log, appendLog)
	rf.log = append(rf.log, appendLog)
	index = rf.getLastIndex()
	term = rf.currentTerm
	rf.persist()
	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.tkr.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<-rf.tkr.C
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		switch rf.state {
		case Follower:
			rf.state = Candidate
			fallthrough
		case Candidate:
			rf.startElection()
		case Leader:
			rf.startHeartBeats()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetTicker() {
	switch rf.state {
	case Follower:
		rf.overTime = time.Duration(100+rand.Intn(100)) * time.Millisecond
		rf.tkr.Reset(rf.overTime)
	case Candidate:
		rf.overTime = time.Duration(100+rand.Intn(100)) * time.Millisecond
		rf.tkr.Reset(rf.overTime)
	case Leader:
		rf.overTime = HeartBeatsTimeout
		rf.tkr.Reset(rf.overTime)
	}
}

func (rf *Raft) startElection() {
	//fmt.Printf("Term [%d]: %d start a Election\n", rf.currentTerm, rf.me)
	rf.currentTerm++
	rf.voteFor = rf.me
	voteNum := 1
	rf.persist()
	rf.resetTicker()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		voteArgs := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastIndex(),
			LastLogTerm:  rf.lastIncludeTerm,
		}
		if len(rf.log) > 0 {
			voteArgs.LastLogTerm = rf.getIndexTerm(voteArgs.LastLogIndex)
		}
		voteReply := RequestVoteReply{}
		go rf.sendRequestVote(i, &voteArgs, &voteReply, &voteNum)
	}
}

func (rf *Raft) startHeartBeats() {
	//fmt.Printf("Term [%d]: %d send a HeartBeats\n", rf.currentTerm, rf.me)
	appendNum := 1
	rf.resetTicker()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogTerm:  0,
			PrevLogIndex: 0,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		if rf.nextIndex[i] <= rf.lastIncludeIndex {
			go rf.sendInstallSnapshot(i)
			return
		}
		if rf.nextIndex[i] <= rf.getLastIndex() {
			args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]-rf.lastIncludeIndex-1:]))
			copy(args.Entries, rf.log[rf.nextIndex[i]-rf.lastIncludeIndex-1:])
		}

		if rf.nextIndex[i] > 0 {
			args.PrevLogIndex = rf.nextIndex[i] - 1
		}
		if args.PrevLogIndex > 0 {
			args.PrevLogTerm = rf.getIndexTerm(args.PrevLogIndex)
		}

		go rf.sendAppendEntry(i, &args, &reply, &appendNum)
	}
	return
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = Follower
	rf.overTime = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.tkr = time.NewTicker(rf.overTime)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.lastIncludeIndex
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
