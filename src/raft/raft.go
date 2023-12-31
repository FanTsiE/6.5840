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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

type Entry struct {
    Command interface{}
    Term    int
	Index  	int
}

type NodeState int
const (
	Follower NodeState = iota
	Candidate
	Leader
)


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg
	applyCond *sync.Cond
	repliCond []*sync.Cond
	state NodeState
	currentTerm int
	votedFor int
	log []Entry

	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int

	electionTimeout time.Duration
	heartbeatTimeout time.Duration

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	if term < rf.currentTerm {
		term = rf.currentTerm
		isleader = false
	} else if term == rf.currentTerm{
		isleader = true
	} else {
		term = rf.currentTerm
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.commitIndex {
		return
	}
	rf.log = rf.log[index:]
	rf.commitIndex = index
	rf.lastApplied = index
	rf.persist()

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	currentTerm int
	voteGranted bool
}

type AppendEntriesArgs struct {
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	entries []Entry
	leaderCommit int
}

type AppendEntriesReply struct {
	term int
	success bool
	conflictIndex int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("Node %d receive RequestVote from %d", rf.me, args.candidateId)
	var term = args.term
	var candidateId = args.candidateId
	if term < rf.currentTerm {
		reply.currentTerm = rf.currentTerm
		reply.voteGranted = false
	} 
	if term >= rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == candidateId {
			reply.currentTerm = rf.currentTerm
			reply.voteGranted = true
			rf.votedFor = candidateId
			rf.state = Follower
		} else {
			reply.currentTerm = rf.currentTerm
			reply.voteGranted = false
		}
	}
	rf.electionTimeout = time.Duration(300 + rand.Int63n(300)) * time.Millisecond
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
	}
	if rf.state == Leader {
		entry := Entry{command, rf.currentTerm, len(rf.log)}
		rf.log = append(rf.log, entry)
		index = len(rf.log) - 1
		term = rf.currentTerm
		rf.persist()
	}

	return index, term, isLeader
}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
			case <- time.After(rf.electionTimeout):
				rf.mu.Lock()
				rf.state = Candidate
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.persist()
				rf.mu.Unlock()
				rf.startElection()
				rf.electionTimeout = time.Duration(300 + rand.Int63n(300)) * time.Millisecond
			case <- time.After(rf.heartbeatTimeout):
				rf.mu.Lock()
				if rf.state == Leader {
					rf.broadcastAppendEntries()
					rf.heartbeatTimeout = time.Duration(100) * time.Millisecond
				}
				rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		 ms := 50 + (rand.Int63() % 300)
		 time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func(rf *Raft) broadcastAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.replicator(i)
		}
	}
}

func (rf *Raft) startElection() {
	request := RequestVoteArgs{}
	request.term = rf.currentTerm
	request.candidateId = rf.me
	request.lastLogIndex = len(rf.log) - 1
	request.lastLogTerm = rf.log[request.lastLogIndex].Term
	DPrintf("Node %d start election for term %d", rf.me, rf.currentTerm)
	granted := 1
	rf.votedFor = rf.me
	rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(i, &request, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.voteGranted {
						granted += 1
						if granted > len(rf.peers) / 2 {
							rf.state = Leader
							rf.broadcastAppendEntries()
						}
					} else {
						if reply.currentTerm > rf.currentTerm {
							rf.currentTerm = reply.currentTerm
							rf.state = Follower
							rf.votedFor = -1
							rf.persist()
						}
					}
				}
			}(i)
		}
	}
}
 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("Node %d receive AppendEntries from %d", rf.me, args.leaderId)
	if args.term < rf.currentTerm {
		reply.term = rf.currentTerm
		reply.success = false
		return
	}
	if args.term >= rf.currentTerm {
		rf.currentTerm = args.term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	if args.prevLogIndex > len(rf.log) - 1 {
		reply.term = rf.currentTerm
		reply.success = false
		reply.conflictIndex = len(rf.log)
		DPrintf("Node %d reject AppendEntries from %d", rf.me, args.leaderId)
		return
	}
	if args.prevLogIndex >= 0 && rf.log[args.prevLogIndex].Term != args.prevLogTerm {
		reply.term = rf.currentTerm
		reply.success = false
		reply.conflictIndex = args.prevLogIndex
		DPrintf("Node %d reject AppendEntries from %d", rf.me, args.leaderId)
		return
	}
	if args.prevLogIndex >= 0 && rf.log[args.prevLogIndex].Term == args.prevLogTerm {
		rf.log = rf.log[:args.prevLogIndex + 1]
		rf.log = append(rf.log, args.entries...)
		reply.term = rf.currentTerm
		reply.success = true
		DPrintf("Node %d accept AppendEntries from %d", rf.me, args.leaderId)
	}
	if args.leaderCommit > rf.commitIndex {
		rf.commitIndex = args.leaderCommit
	}
	rf.applyCond.Signal()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getLastLog() Entry {
	if len(rf.log) == 0 {
		return Entry{nil, 0, 0}
	}
	return rf.log[len(rf.log) - 1]
}


func (rf *Raft) replicator(server int) {
	for rf.killed() == false {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.lastApplied >= rf.commitIndex {
			rf.repliCond[server].Wait()
		}
		if rf.state != Leader {
			return
		}
		var args AppendEntriesArgs
		args.term = rf.currentTerm
		args.leaderId = rf.me
		args.prevLogIndex = rf.nextIndex[server] - 1
		args.prevLogTerm = rf.log[args.prevLogIndex].Term
		args.entries = rf.log[rf.nextIndex[server]:]
		args.leaderCommit = rf.commitIndex
		var reply AppendEntriesReply
		if rf.sendAppendEntries(server, &args, &reply) {
			if reply.success {
				rf.nextIndex[server] = args.prevLogIndex + len(args.entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.repliCond[server].Signal()
			} else {
				if reply.term > rf.currentTerm {
					rf.currentTerm = reply.term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				} else {
					rf.nextIndex[server] = reply.conflictIndex
				}
			}
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
	}
}

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
	rf.dead = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.repliCond = make([]*sync.Cond, len(peers))
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Follower
	rf.electionTimeout = time.Duration(300 + rand.Int63n(300)) * time.Millisecond
	rf.heartbeatTimeout = time.Duration(100) * time.Millisecond
	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState())
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.Index + 1
		if i != me {
			rf.repliCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()


	return rf
}
