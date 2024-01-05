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
	"encoding/gob"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// LogEntry
//each entry contains command for state machine,
//and term when entry was received by leader (first index is 1)
//
type LogEntry struct {
	Command interface{}
	Term    int
}

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

const HeartbeatTime = 50

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//

	//Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all server
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//customized state
	role     Role
	timer    *time.Timer
	isCommit chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	DPrintf("Sever %d Enter GetState", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	DPrintf("Sever %d Return GetState", rf.me)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

func IsLogNew(term1, index1, term2, index2 int) bool {
	if term1 != term2 {
		return term1 > term2
	} else {
		return index1 > index2
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// If election timeout elapses without receiving AppendEntries
	//RPC from current leader or granting vote to candidate
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.ActFollower(args.Term)
	}
	// only one possibility: eq
	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		!IsLogNew(rf.log[len(rf.log)-1].Term, len(rf.log)-1, args.LastLogTerm, args.LastLogIndex) {
		//DPrintf("Server %d receive server RequestVote %d accept", rf.me, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else { // already voted other
		//DPrintf("Server %d receive server RequestVote %d reject", rf.me, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = false
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term       int  //currentTerm, for leader to update itself
	Success    bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	RetryIndex int  //retry at which index
}

// for AppendEntries handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("Sever %d receive AppendEntries", rf.me)
	resetFlag := false
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		reply.Success = true
		rf.ActFollower(args.Term) //will set term = args.Term
		resetFlag = true
	}
	// only two options here
	//  Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("Sever %d receive AppendEntries fast return", rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = true
		if rf.role == Candidate {
			rf.ActFollower(rf.currentTerm)
		}
		if rf.role == Follower {
			// refresh timer
			if !resetFlag {
				rf.timer.Reset(RandomElectionTime())
			}
		}
		if args.Entries == nil && args.LeaderCommit <= rf.commitIndex && args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
			rf.persist()
			rf.mu.Unlock()
			return
		}
		DPrintf("Sever %d receive size %d log entries", rf.me, len(args.Entries))
		// Reply false if log doesn’t contain an entry at prevLogIndex
		//whose term matches prevLogTerm
		if args.PrevLogIndex > len(rf.log)-1 {
			reply.Success = false
			reply.RetryIndex = len(rf.log)
			rf.mu.Unlock()
			return
		}
		// detect conflict
		// If an existing entry conflicts with a new one (same index
		//but different terms), delete the existing entry and all that follow it
		prevTerm := rf.log[args.PrevLogIndex].Term
		if prevTerm != args.PrevLogTerm {
			DPrintf("Conflict: Server %d args.PrevLogTerm %d prevTerm %d", rf.me, args.PrevLogTerm, prevTerm)
			reply.Success = false
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != prevTerm {
					reply.RetryIndex = i + 1
					rf.persist()
					rf.mu.Unlock()
					return
				}
			}
		}
		rf.log = rf.log[:args.PrevLogIndex+1]
		//  Append any new entries not already in the log
		DPrintf("Server %d successfully append at index %d /leaderCommit:%d commitIndex:%d", rf.me, len(rf.log), args.LeaderCommit, rf.commitIndex)
		rf.log = append(rf.log, args.Entries...)
		//If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			lastIndex := len(rf.log) - 1
			DPrintf("Server %d update commitIndex/lastIndex %d LeaderCommit %d", rf.me, lastIndex, args.LeaderCommit)
			if args.LeaderCommit > lastIndex {
				rf.commitIndex = lastIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.persist()
			rf.mu.Unlock()
			rf.isCommit <- 1
			return
		}
		rf.persist()
		rf.mu.Unlock()
		return
	}

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.role == Leader {
		isLeader = true
	} else {
		isLeader = false
	}
	if !isLeader {
		return index, term, isLeader
	}

	// leader should append Log and broadcast
	index = len(rf.log)
	DPrintf("Leader %d Start agreement at index %d command %d", rf.me, index, command)
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	go rf.Broadcast()
	DPrintf("Leader %d exit", rf.me)
	rf.persist()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	rf.role = Follower
	rf.currentTerm = 0           //initialized to 0 on first boot, increases monotonically
	rf.log = make([]LogEntry, 1) //first index is 1
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.isCommit = make(chan int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.ActFollower(rf.currentTerm)
	go rf.ApplyLog(applyCh)
	return rf
}

// RandomElectionTime
//To prevent split votes in the first place, election timeouts are
//chosen randomly from a fixed interval (e.g., 150–300ms)
func RandomElectionTime() time.Duration {
	minTime := 150
	maxTime := 300
	return time.Duration(minTime+rand.Intn(maxTime-minTime)) * time.Millisecond
}

// define three state func
func (rf *Raft) ActFollower(term int) {
	rf.role = Follower
	// when a new term , vote set to null
	rf.votedFor = -1
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower
	rf.currentTerm = term
	rf.timer = time.NewTimer(RandomElectionTime())
	DPrintf("Sever %d Follower Term %d timer %p", rf.me, rf.currentTerm, rf.timer)
	// election wait timeout
	go func(timer *time.Timer) {
		storeTerm := rf.currentTerm
		<-timer.C
		DPrintf("Sever %d timer %p timeout", rf.me, timer)
		rf.mu.Lock()
		// if step into other state, kill the goroutine
		if rf.role != Follower || rf.currentTerm > storeTerm {
			DPrintf("delete Sever %d timer %p", rf.me, timer)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go rf.ActCandidate()
	}(rf.timer)
}

// ActCandidate
//	On conversion to candidate, start election:
//• Increment currentTerm
//• Vote for self
//• Reset election timer
//• Send RequestVote RPCs to all other servers
func (rf *Raft) ActCandidate() {
	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	DPrintf("Sever %d Candidate Term %d", rf.me, rf.currentTerm)
	// If election timeout elapses: start new election
	go func() {
		rf.timer.Reset(RandomElectionTime())
		go rf.Election()
		for range rf.timer.C {
			rf.mu.Lock()
			if rf.role == Candidate {
				DPrintf("Sever %d reElection", rf.me)
				// start a new election by incrementing its term and initiating another round of RequestVote RPCs.
				rf.currentTerm += 1
				rf.mu.Unlock()
				rf.timer.Reset(RandomElectionTime())
				go rf.Election()
			} else {
				rf.mu.Unlock()
				DPrintf("Sever %d exit Candidate Election loop /role %d", rf.me, rf.role)
				return
			}
		}
	}()
}

func (rf *Raft) ActLeader() {
	rf.role = Leader
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	DPrintf("Sever %d Leader Term %d", rf.me, rf.currentTerm)
	go func() {
		storeTerm := rf.currentTerm
		for {
			rf.mu.Lock()
			// if step into other state, kill the goroutine
			if rf.role != Leader || rf.currentTerm > storeTerm {
				rf.mu.Unlock()
				return
			}
			// send heartbeat
			rf.mu.Unlock()
			DPrintf("Sever %d act Leader broadcast", rf.me)
			go rf.Broadcast()
			time.Sleep(time.Duration(HeartbeatTime) * time.Millisecond)
		}
	}()
}

func (rf *Raft) Broadcast() {
	rf.mu.Lock()
	storeLastLogIndex := len(rf.log) - 1
	rf.matchIndex[rf.me] = storeLastLogIndex
	peerNum := len(rf.peers)
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	rf.mu.Unlock()
	for i := 0; i < peerNum; i++ {
		if rf.me == i {
			continue
		}
		go func(i int) {
			// fast return if not leader any longer
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				DPrintf("Sever %d exit Leader /role %d", rf.me, rf.role)
				return
			}
			newargs := AppendEntriesArgs{Term: args.Term, LeaderId: args.LeaderId}
			newargs.LeaderCommit = rf.commitIndex
			newargs.PrevLogIndex = rf.nextIndex[i] - 1
			if newargs.PrevLogIndex > len(rf.log)-1 {
				newargs.PrevLogIndex = len(rf.log) - 1
			}
			newargs.PrevLogTerm = rf.log[newargs.PrevLogIndex].Term
			if storeLastLogIndex >= rf.nextIndex[i] {
				newargs.Entries = rf.log[rf.nextIndex[i]:]
				DPrintf("Sever %d len of log %d PrevLogIndex %d", rf.me, len(rf.log), newargs.PrevLogIndex)
			}
			reply := new(AppendEntriesReply)
			rf.mu.Unlock()
			if ok := rf.sendAppendEntries(i, newargs, reply); ok {
				if reply.Term > newargs.Term {
					rf.ActFollower(reply.Term)
					return
				}
				if reply.Success {
					rf.mu.Lock()
					// update nextIndex and matchIndex
					rf.nextIndex[i] = storeLastLogIndex + 1
					rf.matchIndex[i] = storeLastLogIndex
					// evaluate matchIndex and update commitIndex
					matchIndexCopy := make([]int, len(rf.peers))
					copy(matchIndexCopy, rf.matchIndex)
					sort.Sort(sort.IntSlice(matchIndexCopy))
					maxIdx := matchIndexCopy[(len(rf.peers)-1)/2]
					DPrintf("Server %d append server %d success, maxIdx %d commitIndex %d", rf.me, i, maxIdx, rf.commitIndex)
					if rf.log[maxIdx].Term == rf.currentTerm && maxIdx > rf.commitIndex {
						rf.commitIndex = maxIdx
						rf.mu.Unlock()
						rf.isCommit <- 1
						return
					}
					rf.mu.Unlock()
				} else {
					rf.nextIndex[i] = reply.RetryIndex
					DPrintf("Sever %d reply.RetryIndex %d", i, reply.RetryIndex)
				}
			}
		}(i)
	}
}

func (rf *Raft) Election() {
	currentVotes := 1
	rf.mu.Lock()
	storeTerm := rf.currentTerm
	storeLastLogIndex := len(rf.log) - 1
	storeLastLogTerm := rf.log[storeLastLogIndex].Term
	r := RequestVoteArgs{Term: storeTerm, CandidateId: rf.me, LastLogIndex: storeLastLogIndex, LastLogTerm: storeLastLogTerm}
	peerNum := len(rf.peers)
	rf.mu.Unlock()
	for i := range rf.peers {
		if rf.me == i {
			continue
		}
		go func(i int) {
			reply := new(RequestVoteReply)
			if ok := rf.sendRequestVote(i, r, reply); ok {
				term, vote := reply.Term, reply.VoteGranted
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// transfer to other role
				if rf.role != Candidate {
					return
				}
				if vote {
					currentVotes += 1
					if currentVotes > peerNum/2 {
						go rf.ActLeader()
						return
					}
				}
				if term > rf.currentTerm {
					go rf.ActFollower(term)
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) ApplyLog(applyCh chan ApplyMsg) {
	// notified commit
	for range rf.isCommit {
		rf.mu.Lock()
		DPrintf("Server %d apply log", rf.me)
		if rf.commitIndex > rf.lastApplied {
			// apply log entries
			for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
				applyCh <- ApplyMsg{Index: idx, Command: rf.log[idx].Command}
				rf.lastApplied++
			}
		}
		rf.mu.Unlock()
	}
}
