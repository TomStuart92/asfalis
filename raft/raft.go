package raft

import (
	"bytes"
	"sort"
	"fmt"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	pb "github.com/TomStuart92/asfalis/raft/raftpb"
)

// None is a placeholder node ID used when there is no leader
const None uint64 = 0
const noLimit = math.MaxInt64

// ReadOnlyOption holds flag for read only
type ReadOnlyOption int

// StateType represents the role of a node in a cluster
type StateType uint64

// CampaignType represents the type of campaigning
type CampaignType string

// Possible values for StateType
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Read Only Options
const (
	// ReadOnlySafe guarantees the linearizability of the read only request by communicating with the quorum.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by relying on the leader lease
	ReadOnlyLeaseBased
)

// Possible values for Campaign Type
const (
	campaignPreElection CampaignType = "CampaignPreElection" // first phase of a normal election -> Config.PreVote = true
	campaignElection    CampaignType = "CampaignElection"    // normal time based election
	campaignTransfer    CampaignType = "CampaignTransfer"    // leader transfer
)

// ErrProposalDropped is returned when the proposal is ignored by some cases so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a mutex protected wrapper around rand.Rand
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// Config contains the parameters to start a raft
type Config struct {
	ID                        uint64         // ID of the local raft. Cannot be 0
	peers                     []uint64       // IDs of peer nodes, set at start-up
	learners                  []uint64       // IDs of learner nodes (recieve entries from the leader, but do not vote or self-promote)
	ElectionTick              int            // number of Node.Tick invocations that must pass between elections
	HeartbeatTick             int            // number of Node.Tick invocations that must pass between heartbeats from leader to followers
	Storage                   Storage        // persistent storage for raft
	Applied                   uint64         // The last applied index
	MaxSizePerMsg             uint64         // limits the max size of each append message. (math.MaxUint64 for unlimited, 0 for at most one entry per message)
	MaxInflightMsgs           int            // limits the max number of in-flight append messages during optimistic replication.
	CheckQuorum               bool           // specifies if the leader should check quorum activity.
	PreVote                   bool           // enables the Pre-Vote algorithm. Prevents disruption when a partitioned node rejoins
	ReadOnlyOption            ReadOnlyOption // specifies how read only requests are processed.
	Logger                    Logger         // Default Logger for raft
	DisableProposalForwarding bool           // specifies if followers should drop proposals instead of forwarding to the leader
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type raft struct {
	id   uint64
	Term uint64
	Vote uint64

	readStates []ReadState

	raftLog *raftLog

	maxInFlight int
	maxMsgSize  uint64
	prs         map[uint64]*Progress
	learnerPrs  map[uint64]*Progress
	matchBug    uint64Slice

	state     StateType
	isLearner bool

	votes map[uint64]bool
	msgs  []pb.Message

	lead             uint64
	leadTransferee   uint64
	pendingConfIndex uint64

	readOnly *readOnly

	electionElapsed  int
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout          int
	electionTimeout           int
	randomizedElectionTimeout int // randomizedElectionTimeout is a random number between [electiontimeout, 2 * electiontimeout - 1]
	disableProposalForwarding bool

	tick func()
	step stepFunc

	logger Logger
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLogWithardStateize(c.Storage, c.Logger, c.MaxSizePerMsg)
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := c.peers
	learners := c.learners

	if len(confState.Nodes) > 0 || len(confState.Learners) > 0 {
		// there is existing ConfState in storage
		if len(peers) > 0 || len(learners) > 0 {
			// we were told to create new peers/learners
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)")
		}
		peers = confState.Nodes
		learners = confState.Learners
	}

	r := &raft{
		id:          								c.ID,
		lead:        								None,
		isLearner:   								false,
		raftLog:    		 						raftlog,
		maxMsgSize:  								c.MaxSizePerMsg,
		maxInFlight:								c.MaxInflightMsgs,
		prs:       									make(map[uint64]*Progress),
		learnerPrs:  								make(map[uint64]*Progress),
		electionTimeout: 						c.ElectionTick,
		heartbeatTimeout: 					c.HeartbeatTick,
		logger: 										c.Logger,
		checkQuorum: 								c.CheckQuorum,
		preVote: 										c.PreVote,
		readOnly: 									newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: 	c.DisableProposalForwarding,
	}

	for _, p := range peers {
		r.prs[p] = &Progress{ Next: 1, ins: newInflights(r.maxInFlight) }
	}
	for _, p := range learners {
		if _, ok := r.prs[p]; ok {
			panic(fmt.Sprintf("node %x is in both learner and peer list", p))
		}
		r.learnerPrs[p] = &Progress{ Next: 1, ins: newInflights(r.maxInFlight), isLearner: true }
		if r.id == p {
			r.isLearner = true
		}
	}

	if !isHardStateEqual(hardState, emptyState) {
		r.loadState(hardState)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string

	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof(
		"newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm()
	)
	return r
}

func (r *raft) hasLeader() bool {
	return r.lead != None
}

func (r *raft) softState() *SoftState {
	return &SoftState{ Lead: r.lead, RaftState: r.state} 
}

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term: r.Term,
		Vote: r.Vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) quorum() int {
	return len(r.prs) / 2 + 1
}

func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.learnprsrPrs))
	for id := range r.prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func (r *raft) learnerNodes() []uint64 {
	nodes := make([]uint64, 0, len(r.learnerPrs))
	for id := range r.learnerPrs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func (r *raft) send(m pb.Message) {
	m.From = r.id
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}

		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *raft) getProgress(id uint64) *Progress {
	if pr, ok := r.prs[id]; ok {
		return pr
	}
	return r.learnerPrs[id]
}

func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.getProgress(to)
	if pr.IsPaused() {
		return false
	}
	m := pb.Message{}
	m.To = to

	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	if errt != nil || erre != nil {
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot it temporarily unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		if IsEmptySnapshot(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
		r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		m.Type = pb.MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.raftLog.committed

		if n := len(m.Entries); n != 0 {
			switch pr.State {
			case ProgressStateReplicate:
				last := m.Entries[n-1].Index
				pr.optimisticUpdate(last)
				pr.ins.add(last)
			case ProgressStateProbe:
				pr.pause()
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	r.send(m)
	return true
}


func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	commit := min(r.getProgress(to).Match, r.raftLog.committed)
	m := pb.Message{
		To: to,
		Type: pb.MsgHeartbeat,
		Commit: commit, 
		Context: ctx,
	}
	r.send(m)
}

func (r *raft) forEachProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.prs {
		f(id, pr)
	}
	for id, pr := range r.learnerPrs {
		f(id, pr)
	}
}

func (r *raft) bcastAppend() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
}

func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id, ctx)
	})
}

func (r *raft) maybeCommit() bool {
	if cap(r.matchBuf) < len(r.prs) {
		r.matchBuf = make(uint64Slice, len(r.prs))
	}
	mis := r.matchBuf[:len(r.prs)]
	idx := 0
	for _, p := range r.prs {
		mis[idx] = p.Match
		idx++
	}
	sort.Sort(mis)
	mci := mis[len(mis) - r.quorum()]
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	r.forEachProgress(func(id uint64, pr *Progress) {
		*pr = Progress{Next: r.raftLog.lastIndex + 1, ins: newInflights(r.maxInFlight), isLearner: pr.IsLearner}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	})

	r.pendingConfIndex = 0
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...pb.Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term =  r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	li = r.raftLog.append(es...)
	r.getProgress(r.id).maybeUpdate(li)
	r.maybeCommit()
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			r.Step(pb.Message{ From: r.id, Type: pb.MsgCheckQuorum })
		}
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{ From: r.id, Type: pb.MsgBeat })
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

func (r *raft) becomePreCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	r.step = stepCandidate
	r.votes = make(map[uint64]bool)
	r.tick = r.tickElection
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader

	r.pendingConfIndex = r.raftLog.lastIndex()
	r.appendEntry(pb.Entry{Data: nil})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r * raft) campaign(t CampaignType) {
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}

	if r.quorum()  == r.poll(r.id, voteRespMsgType(voteMsg), true) {
		// we won election after voting for ourselves -> single node cluster
		if t == campaignPreElection {
			r.campaign(campaignElection) 
		} else {
			r.becomeLeader()
		}
		return
	}
	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)
		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx })
	}
}

func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

func (r *raft) Step(m pb.Message) error {
	switch {
		case m.Term == 0:
			// local message
		case m.Term > r.Term:
			if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
				force := bytes.Equal(m.Context, []byte(campaignTransfer))
				inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
				if !force && inLease {
					r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
				}
			}
			switch {
			case m.Type == pb.MsgPreVote:
				// dont change our term in response to a PreVote
			case m.Type == pb.MsgPreVoteResp && !m.Reject:
				// We send pre-vote requests with a term in our future. If the
				// pre-vote is granted, we will increment our term when we get a
				// quorum. If it is not, the term comes from the node that
				// rejected our vote so we should become a follower at the new
				// term.
			default: 
				r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
				if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
					r.becomeFollower(m.Term, m.From)
				} else {
					r.becomeFollower(m.Term, None)
				}
			}
	}
}
