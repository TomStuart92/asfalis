package transport

import (
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/pkg/types"
)

type failureType struct {
	source string
	action string
}

type peerStatus struct {
	local  types.ID
	id     types.ID
	mu     sync.Mutex // protect variables below
	active bool
	since  time.Time
}

func newPeerStatus(local, id types.ID) *peerStatus {
	return &peerStatus{local: local, id: id}
}

func (s *peerStatus) activate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		log.Infof("peer %s became active", s.id)
		s.active = true
		s.since = time.Now()
	}
}

func (s *peerStatus) deactivate(failure failureType, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := fmt.Sprintf("failed to %s %s on %s (%s)", failure.action, s.id, failure.source, reason)
	if s.active {
		log.Errorf(msg)
		log.Infof("peer %s became inactive (message send to peer failed)", s.id)
		s.active = false
		s.since = time.Time{}
		return
	}
	log.Debug("peer deactivated again")
}

func (s *peerStatus) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *peerStatus) activeSince() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.since
}
