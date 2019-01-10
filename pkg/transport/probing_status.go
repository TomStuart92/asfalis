package transport

import (
	"time"

	"github.com/xiang90/probing"
)

const (
	// RoundTripperNameRaftMessage is the name of round-tripper that sends
	// all other Raft messages, other than "snap.Message".
	RoundTripperNameRaftMessage = "ROUND_TRIPPER_RAFT_MESSAGE"
	// RoundTripperNameSnapshot is the name of round-tripper that sends merged snapshot message.
	RoundTripperNameSnapshot = "ROUND_TRIPPER_SNAPSHOT"
)

var (
	// proberInterval must be shorter than read timeout.
	// Or the connection will time-out.
	proberInterval           = ConnReadTimeout - time.Second
	statusMonitoringInterval = 30 * time.Second
	statusErrorInterval      = 5 * time.Second
)

func addPeerToProber(p probing.Prober, id string, us []string, roundTripperName string) {
	hus := make([]string, len(us))
	for i := range us {
		hus[i] = us[i] + ProbingPrefix
	}

	p.AddHTTP(id, proberInterval, hus)

	s, err := p.Status(id)
	if err != nil {
		log.Errorf("failed to add peer %s into prober", id)
		return
	}

	go monitorProbingStatus(s, id, roundTripperName)
}

func monitorProbingStatus(s probing.Status, id string, roundTripperName string) {
	// set the first interval short to log error early.
	interval := statusErrorInterval
	for {
		select {
		case <-time.After(interval):
			if !s.Health() {
				log.Warningf("health check for peer %s could not connect: %v", id, s.Err())
				interval = statusErrorInterval
			} else {
				interval = statusMonitoringInterval
			}
			if s.ClockDiff() > time.Second {
				log.Warningf("the clock difference against peer %s is too high [%v > %v]", id, s.ClockDiff(), time.Second)
			}

		case <-s.StopNotify():
			return
		}
	}
}
