package conf

import (
	"math/rand"
	"time"
)

const LEADER_HEARTBEAT_PERIOD_IN_MS = 100
const REUQEST_TIMEOUT_IN_MS = 200
const RPC_REUQEST_TIMEOUT_IN_MS = 200

// This non-determinisim is by design
// because sometimes randomness brings simplicity
// here it minimizes the possiblity of contention of leader and split vote
const ELECTION_TIMEOUT_MIN_IN_MS = 150
const ELECTION_TIMEOUT_MAX_IN_MS = 350

func GetRandomElectionTimeout() time.Duration {
	diff := ELECTION_TIMEOUT_MAX_IN_MS - ELECTION_TIMEOUT_MIN_IN_MS
	randomMs := rand.Intn(diff) + ELECTION_TIMEOUT_MIN_IN_MS
	return time.Duration(randomMs) * time.Millisecond
}
