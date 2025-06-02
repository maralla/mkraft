package common

import "errors"

func ContextDoneErr() error {
	return errors.New("context done")
}

// raft log errors
var ErrPreLogNotMatch = errors.New("prelog not match")
var ErrNotLeader = errors.New("not leader")
var ErrServerBusy = errors.New("server busy")
