package common

import "errors"

func ContextDoneErr() error {
	return errors.New("context done")
}

// raft log errors
var ErrPreLogNotMatch = errors.New("prelog not match")
