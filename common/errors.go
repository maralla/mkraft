package common

import "errors"

func ContextDoneErr() error {
	return errors.New("context done")
}
