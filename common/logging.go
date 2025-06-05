package common

import (
	zap "go.uber.org/zap"
)

// todo: add get the env variable for the log config,
// currently, only use dev config
func CreateLogger() (*zap.Logger, error) {
	return zap.NewDevelopment()
}
