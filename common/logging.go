package common

import (
	zap "go.uber.org/zap"
)

func CreateLogger() (*zap.Logger, error) {
	return zap.NewDevelopment()
}
