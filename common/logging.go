package common

import zap "go.uber.org/zap"

var logger *zap.Logger

func InitLogger() {
	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func GetLogger() *zap.Logger {
	return logger
}
