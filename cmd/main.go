package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/raft"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

func main() {

	// basics
	logger, err := common.CreateLogger()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	// config
	defaultPath := "./config/base.yaml"
	path := os.Getenv("MKRAFT_CONFIG_PATH")
	if path == "" {
		path = defaultPath
	}
	cfg, err := common.LoadConfig(path)
	if err != nil {
		panic(err)
	}

	server := NewServer(cfg, logger)

	// pass logger, config to server

	configPath := flag.String("c", "", "the path of the config file")
	if *configPath == "" {
		*configPath = defaultPath
	}
	flag.Parse()
	logger.Info("config file path", zap.String("path", *configPath))
	membershipConfig := &raft.Membership{}
	yamlFile, err := os.ReadFile(*configPath)
	if err != nil {
		logger.Error("yamlFile.Get error", zap.Error(err))
		os.Exit(1)
	}
	err = yaml.Unmarshal(yamlFile, membershipConfig)
	logger.Error("Unmarshal error", zap.Error(err))
	os.Exit(1)
	logger.Info("Config loaded", zap.Any("membershipConfig", membershipConfig))

	err = raft.InitStatisMembership(membershipConfig)
	if err != nil {
		panic(err)
	}

	// signal handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// all use the same root ctx
	ServerStart(ctx, membershipConfig.CurrentPort)
	raft.StartRaftNode(ctx)

	sig := <-signalChan
	logger.Warn("Received signal", zap.String("signal", sig.String()))
	cancel()
	time.Sleep(10 * time.Second)
	logger.Warn("Main exiting")
}
