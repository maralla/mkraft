package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maki3cat/mkraft/raft"
	"github.com/maki3cat/mkraft/util"
	"gopkg.in/yaml.v2"
)

var logger = util.GetSugarLogger()

func main() {

	// read config from the yaml file
	defaultPath := "./config/config1.yaml"
	configPath := flag.String("c", "", "the path of the config file")
	if *configPath == "" {
		*configPath = defaultPath
	}
	flag.Parse()
	logger.Info("config file path: ", *configPath)
	membershipConfig := &raft.Membership{}
	yamlFile, err := os.ReadFile(*configPath)
	if err != nil {
		logger.Fatalf("yamlFile.Get err  #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, membershipConfig)
	if err != nil {
		logger.Fatalf("Unmarshal: %v", err)
	}
	logger.Infof("Config: %v", membershipConfig)

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
	logger.Warn("\nReceived signal: %s\n", sig)
	cancel()
	time.Sleep(10 * time.Second)
	logger.Warn("Main exiting")
}
