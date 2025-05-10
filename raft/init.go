package raft

import "github.com/maki3cat/mkraft/common"

var logger = common.GetLogger()
var consensus ConsensusIface = &ConsensusImpl{}
