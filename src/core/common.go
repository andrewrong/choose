package core

import "errors"

type NodeStatus struct {
	Uuid                string `bson:"uuid" json:"uuid"`
	LatestHeartbeatTime int64  `bson:"updateTime" json:"updateTime"`
}

func (n *NodeStatus) Check() bool {
	if n.Uuid == "" || n.LatestHeartbeatTime <= 0 {
		return false
	}
	return true
}

var ParamError = errors.New("param is invalid") //参数无效.

type runStatus = int64

const (
	STOP runStatus = iota
	START
)
