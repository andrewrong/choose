package core

import (
	"errors"
	"log"
	"sync/atomic"
	"time"
)

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

func (n *NodeStatus) UpdateTime(now int64) {
	atomic.StoreInt64(&n.LatestHeartbeatTime, now)
}

func (n *NodeStatus) IsHealthy(diff int64) bool {
	if time.Now().UnixMilli()-n.LatestHeartbeatTime >= diff {
		return false
	}
	return true
}

type VoterTimeConfig struct {
	HBFreq             time.Duration //心跳频率
	HBTimeout          time.Duration //心跳超时
	CheckMasterFreq    time.Duration //检查master是否健康频率
	CheckMasterTimeout time.Duration //检查master超时
	ElectMasterTimeout time.Duration //选举超时
}

func (v *VoterTimeConfig) Check() error {
	if v.HBFreq < 0 {
		log.Printf("voter heartbeat frequency must greater than 0")
		return errors.New("voter heartbeat frequency must greater than 0")
	}

	if v.HBTimeout < 0 {
		log.Printf("voter heartbeat timeout must greater than 0")
		return errors.New("voter heartbeat timeout must greater than 0")
	}

	if v.CheckMasterFreq < 0 {
		log.Printf("voter check master frequency must greater than 0")
		return errors.New("voter check master frequency must greater than 0")
	}

	if v.CheckMasterTimeout < 0 {
		log.Printf("voter check master timeout must greater than 0")
		return errors.New("voter check master timeout must greater than 0")
	}

	if v.ElectMasterTimeout < 0 {
		log.Printf("voter elect master timeout must greater than 0")
		return errors.New("voter check master timeout must greater than 0")
	}

	return nil
}

var ParamError = errors.New("param is invalid")    //参数无效.
var LockExistError = errors.New("lock is existed") //参数无效.
var LockError = errors.New("lock is failure")      //参数无效.

var LockInfoParamsError = errors.New("lock info is invalid") //
var ServiceTimeoutError = errors.New("service timeout")

// ////////////////////////////////////////////////////////////////////////////
var (
	DEFAULT_LEASE_MS           int64 = 1000 //默认租期是1秒
	DEFAULT_BACKEND_TIMEOUT_MS int64 = 200  //默认后端执行的时间
	DEFAULT_CHECK_EXPIRED_MS   int64 = 100  //检查lock过期的时间
	DEFAULT_RENEW_MS           int64 = 500  //renew lock时间
	DEFAULT_RETRY_INTERVAL_MS  int64 = 100

	DEFAULT_MAX_RETRY int = 3
)
