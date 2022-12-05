package core

import (
	"time"
)

type Voter interface {
	// 指定一次心跳;
	Heartbeat() (bool, int64)            //发送一次心跳请求
	GetMasterInfo() (*NodeStatus, error) //获得master的信息
	//执行一次选举
	ElectMaster(master string) (*NodeStatus, error)
	//获得voter的唯一标识
	GetUuid() string
	//voter config
	GetVoterTimeConf() *VoterTimeConfig
}

type MasterTask interface {
	Start() //开启这个任务
	Stop()  //关闭这个任务
}

type EventType string

var (
	VOTER_HB           EventType = "VOTER_HB"           //心跳事件，出现错误会出现
	VOTER_CHECK_MASTER EventType = "VOTER_CHECK_MASTER" //主超时
	VOTER_GET_MASTER   EventType = "VOTER_GET_MASTER"   //获得master的结果
	VOTER_ELECT_MASTER EventType = "VOTER_ELECT_MASTER" //选举master事件
	VOTER_MASTER_TASK  EventType = "VOTER_MASTER_TASK"  //运行master以后的特定任务
)

type Event struct {
	Type EventType
	Res  bool
	Data interface{}
}

func NewEvent(t EventType, res bool, data interface{}) *Event {
	return &Event{
		Type: t,
		Res:  res,
		Data: data,
	}
}

type LockBackend interface {
	// 去抢占锁，
	Preemption(lock *LockInfo, timeoutMs time.Duration) (bool, int64, error)
	// 去更新租期，
	UpdateLease(lock *LockInfo, timeoutMs time.Duration) (bool, int64, error)
	// 释放lock
	Release(lock *LockInfo, timeoutMs time.Duration) (bool, error)
	// 查询Lock信息
	QueryLockInfo(lock *LockInfo, timeoutMs time.Duration) (*LockInfo, error)
	//关闭backend
	Close() error
}
