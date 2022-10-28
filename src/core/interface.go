package core

import "time"

type Voter interface {
	// 指定一次心跳;
	Heartbeat() (bool, int64)            //发送一次心跳请求
	GetHBFreq() int                      //获得心跳频率
	GetHBTimeout() time.Duration         //发送心跳超时时间
	GetMasterInfo() (*NodeStatus, error) //获得master的信息

	//检查master是否健康
	GetCheckMasterFreq() int              //活动检查master的频率
	GetCheckMasterTimeout() time.Duration //获得checkMaster的超时时间

	//执行一次选举
	ElectMaster(master string) (*NodeStatus, error)
	GetElectMasterTimeout() time.Duration //选举超时时间

	//获得voter的唯一标识
	GetUuid() string
}

type MasterTask interface {
	Start() //开启这个任务
	Stop()  //关闭这个任务
}

type EventType string

var (
	HB                   EventType = "HB"
	CHECK_MASTER_FAILURE EventType = "CHECK_MASTER_FAILURE"
	CHECK_MASTER_SUCCESS EventType = "CHECK_MASTER_SUCCESS"
	GET_MASTER_FAILURE   EventType = "GET_MASTER_FAILURE"
	GET_MASTER_SUCCESS   EventType = "GET_MASTER_SUCCESS"
	ELECT_MASTER_FAILURE EventType = "ELECT_MASTER_FAILURE"
	ELECT_MASTER_SUCCESS EventType = "ELECT_MASTER_SUCCESS"
	RUN_MASTER_TASK      EventType = "RUN_MASTER_TASK" //运行master以后的特定任务
)

type Event struct {
	Type EventType
	Data interface{}
}

func NewEvent(t EventType, data interface{}) *Event {
	return &Event{
		Type: t,
		Data: data,
	}
}
