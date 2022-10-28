package core

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"
)

func (n *NodeStatus) UpdateTime(now int64) {
	atomic.StoreInt64(&n.LatestHeartbeatTime, now)
}

func (n *NodeStatus) IsHealthy(diff int64) bool {
	if time.Now().UnixMilli()-n.LatestHeartbeatTime >= diff {
		return false
	}
	return true
}

type VoterNode struct {
	voter          Voter                 //自定义选举功能
	masterTasks    map[string]MasterTask //如果是master就必须要执行的任务
	eventQueue     chan *Event           //事件队列，用来获得整个投票过程中的一些事件；大部分可以任务是打印日志什么的
	consumeEventFc func(e *Event)        //消费event的函数，默认为空函数
	log            *log.Logger
	stop           chan struct{}

	selfNodeStatus   *NodeStatus //自我节点状态管理
	masterNodeStatus *NodeStatus //当前master的节点
	status           runStatus
}

func checkVoter(voter Voter, logger *log.Logger) error {
	if voter.GetHBFreq() < 0 {
		logger.Printf("voter heartbeat frequency must greater than 0")
		return errors.New("voter heartbeat frequency must greater than 0")
	}

	if voter.GetHBTimeout() < 0 {
		logger.Printf("voter heartbeat timeout must greater than 0")
		return errors.New("voter heartbeat timeout must greater than 0")
	}

	if voter.GetCheckMasterFreq() < 0 {
		logger.Printf("voter check master frequency must greater than 0")
		return errors.New("voter check master frequency must greater than 0")
	}

	if voter.GetCheckMasterTimeout() < 0 {
		logger.Printf("voter check master timeout must greater than 0")
		return errors.New("voter check master timeout must greater than 0")
	}

	if voter.GetElectMasterTimeout() < 0 {
		logger.Printf("voter elect master timeout must greater than 0")
		return errors.New("voter check master timeout must greater than 0")
	}

	if voter.GetUuid() == "" {
		logger.Printf("voter Uuid is empty")
		return errors.New("voter Uuid is empty")
	}
	return nil
}

func NewVoterNode(voter Voter, tasks map[string]MasterTask, eventQueueLen int, csEventFc func(e *Event), logger *log.Logger) (*VoterNode, error) {
	if logger == nil {
		logger = log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Llongfile)
	}

	if voter == nil {
		logger.Printf("voter is nil")
		return nil, ParamError
	}

	err := checkVoter(voter, logger)
	if err != nil {
		return nil, err
	}

	if eventQueueLen < 1000 {
		eventQueueLen = 1000
	}

	tmp := VoterNode{}
	tmp.log = logger
	tmp.voter = voter
	tmp.masterTasks = tasks
	tmp.eventQueue = make(chan *Event, eventQueueLen)
	tmp.consumeEventFc = csEventFc
	tmp.selfNodeStatus = &NodeStatus{
		Uuid:                voter.GetUuid(),
		LatestHeartbeatTime: time.Now().UnixMilli(),
	}
	tmp.status = STOP

	masterInfo, err := voter.GetMasterInfo()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("get master is error: %v", err.Error()))
	}
	if masterInfo == nil {
		masterInfo = &NodeStatus{}
	}
	tmp.masterNodeStatus = masterInfo

	return &tmp, nil
}

func (v *VoterNode) Start() {
	if atomic.CompareAndSwapInt64(&v.status, STOP, START) {
		v.heartbeat()
		v.checkMaster()
	} else {
		v.log.Printf("this node has already started")
	}
}

func (v *VoterNode) Stop() {
	if atomic.CompareAndSwapInt64(&v.status, START, STOP) {
		v.stop <- struct{}{}
		v.stop <- struct{}{}
		v.stop <- struct{}{}

		for k, task := range v.masterTasks {
			task.Stop()
			v.putEvent(NewEvent(RUN_MASTER_TASK, fmt.Sprintf("task:%s is stopped, because voterNode is stopped", k)))
		}
		close(v.eventQueue)
	}
}

// 当前node是否健康
func (v *VoterNode) IsHealthy() bool {
	return v.selfNodeStatus.IsHealthy(int64(v.voter.GetCheckMasterFreq()))
}

// 当前节点的心跳时间
func (v *VoterNode) GetHBTime() int64 {
	return v.selfNodeStatus.LatestHeartbeatTime
}

// 当前节点是否是master
func (v *VoterNode) IsMaster() bool {
	return v.selfNodeStatus.Uuid == v.masterNodeStatus.Uuid
}

// 设置事件的回调函数
func (v *VoterNode) SetConsumeEventFc(consumeEventFc func(e *Event)) {
	if consumeEventFc != nil {
		v.consumeEventFc = consumeEventFc
	}
}

func (v *VoterNode) putEvent(e *Event) {
	if e == nil {
		return
	}
	select {
	case v.eventQueue <- e:
		{
		}
	default:
		{
			v.log.Printf("event queue is full")
		}
	}
}

func (v *VoterNode) heartbeat() {
	v.log.Printf("heartbeat task is running")

	go func() {
		ticker := time.NewTicker(time.Duration(v.voter.GetHBFreq()) * time.Millisecond)
		defer func() {
			ticker.Stop()
		}()

		for {
			select {
			case <-ticker.C:
				{
					res, t := v.voter.Heartbeat()
					if !res {
						v.putEvent(NewEvent(HB, "heartbeat is error"))
					} else {
						v.selfNodeStatus.UpdateTime(t)
					}
				}
			case <-v.stop:
				{
					v.log.Printf("heartbeat receive stop signal")
					return
				}
			}
		}
	}()
}

func (v *VoterNode) checkMaster() {
	v.log.Printf("check master task is running")

	go func() {
		ticker := time.NewTicker(time.Duration(v.voter.GetCheckMasterFreq()) * time.Millisecond)
		defer func() {
			ticker.Stop()
		}()

		for {
			select {
			case <-ticker.C:
				{
					masterInfo := &NodeStatus{}
					info, err := v.voter.GetMasterInfo()
					if err != nil {
						v.putEvent(NewEvent(GET_MASTER_FAILURE, err.Error()))
						if v.masterNodeStatus.Check() {
							masterInfo = v.masterNodeStatus
						}
					} else {
						masterInfo = info
					}

					if masterInfo.Check() {
						if masterInfo.IsHealthy(int64(v.voter.GetCheckMasterFreq())) {
							v.putEvent(NewEvent(CHECK_MASTER_SUCCESS, "check master is success"))
							v.masterNodeStatus = masterInfo
							continue
						}

						v.putEvent(NewEvent(CHECK_MASTER_FAILURE, "check master is error"))
						//如果检测check失败的话就需要进行选举
						if !v.selfNodeStatus.IsHealthy(int64(v.voter.GetCheckMasterFreq())) {
							continue
						}
					}
					v.oneElectMaster()
				}
			case <-v.stop:
				{
					v.log.Printf("check master task receive stop signal")
					return
				}
			}
		}
	}()
}

// 执行一次选举
func (v *VoterNode) oneElectMaster() {
	res, err := v.voter.ElectMaster(v.masterNodeStatus.Uuid)

	defer func() {
		if err == nil {
			v.masterNodeStatus = res
		} else {
			v.masterNodeStatus = &NodeStatus{}
		}
	}()

	if err != nil {
		v.putEvent(NewEvent(ELECT_MASTER_FAILURE, err.Error()))
	} else {
		v.putEvent(NewEvent(ELECT_MASTER_SUCCESS, res))

		if res.Uuid == v.selfNodeStatus.Uuid {
			for key, task := range v.masterTasks {
				v.putEvent(NewEvent(RUN_MASTER_TASK, fmt.Sprintf("task:%s is running", key)))
				task.Start()
			}
			return
		}

		for key, task := range v.masterTasks {
			v.putEvent(NewEvent(RUN_MASTER_TASK, fmt.Sprintf("task:%s is stop", key)))
			task.Stop()
		}
	}
}

func (v *VoterNode) consumeEvent() {
	v.log.Printf("consume event is running")
	go func() {
		for {
			select {
			case event := <-v.eventQueue:
				{
					if event == nil || v.consumeEventFc == nil {
						continue
					}
					v.consumeEventFc(event)
				}
			case <-v.stop:
				{
					v.log.Printf("check master task receive stop signal")
					return
				}
			}
		}
	}()
}
