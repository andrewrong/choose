package core

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type LockInfo struct {
	LockName string `json:"lock_name" bson:"lock_name"` //锁的名字
	OwnerId  string `json:"owner_id" bson:"owner_id"`   //锁的owner_id
	LeaseMs  int64  `json:"lease_ms" bson:"lease_ms"`   // 过期事件
}

func (l *LockInfo) Check() error {
	if l.LockName == "" || l.OwnerId == "" || l.LeaseMs <= 0 {
		return LockInfoParamsError
	}
	return nil
}

type Action struct {
	fn func()
}

type expireAction struct {
	Action
	isExec bool //是否执行过
}

func (e *expireAction) reset() {
	e.isExec = false
}

func (e *expireAction) exec() {
	if e.isExec || e.fn == nil {
		return
	}

	e.isExec = true
	go func() {
		e.fn()
	}()
}

type ScopeLock func() error

// dLock理论上应该是独一无二的
type dLock struct {
	service *DistributedLockService
	mutex   sync.RWMutex

	lockName       string       //锁的名字
	leaseMs        int64        //锁的租期
	lockTime       int64        //获得锁的事件,单位ns
	autoRenewLease atomic.Value //是否续约

	expireAction *expireAction //锁过期的时候被调用的action
	lockAction   *Action       //锁被lock之后需要调用的action

	logger *log.Logger
}

/**
* Lock的行为:
* 1. lock没有被占用，那么正常的进行lock服务
* 2. lock被占用，那么阻塞等待;
 */
func (d *dLock) lock() bool {
	res := false
	alreadyLock := false
	for {
		alreadyLock, res = func() (remoteLock, thisRes bool) {
			d.mutex.Lock()
			defer d.mutex.Unlock()
			if d.IsLock() {
				return true, true
			}

			info := &LockInfo{
				LockName: d.lockName,
				LeaseMs:  d.leaseMs,
			}
			lockRes, lockTime := d.service.lock(info)

			if lockRes {
				atomic.StoreInt64(&d.lockTime, lockTime)
				return false, true
			}
			return false, false
		}()

		if alreadyLock {
			d.logger.Printf("lock_name:%s already locked", d.lockName)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	return res
}

func (d *dLock) TryLock(timeout time.Duration) bool {
	//TODO
	return false
}

/**
* unlock行为:
* 1. 未lock调用Unlock，出现fatal
 */
func (d *dLock) unlock() bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	//判断是有意义的，如果这边判断它已经过期了，那么远程也会过期了;其他的可以被抢占了
	if !d.IsLock() {
		d.logger.Printf("unlock lock_name:%s already unlocked", d.lockName)
		return true
	}

	info := &LockInfo{
		LockName: d.lockName,
		LeaseMs:  d.leaseMs,
	}

	res := d.service.unlock(info)
	if res {
		atomic.StoreInt64(&d.lockTime, 0)
		return true
	}
	return false
}

func (d *dLock) IsLock() bool {
	return (atomic.LoadInt64(&d.lockTime) + d.leaseMs*1000000) <= time.Now().UnixNano()
}

func (d *dLock) isAutoRenewLease() bool {
	return d.autoRenewLease.Load().(bool)
}

func (d *dLock) renewLease() bool {
	if !d.isAutoRenewLease() {
		return true
	}

	//如果已经过期，不会进行续约
	if !d.IsLock() {
		return true
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	info := &LockInfo{
		LockName: d.lockName,
		LeaseMs:  d.leaseMs,
	}
	lockRes, lockTime := d.service.renew(info)
	if lockRes {
		atomic.StoreInt64(&d.lockTime, lockTime)
		return true
	}

	d.logger.Printf("lock_name:%s renew is error", d.lockName)
	return false
}

// 被动触发锁过期
func (d *dLock) expireLock() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.expireAction.exec()
}
