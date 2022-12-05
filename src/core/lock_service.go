package core

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
)

// 分布式锁
type DistributedLockService struct {
	cfg          *LockManagerConfig
	log          *log.Logger
	currentLocks cmap.ConcurrentMap[string, *dLock]

	stop chan struct{}
	wg   sync.WaitGroup
}

type LockManagerConfig struct {
	backend          LockBackend
	ownerId          string
	checkExpiredFreq time.Duration
	checkRenewFreq   time.Duration
	backendTimeout   time.Duration
	retryInterval    time.Duration
	leaseMs          time.Duration //租期
	maxRetry         int
}

func (l *LockManagerConfig) Check() error {
	if l.backend == nil {
		return errors.New("backend is nil")
	}

	if l.ownerId == "" {
		return errors.New("ownerId is empty")
	}

	if l.checkExpiredFreq.Milliseconds() < DEFAULT_CHECK_EXPIRED_MS {
		l.checkExpiredFreq = time.Duration(DEFAULT_CHECK_EXPIRED_MS) * time.Millisecond
	}

	if l.checkRenewFreq.Milliseconds() <= 0 {
		l.checkRenewFreq = time.Duration(DEFAULT_RENEW_MS) * time.Millisecond
	}

	if l.backendTimeout.Milliseconds() <= 0 {
		l.backendTimeout = time.Duration(DEFAULT_BACKEND_TIMEOUT_MS) * time.Millisecond
	}

	if l.retryInterval.Milliseconds() <= DEFAULT_RETRY_INTERVAL_MS {
		l.retryInterval = time.Duration(DEFAULT_RETRY_INTERVAL_MS) * time.Millisecond
	}

	if l.leaseMs.Milliseconds() <= 0 {
		l.leaseMs = time.Duration(DEFAULT_LEASE_MS) * time.Millisecond
	}

	if l.leaseMs < l.checkRenewFreq || l.leaseMs < l.checkExpiredFreq {
		return errors.New("lease is too small")
	}

	if l.maxRetry <= 0 {
		l.maxRetry = DEFAULT_MAX_RETRY
	}

	if l.checkRenewFreq < l.backendTimeout*2 {
		return errors.New("renew freq is too small, it must be greater than 2 * backendTimeout")
	}

	return nil
}

func NewDistributedLockService(cfg *LockManagerConfig, logger *log.Logger) (*DistributedLockService, error) {
	if cfg == nil {
		return nil, ParamError
	}

	err := cfg.Check()
	if err != nil {
		return nil, err
	}

	if logger == nil {
		logger = log.Default()
	}

	tmpCfg := *cfg
	tmp := &DistributedLockService{
		cfg:  &tmpCfg,
		log:  logger,
		stop: make(chan struct{}),
		wg:   sync.WaitGroup{},
	}

	return tmp, nil
}

func (d *DistributedLockService) Start() {
	d.log.Printf("DistributedLockService is starting")
	d.renewLease()
	d.checkLockExpired()
}

func (d *DistributedLockService) Stop() {
	d.stop <- struct{}{}
	d.stop <- struct{}{}
	d.cfg.backend.Close()

	d.wg.Wait()
	d.log.Printf("DistributedLockService is stopped")
}

func (d *DistributedLockService) renewLease() {
	d.log.Printf("renew task is running")
	go func() {
		d.wg.Add(1)
		defer func() {
			d.wg.Done()
		}()

		ticker := time.NewTicker(d.cfg.checkRenewFreq)
		defer func() {
			ticker.Stop()
		}()

		for {
			select {
			case <-ticker.C:
				{
					tmp := d.currentLocks.Items()
					for k, v := range tmp {
						res := v.renewLease()
						d.log.Printf("lock_name:%s renew lease result:%v", k, res)
					}
				}
			case <-d.stop:
				{
					d.log.Printf("renew task is stopped")
					return
				}
			}
		}
	}()
}

func (d *DistributedLockService) checkLockExpired() {
	d.log.Printf("check mutex expire task is running")
	go func() {
		d.wg.Add(1)
		defer func() {
			d.wg.Done()
		}()

		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
		ticker := time.NewTicker(d.cfg.checkExpiredFreq)
		defer func() {
			ticker.Stop()
		}()

		for {
			select {
			case <-ticker.C:
				{
					items := d.currentLocks.Items()

					for k, v := range items {
						res := v.IsLock()
						d.log.Printf("lock_name:%s isExpired result:%v", k, res)
					}
				}
			case <-d.stop:
				{
					d.log.Printf("check mutex expire task is stopped")
					return
				}
			}
		}
	}()
}

func (d *DistributedLockService) NewScopeLock(lockName string, autoRenewLease bool, lockAction, eAction func()) (ScopeLock, error) {
	if lockName == "" {
		d.log.Printf("lockName cannot be empty and lease cannot be negative")
		return nil, ParamError
	}

	if lockAction == nil {
		d.log.Printf("lockAction cannot be empty")
		return nil, ParamError
	}

	scopeLock := ScopeLock(func() error {
		//构造内部的dLock
		tmpLock, err := func() (*dLock, error) {
			_, isExisted := d.currentLocks.Get(lockName)
			if isExisted {
				d.log.Printf("lockName: %s already exists", lockName)
				return nil, LockExistError
			}

			tmp := &dLock{
				service: d,

				lockName:       lockName,
				leaseMs:        d.cfg.leaseMs.Milliseconds(),
				autoRenewLease: atomic.Value{},
				expireAction: &expireAction{
					Action: Action{
						fn: eAction,
					},
					isExec: false,
				},
				lockAction: &Action{
					fn: lockAction,
				},
				lockTime: 0,

				mutex:  sync.RWMutex{},
				logger: d.log,
			}
			tmp.autoRenewLease.Store(autoRenewLease)

			return tmp, nil
		}()

		if err != nil {
			return err
		}

		d.currentLocks.Set(lockName, tmpLock)
		//执行完毕之后清理这个lockName
		defer d.currentLocks.Remove(lockName)

		//开始执行lock操作,
		lockRes := tmpLock.lock()
		if lockRes {
			defer tmpLock.unlock()
			tmpLock.lockAction.fn()
			return nil
		}
		return LockError
	})

	return scopeLock, nil
}

func (d *DistributedLockService) lock(info *LockInfo) (bool, int64) {
	if info == nil {
		return false, 0
	}

	info.OwnerId = d.cfg.ownerId
	lockTime := int64(0)
	finalRes := Retry(d.log, d.cfg.retryInterval, d.cfg.backendTimeout, d.cfg.maxRetry, func() (bool, error) {
		res := false
		var err error = nil
		res, lockTime, err = d.cfg.backend.Preemption(info, d.cfg.backendTimeout)
		return res, err
	})

	return finalRes, lockTime
}

func (d *DistributedLockService) unlock(info *LockInfo) bool {
	if info == nil {
		return false
	}

	info.OwnerId = d.cfg.ownerId
	return Retry(d.log, d.cfg.retryInterval, d.cfg.backendTimeout, d.cfg.maxRetry, func() (bool, error) {
		return d.cfg.backend.Release(info, d.cfg.backendTimeout)
	})
}

func (d *DistributedLockService) renew(info *LockInfo) (bool, int64) {
	if info == nil {
		return false, 0
	}
	info.OwnerId = d.cfg.ownerId

	lockTime := int64(0)
	finalRes := Retry(d.log, d.cfg.retryInterval, d.cfg.backendTimeout, d.cfg.maxRetry, func() (bool, error) {
		res := false
		var err error = nil
		res, lockTime, err = d.cfg.backend.UpdateLease(info, d.cfg.backendTimeout)

		return res, err
	})

	return finalRes, lockTime
}

func (d *DistributedLockService) queryLockInfo(info *LockInfo) *LockInfo {
	if info == nil {
		return nil
	}

	info.OwnerId = d.cfg.ownerId
	var resInfo *LockInfo = nil
	var err error = nil
	_ = Retry(d.log, d.cfg.retryInterval, d.cfg.backendTimeout, d.cfg.maxRetry, func() (bool, error) {
		resInfo, err = d.cfg.backend.QueryLockInfo(info, d.cfg.backendTimeout)
		return true, err
	})
	if err != nil {
		return nil
	}
	return resInfo
}
