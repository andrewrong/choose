package plugins

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/groupcache/singleflight"

	"choose/src/core"
)

var defaultSocketTimeout = time.Duration(1) * time.Second

type MongoBackend struct {
	host          string
	db            string
	coll          string
	socketTimeout time.Duration
	session       *mgo.Session
	group         *singleflight.Group
}

type mongoElement struct {
	Key        string `json:"_id" bson:"_id"`               //lock name
	OwnerId    string `json:"ownerId" bson:"ownerId"`       // owner
	ExpireAt   int64  `json:"expireAt" bson:"expireAt"`     // 过期时间
	UpdateTime int64  `json:"updateTime" bson:"updateTime"` // lock被更新的时间
}

func lockInfoToMongoElement(info *core.LockInfo) *mongoElement {
	tmp := &mongoElement{
		Key:     info.LockName,
		OwnerId: info.OwnerId,
	}

	return tmp
}

func NewMongoBackend(host, db, coll string, socketTimeoutMs time.Duration) (core.LockBackend, error) {
	if db == "" || coll == "" || host == "" {
		return nil, core.ParamError
	}

	if socketTimeoutMs.Milliseconds() == 0 {
		socketTimeoutMs = defaultSocketTimeout
	}

	tmp := &MongoBackend{
		host:          host,
		db:            db,
		coll:          coll,
		socketTimeout: socketTimeoutMs,
		group:         &singleflight.Group{},
	}

	sess, err := mgo.Dial(host)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("mongo host init is error, err:%s", err.Error()))
	}
	sess.SetSocketTimeout(tmp.socketTimeout)
	tmp.session = sess

	return tmp, nil
}

func (m *MongoBackend) Preemption(lock *core.LockInfo, timeoutMs time.Duration) (bool, int64, error) {
	if lock == nil {
		return false, 0, core.LockInfoParamsError
	}

	if err := lock.Check(); err != nil {
		return false, 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutMs)
	defer cancel()

	now := time.Now()

	var resError error = nil
	res := false
	lockTime := int64(0)

	completed := make(chan struct{})
	defer close(completed)

	go func() {
		defer func() {
			completed <- struct{}{}
		}()

		elem := lockInfoToMongoElement(lock)
		elem.ExpireAt = now.UnixNano() + lock.LeaseMs*1000000
		elem.UpdateTime = now.UnixMilli()

		selectQuery := bson.M{
			"_id": elem.Key,
			"expireAt": bson.M{
				"$lte": now.UnixNano() - lock.LeaseMs*1000000,
			},
		}
		changeInfo, err := m.session.DB(m.db).C(m.coll).Upsert(selectQuery, elem)
		if err != nil {
			if mgo.IsDup(err) {
				return
			}

			go func() {
				m.group.Do("refresh", func() (interface{}, error) {
					m.session.Refresh()
					return nil, nil
				})
			}()
			log.Printf("upsert error: %v", err)
			resError = err
			return
		}
		if changeInfo.Updated == 1 {
			res = true
			lockTime = now.UnixNano()
			return
		}
		return
	}()

	select {
	case <-ctx.Done():
		{
			log.Printf("preemption is timeout")
			return false, 0, core.ServiceTimeoutError
		}
	case <-completed:
		{
			return res, lockTime, resError
		}
	}
}

func (m *MongoBackend) UpdateLease(lock *core.LockInfo, timeoutMs time.Duration) (bool, int64, error) {
	if lock == nil {
		return false, 0, core.LockInfoParamsError
	}

	if err := lock.Check(); err != nil {
		return false, 0, err
	}

	elem := lockInfoToMongoElement(lock)
	now := time.Now()
	elem.ExpireAt = now.UnixNano() + lock.LeaseMs*1000000
	elem.UpdateTime = now.UnixMilli()

	selectQuery := bson.M{
		"_id":     elem.Key,
		"ownerId": elem.OwnerId,
		"expireAt": bson.M{
			"$gt": now.UnixNano() - lock.LeaseMs*1000000,
		},
	}

	changeInfo, err := m.session.DB(m.db).C(m.coll).Upsert(selectQuery, elem)
	if err != nil {
		if mgo.IsDup(err) {
			return false, 0, nil
		}
		go func() {
			m.group.Do("refresh", func() (interface{}, error) {
				m.session.Refresh()
				return nil, nil
			})
		}()
		log.Printf("updateLease error: %v", err)
		return false, 0, err
	}

	if changeInfo.Updated == 1 {
		return true, now.UnixNano(), nil
	}
	return false, 0, nil

}

func (m *MongoBackend) Release(lock *core.LockInfo, timeoutMs time.Duration) (bool, error) {
	if lock == nil {
		return false, core.LockInfoParamsError
	}

	if err := lock.Check(); err != nil {
		return false, err
	}
	now := time.Now()

	selectQuery := bson.M{
		"_id":     lock.LockName,
		"ownerId": lock.OwnerId,
		"expireAt": bson.M{
			"$gt": now.UnixNano() - lock.LeaseMs*1000000,
		},
	}

	err := m.session.DB(m.db).C(m.coll).Remove(selectQuery)
	if err != nil {
		go func() {
			m.group.Do("refresh", func() (interface{}, error) {
				m.session.Refresh()
				return nil, nil
			})
		}()
		log.Printf("delete error: %v", err)
		return false, err
	}
	return true, nil
}

func (m *MongoBackend) QueryLockInfo(lock *core.LockInfo, timeoutMs time.Duration) (*core.LockInfo, error) {
	if lock == nil {
		return nil, core.LockInfoParamsError
	}

	if err := lock.Check(); err != nil {
		return nil, err
	}

	query := bson.M{
		"_id":     lock.LockName,
		"ownerId": lock.OwnerId,
	}

	info := core.LockInfo{}
	err := m.session.DB(m.db).C(m.coll).Find(query).One(&info)
	if err != nil {
		if strings.Contains(err.Error(), mgo.ErrNotFound.Error()) {
			return nil, nil
		}
		go func() {
			m.group.Do("refresh", func() (interface{}, error) {
				m.session.Refresh()
				return nil, nil
			})
		}()
		log.Printf("query error: %v", err)
		return nil, err
	}
	return &info, nil
}

func (m *MongoBackend) Close() error {
	log.Printf("MongoBackend is close")
	m.session.Close()
	m.session = nil
	return nil
}
