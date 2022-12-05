package plugins

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"

	"choose/src/core"
)

/**
 *大概实现思路:
 * mongo中会有两张表:
 * 1. heartbeat: 心跳表;用来更新各个节点心跳;
 * 2. master: 用来抢占节点使用；如果需要进行一次选举，就原子的写入数据，如果成功了就表示抢占成功
 */

// TODO session出现错误之后需要重新刷新;

type MongoVoterImpl struct {
	host      string
	db        string
	hbC       string
	masterC   string
	uuid      string
	session   *mgo.Session
	voterConf *core.VoterTimeConfig //选举需要使用到的超时配置
}

func NewMongoVoterImpl(host, db, hbC, masterC, uuid string, voterConf *core.VoterTimeConfig) (core.Voter, error) {
	if db == "" || hbC == "" || masterC == "" || uuid == "" || host == "" || voterConf == nil {
		return nil, core.ParamError
	}

	if err := voterConf.Check(); err != nil {
		return nil, err
	}

	tmp := &MongoVoterImpl{
		host:      host,
		db:        db,
		masterC:   masterC,
		hbC:       hbC,
		uuid:      uuid,
		voterConf: voterConf,
	}

	sess, err := mgo.Dial(host)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("mongo host init is error, err:%s", err.Error()))
	}
	tmp.session = sess
	return tmp, nil
}

func (m *MongoVoterImpl) Heartbeat() (bool, int64) {
	now := time.Now().UnixMilli()
	_, err := m.session.DB(m.db).C(m.hbC).Upsert(bson.M{"_id": m.uuid}, bson.M{
		"_id":        m.uuid,
		"updateTime": now,
	})

	if err != nil {
		log.Printf("update heartbeat time is error: %s", err.Error())
		return false, 0
	}
	return true, now
}

func (m *MongoVoterImpl) GetVoterTimeConf() *core.VoterTimeConfig {
	return m.voterConf
}

func (m *MongoVoterImpl) GetMasterInfo() (*core.NodeStatus, error) {
	item := &core.NodeStatus{}
	err := m.session.DB(m.db).C(m.masterC).Find(bson.M{"_id": "master"}).One(item)
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (m *MongoVoterImpl) ElectMaster(master string) (*core.NodeStatus, error) {
	now := time.Now().UnixMilli()

	filter := bson.M{"_id": "master"}
	if master != "" {
		filter["master"] = master
	}

	info, err := m.session.DB(m.db).C(m.hbC).Upsert(filter, bson.M{
		"_id":        "master",
		"updateTime": now,
		"master":     m.uuid,
	})

	if err != nil {
		return nil, err
	}

	if info.Updated > 0 {
		log.Printf("master from %s ==> to %s", master, m.uuid)
		return &core.NodeStatus{
			Uuid:                m.uuid,
			LatestHeartbeatTime: now,
		}, nil
	}
	return m.GetMasterInfo()
}

func (m *MongoVoterImpl) GetUuid() string {
	return m.uuid
}
