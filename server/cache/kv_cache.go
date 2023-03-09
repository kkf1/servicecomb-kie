package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/servicecomb-kie/pkg/model"
	"github.com/apache/servicecomb-kie/pkg/stringutil"
	"github.com/go-chassis/foundation/backoff"
	"github.com/go-chassis/openlog"
	"github.com/little-cui/etcdadpt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"strings"
	"sync"
	"time"
)

func Init() {
	Kc = NewKvCache(PrefixKvs, 0, &sync.Map{})
	go Kc.refresh(context.Background())
}

var Kc *KvCache

const (
	PrefixKvs = "kvs"
)

type KvCache struct {
	Client   etcdadpt.Client
	Prefix   string
	Revision int64
	Cache    *sync.Map
}

func NewKvCache(prefix string, rev int64, cache *sync.Map) *KvCache {
	return &KvCache{
		Client:   etcdadpt.Instance(),
		Prefix:   prefix,
		Revision: rev,
		Cache:    cache,
	}
}

func (kc *KvCache) refresh(ctx context.Context) {
	openlog.Info("start to list and watch")
	retries := 0

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		nextPeriod := time.Second
		if err := kc.listWatch(ctx); err != nil {
			retries++
			nextPeriod = backoff.GetBackoff().Delay(retries)
		} else {
			retries = 0
		}

		select {
		case <-ctx.Done():
			openlog.Info("stop to list and watch")
			return
		case <-timer.C:
			timer.Reset(nextPeriod)
		}
	}
}

func (kc *KvCache) listWatch(ctx context.Context) error {
	rsp, err := kc.Client.Do(ctx, etcdadpt.WatchPrefixOpOptions(kc.Prefix)...)
	if err != nil {
		openlog.Error(fmt.Sprintf("list prefix %s failed, current rev: %d, err, %v", kc.Prefix, kc.Revision, err))
		return err
	}
	kc.Revision = rsp.Revision

	kc.cachePut(rsp)

	rev := kc.Revision
	opts := append(
		etcdadpt.WatchPrefixOpOptions(kc.Prefix),
		etcdadpt.WithRev(kc.Revision+1),
		etcdadpt.WithWatchCallback(kc.watchCallBack),
	)
	err = kc.Client.Watch(ctx, opts...)
	if err != nil {
		openlog.Error(fmt.Sprintf("watch prefix %s failed, start rev: %d+1->%d->0, err %v", kc.Prefix, rev, kc.Revision, err))
		kc.Revision = 0
	}
	return err
}

func (kc *KvCache) watchCallBack(message string, rsp *etcdadpt.Response) error {
	if rsp == nil || len(rsp.Kvs) == 0 {
		return fmt.Errorf("unknown event")
	}
	kc.Revision = rsp.Revision

	switch rsp.Action {
	case etcdadpt.ActionPut:
		kc.cachePut(rsp)
	case etcdadpt.ActionDelete:
		kc.cacheDelete(rsp)
	default:
		openlog.Warn(fmt.Sprintf("unrecognized action::%v", rsp.Action))
	}
	return nil
}

func (kc *KvCache) cachePut(rsp *etcdadpt.Response) {
	for _, kv := range rsp.Kvs {
		kvDoc, inputKey, err := getInputKey(kv)
		if err != nil {
			continue
		}
		if _, ok := kc.Cache.Load(inputKey); !ok {
			kc.Cache.Store(inputKey, map[string]struct{}{})
		}
		val, _ := kc.Cache.Load(inputKey)
		m, _ := val.(map[string]struct{})
		m[kvDoc.ID] = struct{}{}
	}
}

func (kc *KvCache) cacheDelete(rsp *etcdadpt.Response) {
	for _, kv := range rsp.Kvs {
		kvDoc, inputKey, err := getInputKey(kv)
		if err != nil {
			continue
		}
		if _, ok := kc.Cache.Load(inputKey); !ok {
			continue
		}
		val, _ := kc.Cache.Load(inputKey)
		m, _ := val.(map[string]struct{})
		delete(m, kvDoc.ID)
	}
}

func getInputKey(kv *mvccpb.KeyValue) (*model.KVDoc, string, error) {
	kvDoc := &model.KVDoc{}
	err := json.Unmarshal(kv.Value, kvDoc)
	if err != nil {
		openlog.Error(fmt.Sprintf("failed to unmarshal kv, err %v", err))
		return nil, "", err
	}
	labelFormat := stringutil.FormatMap(kvDoc.Labels)
	inputKey := strings.Join([]string{
		"",
		kvDoc.Domain,
		kvDoc.Project,
		labelFormat,
	}, "/")
	return kvDoc, inputKey, nil
}
