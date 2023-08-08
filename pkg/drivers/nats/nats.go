package nats

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/nats/kv"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// TODO: version this data structure to simplify and optimize for size.
type natsData struct {
	// v1 fields
	KV           *server.KeyValue `json:"KV"`
	PrevRevision int64            `json:"PrevRevision"`
	Create       bool             `json:"Create"`
	Delete       bool             `json:"Delete"`

	CreateTime time.Time `json:"-"`
}

func (d *natsData) Encode() ([]byte, error) {
	buf, err := json.Marshal(d)
	return buf, err
}

func (d *natsData) Decode(e nats.KeyValueEntry) error {
	if e == nil || e.Value() == nil {
		return nil
	}

	err := json.Unmarshal(e.Value(), d)
	if err != nil {
		return err
	}
	d.KV.ModRevision = int64(e.Revision())
	d.CreateTime = e.Created()
	return nil
}

var (
	// Ensure Backend implements server.Backend.
	_ server.Backend = (&Backend{})
)

type Backend struct {
	nc *nats.Conn
	js nats.JetStreamContext
	kv *kv.EncodedKV
	l  *logrus.Logger
}

// isExpiredKey checks if the key is expired based on the create time and lease.
func (b *Backend) isExpiredKey(value *natsData) bool {
	if value.KV.Lease == 0 {
		return false
	}

	return time.Now().After(value.CreateTime.Add(time.Second * time.Duration(value.KV.Lease)))
}

// get returns the key-value entry for the given key and revision, if specified.
// This takes into account entries that have been marked as deleted or expired.
func (b *Backend) get(ctx context.Context, key string, revision int64, allowDeletes bool) (int64, *natsData, error) {
	var (
		entry nats.KeyValueEntry
		err   error
	)

	// Get latest revision if not specified.
	if revision <= 0 {
		entry, err = b.kv.Get(key)
	} else {
		entry, err = b.kv.GetRevision(key, uint64(revision))
	}
	if err != nil {
		return 0, nil, err
	}

	rev := int64(entry.Revision())

	var val natsData
	err = val.Decode(entry)
	if err != nil {
		return rev, nil, err
	}

	if val.Delete && !allowDeletes {
		return rev, nil, nats.ErrKeyNotFound
	}

	if b.isExpiredKey(&val) {
		err := b.kv.Delete(val.KV.Key, nats.LastRevision(uint64(rev)))
		if err != nil {
			b.l.Warnf("Failed to delete expired key %s: %v", val.KV.Key, err)
		}
		// Return a zero indicating the key was deleted.
		return 0, nil, nats.ErrKeyNotFound
	}

	return rev, &val, nil
}

// Start starts the backend.
// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
func (b *Backend) Start(ctx context.Context) error {
	if _, err := b.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			b.l.Errorf("Failed to create health check key: %v", err)
		}
	}
	return nil
}

// DbSize get the kineBucket size from JetStream.
func (b *Backend) DbSize(context.Context) (int64, error) {
	return b.kv.BucketSize()
}

// Count returns an exact count of the number of matching keys and the current revision of the database.
func (b *Backend) Count(ctx context.Context, prefix string) (int64, int64, error) {
	keys, err := b.kv.GetKeys(ctx, prefix, false)
	if err != nil {
		return 0, 0, err
	}
	storeRev, err := b.kv.BucketRevision()
	if err != nil {
		return 0, 0, err
	}
	return storeRev, int64(len(keys)), nil
}

// Get returns the store's current revision, the associated server.KeyValue or an error.
func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (int64, *server.KeyValue, error) {
	// Get the kv entry and return the revision.
	rev, nv, err := b.get(ctx, key, revision, false)
	if err != nil {
		return 0, nil, err
	}

	// Attempt to get the latest store revision. If this fails, use the key rev.
	storeRev, err := b.kv.BucketRevision()
	if err == nil && storeRev > rev {
		rev = storeRev
	}

	return rev, nv.KV, nil
}

// Create attempts to create the key-value entry and returns the revision number.
func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	// Check if key exists already. If the entry exists even if marked as expired or deleted,
	// the revision will be returned to apply an update.
	rev, pnv, err := b.get(ctx, key, 0, false)
	if pnv != nil {
		return rev, server.ErrKeyExists
	}
	// If an error other than key not found, return.
	if err != nil && err != nats.ErrKeyNotFound {
		return 0, err
	}

	nv := natsData{
		Delete:       false,
		Create:       true,
		PrevRevision: 0,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: 0,
			ModRevision:    0,
			Value:          value,
			Lease:          lease,
		},
	}

	data, err := nv.Encode()
	if err != nil {
		return 0, err
	}

	// An update with a zero revision will create the key.
	seq, err := b.kv.Create(key, data)
	if err != nil {
		// This may occur if a concurrent writer created the key.
		if jsWrongLastSeqErr.Is(err) {
			b.l.Warnf("create: key=%s, err=%s", key, err)
			return 0, server.ErrKeyExists
		}

		return 0, err
	}

	return int64(seq), nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	// Get the key, allow deletes.
	rev, value, err := b.get(ctx, key, 0, true)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return rev, nil, true, nil
		}
		// TODO: if false, get the current KV.
		// Expected last version is in the header.
		return rev, nil, false, err
	}

	// If deleted
	if value.Delete {
		return rev, value.KV, true, nil
	}

	if revision != 0 && value.KV.ModRevision != revision {
		return rev, value.KV, false, nil
	}

	nv := natsData{
		Delete:       true,
		PrevRevision: rev,
		KV:           value.KV,
	}
	data, err := nv.Encode()
	if err != nil {
		return rev, nil, false, err
	}

	drev, err := b.kv.Update(key, data, uint64(rev))
	if err != nil {
		return rev, value.KV, false, nil
	}

	err = b.kv.Delete(key, nats.LastRevision(drev))
	if err != nil {
		return rev, value.KV, false, nil
	}

	return int64(drev), value.KV, true, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	rev, pjv, err := b.get(ctx, key, 0, false)

	if err != nil {
		if err == nats.ErrKeyNotFound {
			return rev, nil, false, nil
		}
		return rev, nil, false, err
	}

	if pjv == nil {
		return 0, nil, false, nil
	}

	if pjv.KV.ModRevision != revision {
		return rev, pjv.KV, false, nil
	}

	updateValue := natsData{
		Delete:       false,
		Create:       false,
		PrevRevision: pjv.KV.ModRevision,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: pjv.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
	}
	if pjv.KV.CreateRevision == 0 {
		updateValue.KV.CreateRevision = rev
	}

	valueBytes, err := updateValue.Encode()
	if err != nil {
		return 0, nil, false, err
	}

	seq, err := b.kv.Update(key, valueBytes, uint64(revision))
	if err != nil {
		return 0, nil, false, err
	}

	updateValue.KV.ModRevision = int64(seq)

	return int64(seq), updateValue.KV, true, err
}

func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*server.KeyValue, error) {
	// its assumed that when there is a start key that that key exists.
	if strings.HasSuffix(prefix, "/") {
		if prefix == startKey || strings.HasPrefix(prefix, startKey) {
			startKey = ""
		}
	}

	storeRev, err := b.kv.BucketRevision()
	if err != nil {
		return 0, nil, err
	}

	kvs := make([]*server.KeyValue, 0)
	var count int64

	// startkey provided so get max revision after the startKey matching the prefix
	if startKey != "" {
		histories := make(map[string][]nats.KeyValueEntry)
		var minRev int64
		//var innerEntry nats.KeyValueEntry
		if entries, err := b.kv.History(startKey, nats.Context(ctx)); err == nil {
			histories[startKey] = entries
			for i := len(entries) - 1; i >= 0; i-- {
				// find the matching startKey
				if int64(entries[i].Revision()) <= revision {
					minRev = int64(entries[i].Revision())
					b.l.Debugf("Found min revision=%d for key=%s", minRev, startKey)
					break
				}
			}
		} else {
			return 0, nil, err
		}

		keys, err := b.kv.GetKeys(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}

		for _, key := range keys {
			if key != startKey {
				if history, err := b.kv.History(key, nats.Context(ctx)); err == nil {
					histories[key] = history
				} else {
					// should not happen
					b.l.Warnf("no history for %s", key)
				}
			}
		}
		var nextRevID = minRev
		var nextRevision nats.KeyValueEntry
		for k, v := range histories {
			b.l.Debugf("Checking %s history", k)
			for i := len(v) - 1; i >= 0; i-- {
				if int64(v[i].Revision()) > nextRevID && int64(v[i].Revision()) <= revision {
					nextRevID = int64(v[i].Revision())
					nextRevision = v[i]
					b.l.Debugf("found next rev=%d", nextRevID)
					break
				} else if int64(v[i].Revision()) <= nextRevID {
					break
				}
			}
		}
		if nextRevision != nil {
			var entry natsData
			err := entry.Decode(nextRevision)
			if err != nil {
				return 0, nil, err
			}
			kvs = append(kvs, entry.KV)
		}

		return storeRev, kvs, nil
	}

	current := true

	if revision != 0 {
		storeRev = revision
		current = false
	}

	if current {
		entries, err := b.kv.GetKeyValues(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}

		for _, e := range entries {
			if count < limit || limit == 0 {
				var entry natsData
				err := entry.Decode(e)
				if !b.isExpiredKey(&entry) && err == nil {
					kvs = append(kvs, entry.KV)
					count++
				}
			} else {
				break
			}
		}

	} else {
		keys, err := b.kv.GetKeys(ctx, prefix, true)
		if err != nil {
			return 0, nil, err
		}
		if revision == 0 && len(keys) == 0 {
			return storeRev, nil, nil
		}

		for _, key := range keys {
			if count < limit || limit == 0 {
				if history, err := b.kv.History(key, nats.Context(ctx)); err == nil {
					for i := len(history) - 1; i >= 0; i-- {
						if int64(history[i].Revision()) <= revision {
							var entry natsData
							if err := entry.Decode(history[i]); err == nil {
								kvs = append(kvs, entry.KV)
								count++
							} else {
								b.l.Warnf("Could not decode %s rev=> %d", key, history[i].Revision())
							}
							break
						}
					}
				} else {
					// should not happen
					b.l.Warnf("no history for %s", key)
				}
			}
		}

	}
	return storeRev, kvs, nil
}

func (d *Backend) listAfter(ctx context.Context, prefix string, revision int64) (int64, []*server.Event, error) {
	entries, err := d.kv.GetKeyValues(ctx, prefix, false)

	if err != nil {
		return 0, nil, err
	}

	storeRev, err := d.kv.BucketRevision()
	if err != nil {
		return 0, nil, err
	}
	if revision != 0 {
		storeRev = revision
	}
	events := make([]*server.Event, 0)
	for _, e := range entries {
		var kv natsData
		err := kv.Decode(e)
		if err == nil && int64(e.Revision()) > revision {
			event := server.Event{
				Delete: kv.Delete,
				Create: kv.Create,
				KV:     kv.KV,
				PrevKV: &server.KeyValue{},
			}
			if _, prevKV, err := d.Get(ctx, kv.KV.Key, "", 1, kv.PrevRevision); err == nil && prevKV != nil {
				event.PrevKV = prevKV
			}

			events = append(events, &event)
		}
	}
	return storeRev, events, nil
}

func (b *Backend) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {
	// TODO: refactor to use a consumer to start after revision.
	watcher, err := b.kv.Watch(prefix, nats.IgnoreDeletes(), nats.Context(ctx))
	if err != nil {
		b.l.Errorf("failed to create watcher %s for revision %d", prefix, revision)
		ch := make(chan []*server.Event, 0)
		close(ch)
		return ch
	}

	if revision > 0 {
		revision--
	}

	_, events, err := b.listAfter(ctx, prefix, revision)
	if err != nil {
		b.l.Errorf("failed to create watcher %s for revision %d", prefix, revision)
	}

	result := make(chan []*server.Event, 100)

	go func() {
		if len(events) > 0 {
			result <- events
			revision = events[len(events)-1].KV.ModRevision
		}

		for {
			select {
			case i := <-watcher.Updates():
				if i != nil {
					if int64(i.Revision()) > revision {
						events := make([]*server.Event, 1)
						var err error
						value := &natsData{
							KV:           &server.KeyValue{},
							PrevRevision: 0,
							Create:       false,
							Delete:       false,
						}
						prevValue := &natsData{
							KV:           &server.KeyValue{},
							PrevRevision: 0,
							Create:       false,
							Delete:       false,
						}
						lastEntry := i

						err = value.Decode(lastEntry)
						if err != nil {
							b.l.Warnf("watch event: could not decode %s seq %d", i.Key(), i.Revision())
						}
						if _, prevEntry, prevErr := b.get(ctx, i.Key(), value.PrevRevision, false); prevErr == nil {
							if prevEntry != nil {
								prevValue = prevEntry
							}
						}
						if err == nil {
							event := &server.Event{
								Create: value.Create,
								Delete: value.Delete,
								KV:     value.KV,
								PrevKV: prevValue.KV,
							}
							events[0] = event
							result <- events
						} else {
							b.l.Warnf("error decoding %s event %v", i.Key(), err)
							continue
						}
					}
				}
			case <-ctx.Done():
				b.l.Infof("watcher: %s context cancelled", prefix)
				if err := watcher.Stop(); err != nil && err != nats.ErrBadSubscription {
					b.l.Warnf("error stopping %s watcher: %v", prefix, err)
				}
				return
			}
		}
	}()

	return result
}
