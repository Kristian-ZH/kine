package kv

import (
	"bytes"
	"context"
	"io"
	"sort"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

func NewEncodedKV(bucket nats.KeyValue, k KeyCodec, v ValueCodec) *EncodedKV {
	return &EncodedKV{bucket: bucket, keyCodec: k, valueCodec: v}
}

type WatcherWithCtx interface {
	WatchWithCtx(ctx context.Context, keys string, opts ...nats.WatchOpt) nats.KeyWatcher
}

type KeyCodec interface {
	Encode(key string) (string, error)
	Decode(key string) (string, error)
	EncodeRange(keys string) (string, error)
}

type ValueCodec interface {
	Encode(src []byte, dst io.Writer) error
	Decode(src io.Reader, dst io.Writer) error
}

type EncodedKV struct {
	WatcherWithCtx
	bucket     nats.KeyValue
	keyCodec   KeyCodec
	valueCodec ValueCodec
}

type watcher struct {
	watcher    nats.KeyWatcher
	keyCodec   KeyCodec
	valueCodec ValueCodec
	updates    chan nats.KeyValueEntry
	ctx        context.Context
	cancel     context.CancelFunc
}

func (w *watcher) Context() context.Context {
	if w == nil {
		return nil
	}
	return w.ctx
}

type entry struct {
	keyCodec   KeyCodec
	valueCodec ValueCodec
	entry      nats.KeyValueEntry
}

func (e *entry) Key() string {
	dk, err := e.keyCodec.Decode(e.entry.Key())
	// should not happen
	if err != nil {
		// should not happen
		logrus.Warnf("could not decode key %s: %v", e.entry.Key(), err)
		return ""
	}

	return dk
}

func (e *entry) Bucket() string { return e.entry.Bucket() }
func (e *entry) Value() []byte {
	buf := new(bytes.Buffer)
	if err := e.valueCodec.Decode(bytes.NewBuffer(e.entry.Value()), buf); err != nil {
		// should not happen
		logrus.Warnf("could not decode value for %s: %v", e.Key(), err)
	}
	return buf.Bytes()
}
func (e *entry) Revision() uint64           { return e.entry.Revision() }
func (e *entry) Created() time.Time         { return e.entry.Created() }
func (e *entry) Delta() uint64              { return e.entry.Delta() }
func (e *entry) Operation() nats.KeyValueOp { return e.entry.Operation() }

func (w *watcher) Updates() <-chan nats.KeyValueEntry { return w.updates }
func (w *watcher) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}

	return w.watcher.Stop()
}

func (e *EncodedKV) newWatcher(w nats.KeyWatcher) nats.KeyWatcher {
	watch := &watcher{
		watcher:    w,
		keyCodec:   e.keyCodec,
		valueCodec: e.valueCodec,
		updates:    make(chan nats.KeyValueEntry, 32)}

	if w.Context() == nil {
		watch.ctx, watch.cancel = context.WithCancel(context.Background())
	} else {
		watch.ctx, watch.cancel = context.WithCancel(w.Context())
	}

	go func() {
		for {
			select {
			case ent := <-w.Updates():
				if ent == nil {
					watch.updates <- nil
					continue
				}

				watch.updates <- &entry{
					keyCodec:   e.keyCodec,
					valueCodec: e.valueCodec,
					entry:      ent,
				}
			case <-watch.ctx.Done():
				return
			}
		}
	}()

	return watch
}

func (e *EncodedKV) Get(key string) (nats.KeyValueEntry, error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}

	ent, err := e.bucket.Get(ek)
	if err != nil {
		return nil, err
	}

	return &entry{
		keyCodec:   e.keyCodec,
		valueCodec: e.valueCodec,
		entry:      ent,
	}, nil
}

func (e *EncodedKV) GetRevision(key string, revision uint64) (nats.KeyValueEntry, error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}

	ent, err := e.bucket.GetRevision(ek, revision)
	if err != nil {
		return nil, err
	}

	return &entry{
		keyCodec:   e.keyCodec,
		valueCodec: e.valueCodec,
		entry:      ent,
	}, nil
}

func (e *EncodedKV) Create(key string, value []byte) (uint64, error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.valueCodec.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.bucket.Create(ek, buf.Bytes())
}

func (e *EncodedKV) Update(key string, value []byte, last uint64) (uint64, error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.valueCodec.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.bucket.Update(ek, buf.Bytes(), last)
}

func (e *EncodedKV) Delete(key string, opts ...nats.DeleteOpt) error {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return err
	}

	return e.bucket.Delete(ek, opts...)
}

func (e *EncodedKV) Purge(key string, opts ...nats.DeleteOpt) error {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return err
	}

	return e.bucket.Purge(ek, opts...)
}

func (e *EncodedKV) Watch(keys string, opts ...nats.WatchOpt) (nats.KeyWatcher, error) {
	ek, err := e.keyCodec.EncodeRange(keys)
	if err != nil {
		return nil, err
	}

	nw, err := e.bucket.Watch(ek, opts...)
	if err != nil {
		return nil, err
	}

	return e.newWatcher(nw), err
}

func (e *EncodedKV) History(key string, opts ...nats.WatchOpt) ([]nats.KeyValueEntry, error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}

	var res []nats.KeyValueEntry
	hist, err := e.bucket.History(ek, opts...)
	if err != nil {
		return nil, err
	}

	for _, ent := range hist {
		res = append(res, &entry{e.keyCodec, e.valueCodec, ent})
	}

	return res, nil
}

// GetKeys returns all keys matching the prefix.
func (e *EncodedKV) GetKeys(ctx context.Context, prefix string, sortResults bool) ([]string, error) {
	watcher, err := e.Watch(prefix, nats.MetaOnly(), nats.IgnoreDeletes(), nats.Context(ctx))
	if err != nil {
		return nil, err
	}
	defer func() {
		err := watcher.Stop()
		if err != nil {
			logrus.Warnf("failed to stop %s getKeys watcher", prefix)
		}
	}()

	var keys []string
	// grab all matching keys immediately
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		keys = append(keys, entry.Key())
	}

	if sortResults {
		sort.Strings(keys)
	}

	return keys, nil
}

// GetKeyValues returns a []nats.KeyValueEntry matching prefix
func (e *EncodedKV) GetKeyValues(ctx context.Context, prefix string, sortResults bool) ([]nats.KeyValueEntry, error) {
	watcher, err := e.bucket.Watch(prefix, nats.IgnoreDeletes(), nats.Context(ctx))
	if err != nil {
		return nil, err
	}
	defer func() {
		err := watcher.Stop()
		if err != nil {
			logrus.Warnf("failed to stop %s getKeyValues watcher", prefix)
		}
	}()

	var entries []nats.KeyValueEntry
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		entries = append(entries, entry)
	}

	if sortResults {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Key() < entries[j].Key()
		})
	}

	return entries, nil
}

// BucketSize returns the size of the bucket in bytes.
func (e *EncodedKV) BucketSize() (int64, error) {
	status, err := e.bucket.Status()
	if err != nil {
		return 0, err
	}
	return int64(status.Bytes()), nil
}

// BucketRevision returns the latest revision of the bucket.
func (e *EncodedKV) BucketRevision() (int64, error) {
	status, err := e.bucket.Status()
	if err != nil {
		return 0, err
	}
	return int64(status.(*nats.KeyValueBucketStatus).StreamInfo().State.LastSeq), nil
}
