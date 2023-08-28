package nats

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

type entry struct {
	kc    *keyCodec
	vc    *valueCodec
	entry nats.KeyValueEntry
}

func (e *entry) Key() string {
	dk, err := e.kc.Decode(e.entry.Key())
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
	if err := e.vc.Decode(bytes.NewBuffer(e.entry.Value()), buf); err != nil {
		// should not happen
		logrus.Warnf("could not decode value for %s: %v", e.Key(), err)
	}
	return buf.Bytes()
}
func (e *entry) Revision() uint64           { return e.entry.Revision() }
func (e *entry) Created() time.Time         { return e.entry.Created() }
func (e *entry) Delta() uint64              { return e.entry.Delta() }
func (e *entry) Operation() nats.KeyValueOp { return e.entry.Operation() }

type KeyValue struct {
	nkv     nats.KeyValue
	js      nats.JetStreamContext
	kc      *keyCodec
	vc      *valueCodec
	bt      *btree.Map[string, []*seqOp]
	btm     sync.RWMutex
	lastSeq uint64
}

type seqOp struct {
	seq uint64
	op  nats.KeyValueOp
	ex  time.Time
}

type streamWatcher struct {
	sub        *nats.Subscription
	keyCodec   *keyCodec
	valueCodec *valueCodec
	updates    chan nats.KeyValueEntry
	keyPrefix  string
	ctx        context.Context
	cancel     context.CancelFunc
}

func (w *streamWatcher) Context() context.Context {
	if w == nil {
		return nil
	}
	return w.ctx
}

func (w *streamWatcher) Updates() <-chan nats.KeyValueEntry {
	return w.updates
}

func (w *streamWatcher) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}
	w.sub.Drain()
	return nil
}

type kvEntry struct {
	key       string
	bucket    string
	value     []byte
	revision  uint64
	created   time.Time
	delta     uint64
	operation nats.KeyValueOp
}

func (e *kvEntry) Key() string {
	return e.key
}

func (e *kvEntry) Bucket() string { return e.bucket }
func (e *kvEntry) Value() []byte {
	return e.value
}
func (e *kvEntry) Revision() uint64           { return e.revision }
func (e *kvEntry) Created() time.Time         { return e.created }
func (e *kvEntry) Delta() uint64              { return e.delta }
func (e *kvEntry) Operation() nats.KeyValueOp { return e.operation }

func (e *KeyValue) Get(key string) (nats.KeyValueEntry, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return nil, err
	}

	ent, err := e.nkv.Get(ek)
	if err != nil {
		return nil, err
	}

	return &entry{
		kc:    e.kc,
		vc:    e.vc,
		entry: ent,
	}, nil
}

func (e *KeyValue) GetRevision(key string, revision uint64) (nats.KeyValueEntry, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return nil, err
	}

	ent, err := e.nkv.GetRevision(ek, revision)
	if err != nil {
		return nil, err
	}

	return &entry{
		kc:    e.kc,
		vc:    e.vc,
		entry: ent,
	}, nil
}

func (e *KeyValue) Create(key string, value []byte) (uint64, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.vc.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.nkv.Create(ek, buf.Bytes())
}

func (e *KeyValue) Update(key string, value []byte, last uint64) (uint64, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.vc.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.nkv.Update(ek, buf.Bytes(), last)
}

func (e *KeyValue) Delete(key string, opts ...nats.DeleteOpt) error {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return err
	}

	return e.nkv.Delete(ek, opts...)
}

func (e *KeyValue) Watch(ctx context.Context, keys string, startRev int64) (nats.KeyWatcher, error) {
	// Everything but the last token will be treated as a filter
	// on the watcher. The last token will used as a deliver-time filter.
	filter := keys
	if !strings.HasSuffix(filter, "/") {
		idx := strings.LastIndexByte(filter, '/')
		if idx > -1 {
			filter = keys[:idx+1]
		}
	}

	sopts := []nats.SubOpt{
		nats.BindStream(fmt.Sprintf("KV_%s", e.nkv.Bucket())),
		nats.OrderedConsumer(),
	}

	if filter != "" {
		p, err := e.kc.EncodeRange(filter)
		if err != nil {
			return nil, err
		}
		filter = fmt.Sprintf("$KV.%s.%s", e.nkv.Bucket(), p)
	}

	if startRev <= 0 {
		sopts = append(sopts, nats.DeliverLastPerSubject())
	} else {
		sopts = append(sopts, nats.StartSequence(uint64(startRev)))
	}

	wctx, cancel := context.WithCancel(ctx)

	updates := make(chan nats.KeyValueEntry, 100)
	subjectPrefix := fmt.Sprintf("$KV.%s.", e.nkv.Bucket())

	handler := func(msg *nats.Msg) {
		md, _ := msg.Metadata()
		key := strings.TrimPrefix(msg.Subject, subjectPrefix)

		if keys != "" {
			dkey, err := e.kc.Decode(strings.TrimPrefix(key, "."))
			if err != nil || !strings.HasPrefix(dkey, keys) {
				return
			}
		}

		// Default is PUT
		var op nats.KeyValueOp
		switch msg.Header.Get("KV-Operation") {
		case "DEL":
			op = nats.KeyValueDelete
		case "PURGE":
			op = nats.KeyValuePurge
		}
		// Not currently used...
		delta := 0

		updates <- &entry{
			kc: e.kc,
			vc: e.vc,
			entry: &kvEntry{
				key:       key,
				bucket:    e.nkv.Bucket(),
				value:     msg.Data,
				revision:  md.Sequence.Stream,
				created:   md.Timestamp,
				delta:     uint64(delta),
				operation: op,
			},
		}
	}

	sub, err := e.js.Subscribe(filter, handler, sopts...)
	if err != nil {
		cancel()
		return nil, err
	}

	w := &streamWatcher{
		sub:        sub,
		keyCodec:   e.kc,
		valueCodec: e.vc,
		updates:    updates,
		ctx:        wctx,
		cancel:     cancel,
	}

	return w, nil
}

// BucketSize returns the size of the bucket in bytes.
func (e *KeyValue) BucketSize() (int64, error) {
	status, err := e.nkv.Status()
	if err != nil {
		return 0, err
	}
	return int64(status.Bytes()), nil
}

// BucketRevision returns the latest revision of the bucket.
func (e *KeyValue) BucketRevision() int64 {
	e.btm.RLock()
	s := e.lastSeq
	e.btm.RUnlock()
	return int64(s)
}

func (e *KeyValue) btreeWatcher(ctx context.Context) error {
	w, err := e.Watch(ctx, "/", int64(e.lastSeq))
	if err != nil {
		return err
	}
	defer w.Stop()

	status, _ := e.nkv.Status()
	hsize := status.History()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case x := <-w.Updates():
			if x == nil {
				continue
			}

			seq := x.Revision()
			op := x.Operation()

			key := x.Key()

			var ex time.Time
			if op == nats.KeyValuePut {
				var nd natsData
				err = nd.Decode(x)
				if err != nil {
					continue
				}
				if nd.KV.Lease > 0 {
					ex = nd.CreateTime.Add(time.Second * time.Duration(nd.KV.Lease))
				}
			}

			e.btm.Lock()
			e.lastSeq = seq
			val, ok := e.bt.Get(key)
			if !ok {
				val = make([]*seqOp, 0, hsize)
			}
			// Remove the oldest entry.
			if len(val) == cap(val) {
				val = append(val[:0], val[1:]...)
			}
			val = append(val, &seqOp{
				seq: seq,
				op:  op,
				ex:  ex,
			})
			e.bt.Set(key, val)
			e.btm.Unlock()
		}
	}
}

type keySeq struct {
	key string
	seq uint64
}

func (e *KeyValue) Count(prefix string) (int64, error) {
	it := e.bt.Iter()

	if prefix != "" {
		ok := it.Seek(prefix)
		if !ok {
			return 0, nil
		}
	}

	var count int64
	now := time.Now()

	e.btm.RLock()
	defer e.btm.RUnlock()

	for {
		k := it.Key()
		if !strings.HasPrefix(k, prefix) {
			break
		}
		v := it.Value()
		so := v[len(v)-1]

		if so.op == nats.KeyValuePut {
			if so.ex.IsZero() || so.ex.After(now) {
				count++
			}
		}

		if !it.Next() {
			break
		}
	}

	return count, nil
}

func (e *KeyValue) List(prefix, startKey string, limit, revision int64) ([]nats.KeyValueEntry, error) {
	seekKey := prefix
	if startKey != "" {
		seekKey = strings.TrimSuffix(seekKey, "/")
		seekKey = fmt.Sprintf("%s/%s", seekKey, startKey)
	}

	it := e.bt.Iter()
	if seekKey != "" {
		ok := it.Seek(seekKey)
		if !ok {
			return nil, nil
		}
	}

	var matches []*keySeq

	e.btm.RLock()
	defer e.btm.RUnlock()

	for {
		if limit > 0 && len(matches) == int(limit) {
			break
		}

		k := it.Key()
		if !strings.HasPrefix(k, prefix) {
			break
		}

		v := it.Value()

		// Get the latest update for the key.
		if revision <= 0 {
			so := v[len(v)-1]
			if so.op == nats.KeyValuePut {
				if so.ex.IsZero() || so.ex.After(time.Now()) {
					matches = append(matches, &keySeq{key: k, seq: so.seq})
				}
			}
		} else {
			// Find the latest update below the given revision.
			for i := len(v) - 1; i >= 0; i-- {
				so := v[i]
				if so.seq <= uint64(revision) {
					if so.op == nats.KeyValuePut {
						if so.ex.IsZero() || so.ex.After(time.Now()) {
							matches = append(matches, &keySeq{key: k, seq: so.seq})
						}
					}
					break
				}
			}
		}

		if !it.Next() {
			break
		}
	}

	var entries []nats.KeyValueEntry
	for _, m := range matches {
		e, err := e.GetRevision(m.key, m.seq)
		if err != nil {
			logrus.Errorf("get revision in list error: %s @ %d: %v", m.key, m.seq, err)
			continue
		}
		entries = append(entries, e)
	}

	return entries, nil
}

func NewKeyValue(ctx context.Context, bucket nats.KeyValue, js nats.JetStreamContext) *KeyValue {
	kv := &KeyValue{
		nkv: bucket,
		js:  js,
		kc:  &keyCodec{},
		vc:  &valueCodec{},
		bt:  btree.NewMap[string, []*seqOp](0),
	}

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		for {
			err := kv.btreeWatcher(ctx)
			if err != nil {
				logrus.Errorf("btree watcher error: %v", err)
			}
		}
	}()

	return kv
}
