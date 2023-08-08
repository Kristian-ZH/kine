package nats

import (
	"context"
	"errors"
	"io/ioutil"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/nats/kv"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func expErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
}

func expEqualErr(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(want, got) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func expEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func setupBackend(t *testing.T) (*server.Server, *nats.Conn, *Backend) {
	ns := test.RunServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})

	nc, err := nats.Connect(ns.ClientURL())
	noErr(t, err)

	js, err := nc.JetStream()
	noErr(t, err)

	bkt, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "kine",
		History: 10,
	})
	noErr(t, err)

	ekv := kv.NewEncodedKV(bkt, &kv.EtcdKeyCodec{}, &kv.S2ValueCodec{})

	l := logrus.New()
	l.SetOutput(ioutil.Discard)

	b := Backend{
		l:  l,
		kv: ekv,
		js: js,
	}

	return ns, nc, &b
}

func TestBackend(t *testing.T) {
	ns, nc, b := setupBackend(t)
	defer ns.Shutdown()
	defer nc.Drain()

	ctx := context.Background()

	// Create a key with a lease of 1 second.
	rev, err := b.Create(ctx, "/foo", []byte("bar"), 1)
	noErr(t, err)
	expEqual(t, 1, rev)

	rev, ent, err := b.Get(ctx, "/foo", "", 0, 0)
	noErr(t, err)
	expEqual(t, 1, rev)
	expEqual(t, "bar", string(ent.Value))
	expEqual(t, "/foo", ent.Key)
	expEqual(t, 1, ent.Lease)
	expEqual(t, 1, ent.ModRevision)
	expEqual(t, 0, ent.CreateRevision)

	// Count the items.
	rev, count, err := b.Count(ctx, "/foo")
	noErr(t, err)
	expEqual(t, 1, rev)
	expEqual(t, int64(1), count)

	// List the keys.
	rev, ents, err := b.List(ctx, "/foo", "", 0, 0)
	noErr(t, err)
	expEqual(t, 1, rev)
	expEqual(t, 1, len(ents))

	// Expire the lease.
	time.Sleep(time.Second)

	// Try to get again.
	rev, ent, err = b.Get(ctx, "/foo", "", 0, 0)
	expEqualErr(t, err, nats.ErrKeyNotFound)

	// Should be no items.
	rev, count, err = b.Count(ctx, "/foo")
	noErr(t, err)
	expEqual(t, 2, rev)
	expEqual(t, 0, count)

	// Re-create the key without a lease.
	rev, err = b.Create(ctx, "/foo", []byte("bar"), 0)
	noErr(t, err)
	expEqual(t, 3, rev)

	// Get the key again. Expect revision to be 3 since the
	// the key was deleted => 2 and create => 3.
	rev, ent, err = b.Get(ctx, "/foo", "", 0, 0)
	noErr(t, err)
	expEqual(t, 3, rev)
}
