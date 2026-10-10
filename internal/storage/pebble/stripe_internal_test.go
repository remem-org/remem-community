package pebble

import (
	"math/bits"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// The stripe set is the whole of what apply locks, so it has to contain a bit
// for every key the batch touches — written or merely guarded by a condition.
// A missing bit is a conditional commit that is not atomic against the writer
// it was meant to exclude, and nothing else in the package would notice.
func TestABatchStripesEveryTenantItTouches(t *testing.T) {
	const (
		one   = tenant.ID("tenant-one")
		two   = tenant.ID("tenant-two")
		three = tenant.ID("tenant-three")
	)
	// The assertions below count bits, so they only mean anything if these
	// three tenants really do hash apart.
	for _, pair := range [][2]tenant.ID{{one, two}, {one, three}, {two, three}} {
		if stripeOfTenant(t, pair[0]) == stripeOfTenant(t, pair[1]) {
			t.Fatalf("tenants %q and %q share a stripe; pick different ids for this test",
				pair[0], pair[1])
		}
	}

	rid := id.New()
	key := func(tid tenant.ID) []byte {
		return keys.Record(tid, tenant.DefaultNamespace, keys.RecordMemory, rid)
	}

	cases := map[string]struct {
		stage func(b storage.Batch)
		want  uint64
	}{
		"one tenant, one key": {
			stage: func(b storage.Batch) { b.Set(key(one), []byte("v")) },
			want:  stripeSet(t, one),
		},
		"one tenant, several keys and spaces": {
			stage: func(b storage.Batch) {
				b.Set(key(one), []byte("v"))
				b.Set(keys.Vector(one, tenant.DefaultNamespace, rid), []byte("v"))
				b.Set(keys.Text(one, "other-namespace", "term", rid), []byte("v"))
				b.Delete(keys.AttrRow(one, tenant.DefaultNamespace, rid))
			},
			want: stripeSet(t, one),
		},
		"two tenants": {
			stage: func(b storage.Batch) {
				b.Set(key(one), []byte("v"))
				b.Set(key(two), []byte("v"))
			},
			want: stripeSet(t, one, two),
		},
		// A condition is the reason the lock exists, so a condition key counts
		// even when the batch writes nothing in that tenant.
		"a condition in a third tenant": {
			stage: func(b storage.Batch) {
				b.Set(key(one), []byte("v"))
				b.Set(key(two), []byte("v"))
				b.Expect(key(three), nil, false)
			},
			want: stripeSet(t, one, two, three),
		},
		"a condition alone": {
			stage: func(b storage.Batch) { b.Expect(key(three), nil, false) },
			want:  stripeSet(t, three),
		},
		// The system space is untenanted and shares one stripe, which is the
		// stripe a key with no readable tenant falls in too.
		"the system space": {
			stage: func(b storage.Batch) { b.Set(keys.System("/tenants/acme"), []byte("v")) },
			want:  1,
		},
		"a key that is not a Remem key": {
			stage: func(b storage.Batch) { b.Set([]byte("bench/0001"), []byte("v")) },
			want:  1,
		},
		// Nothing staged guards nothing, and locks nothing.
		"empty": {
			stage: func(storage.Batch) {},
			want:  0,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			s := openStore(t)
			b := s.NewBatch().(*batch)
			defer func() { _ = b.Close() }()
			c.stage(b)
			if b.stripes != c.want {
				t.Errorf("stripe set = %#016x (%d stripes), want %#016x (%d stripes)",
					b.stripes, bits.OnesCount64(b.stripes), c.want, bits.OnesCount64(c.want))
			}
		})
	}
}

// Every key of one tenant falls in one stripe, whatever space it is in: that is
// what makes the stripe set small and what makes a tenant's writes order
// against each other.
func TestEveryKeyOfOneTenantSharesAStripe(t *testing.T) {
	const tid = tenant.ID("acme")
	rid := id.New()
	all := [][]byte{
		keys.Record(tid, tenant.DefaultNamespace, keys.RecordMemory, rid),
		keys.Vector(tid, tenant.DefaultNamespace, rid),
		keys.AttrRow(tid, tenant.DefaultNamespace, rid),
		keys.AttrIndex(tid, tenant.DefaultNamespace, 3, []byte("v"), rid),
		keys.Text(tid, tenant.DefaultNamespace, "term", rid),
		keys.EdgeOut(tid, tenant.DefaultNamespace, rid, 1, rid),
		keys.Job(tid, tenant.DefaultNamespace, keys.JobPending, 5, rid),
		keys.Session(tid, tenant.DefaultNamespace, rid),
		keys.VectorIndex(tid, tenant.DefaultNamespace, 9),
		// Another namespace of the same tenant is still the same tenant.
		keys.Record(tid, "other-namespace", keys.RecordMemory, rid),
	}
	want := stripeOf(all[0])
	for _, k := range all[1:] {
		if got := stripeOf(k); got != want {
			t.Errorf("key %x landed in stripe %d, want %d", k, got, want)
		}
	}
}

// A hash that piles every tenant into a few stripes would make the striping
// pointless without failing anything else.
func TestStripesSpreadTenantsAcrossTheStore(t *testing.T) {
	var seen uint64
	for i := range 64 {
		seen |= 1 << stripeOf(keys.Record(
			tenant.ID(string(rune('a'+i%26))+string(rune('a'+i/26))),
			tenant.DefaultNamespace, keys.RecordMemory, id.Zero))
	}
	if n := bits.OnesCount64(seen); n < stripeCount/2 {
		t.Errorf("64 tenants landed in %d of %d stripes; the hash does not spread them", n, stripeCount)
	}
}

// stripeSet is the bitmask the stripes of these tenants make up.
func stripeSet(t *testing.T, tenants ...tenant.ID) uint64 {
	t.Helper()
	var set uint64
	for _, tid := range tenants {
		set |= 1 << stripeOfTenant(t, tid)
	}
	return set
}

func stripeOfTenant(t *testing.T, tid tenant.ID) uint {
	t.Helper()
	return stripeOf(keys.Record(tid, tenant.DefaultNamespace, keys.RecordMemory, id.Zero))
}

// openStore is [open] for the internal tests, which need the concrete type.
func openStore(t *testing.T) *Store {
	t.Helper()
	kv, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv.(*Store)
}
