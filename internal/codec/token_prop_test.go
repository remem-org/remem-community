package codec_test

import (
	"bytes"
	"flag"
	"os"
	"testing"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/tenant"
	"pgregory.net/rapid"
)

func TestMain(m *testing.M) {
	_ = flag.Set("rapid.checks", "1000")
	os.Exit(m.Run())
}

// Any payload survives a round trip, and no payload can forge a different
// tenant or kind -- the header is framed, not delimited, so a payload
// containing a version byte cannot be mistaken for one.
func TestTokenRoundTripsForAnyPayload(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		name := rapid.StringMatching(`[a-z0-9][a-z0-9_-]{0,31}`).Draw(rt, "tenant")
		payload := rapid.SliceOfN(rapid.Byte(), 0, 512).Draw(rt, "payload")
		kind := codec.TokenKind(rapid.SampledFrom([]uint8{1, 2, 3, 4, 5}).Draw(rt, "kind"))

		tok := codec.EncodeToken(kind, tenant.ID(name), payload)

		got, err := codec.DecodeToken(tok, kind, tenant.ID(name))
		if err != nil {
			rt.Fatalf("round trip failed: %v", err)
		}
		if !bytes.Equal(got, payload) {
			rt.Fatalf("payload changed: %v -> %v", payload, got)
		}
	})
}

func TestNoTokenDecodesUnderAnotherTenant(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		a := rapid.StringMatching(`[a-z]{1,8}`).Draw(rt, "a")
		b := rapid.StringMatching(`[a-z]{1,8}`).Draw(rt, "b")
		if a == b {
			rt.Skip("same tenant")
		}
		payload := rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(rt, "payload")

		tok := codec.EncodeToken(codec.TokenSearch, tenant.ID(a), payload)
		if _, err := codec.DecodeToken(tok, codec.TokenSearch, tenant.ID(b)); err == nil {
			rt.Fatalf("a token minted for %q decoded under %q", a, b)
		}
	})
}
