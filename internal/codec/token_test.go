package codec_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

func TestTokenRoundTrips(t *testing.T) {
	payload := []byte{1, 2, 3, 4, 5}
	tok := codec.EncodeToken(codec.TokenSearch, tenant.ID("acme"), payload)

	got, err := codec.DecodeToken(tok, codec.TokenSearch, tenant.ID("acme"))
	if err != nil {
		t.Fatalf("decoding a token this package encoded: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload came back as %v, want %v", got, payload)
	}
}

// A token is base64url without padding because it travels in a query string
// and in a JSON body, and '+' and '=' cost an escaping bug in both eventually.
func TestTokenIsURLSafe(t *testing.T) {
	tok := codec.EncodeToken(codec.TokenListing, tenant.ID("acme"), []byte{0xff, 0xfe, 0xfd, 0xfc})
	if strings.ContainsAny(tok, "+/=") {
		t.Fatalf("token %q contains a character that needs escaping in a URL", tok)
	}
}

// The tenant check is the important one, and it is inside the decoder rather
// than at a call site. A token minted in one tenant and presented in another
// must resolve to nothing -- not to the minting tenant's rows, and not to the
// presenting tenant's rows at the minting tenant's position.
func TestATokenFromAnotherTenantIsRefused(t *testing.T) {
	tok := codec.EncodeToken(codec.TokenSearch, tenant.ID("acme"), []byte{7})

	_, err := codec.DecodeToken(tok, codec.TokenSearch, tenant.ID("globex"))
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("presenting acme's token as globex returned %v, want Invalid", err)
	}
	if !strings.Contains(err.Error(), "different tenant") {
		t.Fatalf("the message does not name the cause: %q", err)
	}
}

// A search cursor presented where a history cursor is expected decodes
// structurally -- same version, same tenant -- and means something completely
// different. The kind byte is what stops it being read as a real position in
// the wrong stream.
func TestATokenOfTheWrongKindIsRefused(t *testing.T) {
	tok := codec.EncodeToken(codec.TokenSearch, tenant.ID("acme"), []byte{7})

	_, err := codec.DecodeToken(tok, codec.TokenHistory, tenant.ID("acme"))
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("presenting a search token as a history token returned %v, want Invalid", err)
	}
}

func TestAnOlderTokenVersionIsRefusedByNumber(t *testing.T) {
	// Version 2 is what query.Cursor issued before this format. A client
	// holding one must be told to start again, not have it misread: the
	// fields after the version byte are positional, so a token read at the
	// wrong version resolves to a real position in a real index -- the wrong
	// one.
	old := "AgVhY21l" // version byte 2, then arbitrary bytes
	_, err := codec.DecodeToken(old, codec.TokenListing, tenant.ID("acme"))
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("a version-2 token returned %v, want Invalid", err)
	}
	if !strings.Contains(err.Error(), "version 2") || !strings.Contains(err.Error(), "version 3") {
		t.Fatalf("the message names neither version: %q", err)
	}
}

func TestAMalformedTokenIsRefusedRatherThanPanicking(t *testing.T) {
	for _, bad := range []string{"!!!not base64!!!", "", "AQ", "AwE"} {
		if _, err := codec.DecodeToken(bad, codec.TokenListing, tenant.ID("acme")); err == nil {
			t.Fatalf("token %q decoded without error", bad)
		}
	}
}

func TestTokenTenantReadsTheTenantWithoutCheckingIt(t *testing.T) {
	tok := codec.EncodeToken(codec.TokenListing, tenant.ID("acme"), []byte{1})
	got, err := codec.TokenTenant(tok)
	if err != nil {
		t.Fatalf("reading the tenant out of a token: %v", err)
	}
	if got != tenant.ID("acme") {
		t.Fatalf("tenant came back as %q, want acme", got)
	}
}
