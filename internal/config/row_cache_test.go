package config_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
)

// The row cache is on by default and sized by one setting, reachable from all
// three sources like every other.
func TestTheRowCacheSizeLoadsAcrossSources(t *testing.T) {
	if n := config.Default().Search.RowCacheEntries; n != config.DefaultRowCacheEntries {
		t.Fatalf("the default row cache holds %d entries, want %d", n, config.DefaultRowCacheEntries)
	}
	path := writeTOML(t, "[search]\nrow_cache_entries = 2048\n")
	for _, tc := range []struct {
		name      string
		env, args []string
		want      string
	}{
		{"file", nil, nil, "2048"},
		{"environment", []string{"REMEM_SEARCH_ROW_CACHE_ENTRIES=4096"}, nil, "4096"},
		{"flags", []string{"REMEM_SEARCH_ROW_CACHE_ENTRIES=4096"}, []string{"--search.row_cache_entries=8192"}, "8192"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := config.Load([]string{path}, tc.env, tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if want := "search.row_cache_entries = " + tc.want + "\n"; !strings.Contains(got.Redacted(), want) {
				t.Fatalf("loaded configuration does not expose %q", want)
			}
		})
	}
}

// Zero is a legal setting and means "no cache". A negative one is a typo, and is
// refused by name rather than clamped.
func TestARowCacheSizeOfZeroIsAllowedAndANegativeOneIsNot(t *testing.T) {
	cfg, err := config.Load(nil, nil, []string{"--search.row_cache_entries=0"})
	if err != nil {
		t.Fatalf("zero must be allowed: it is how an operator turns the cache off: %v", err)
	}
	if cfg.Search.RowCacheEntries != 0 {
		t.Errorf("row_cache_entries loaded as %d, want 0", cfg.Search.RowCacheEntries)
	}
	_, err = config.Load(nil, nil, []string{"--search.row_cache_entries=-1"})
	if err == nil || !strings.Contains(err.Error(), "search.row_cache_entries") {
		t.Errorf("want validation naming the key, got %v", err)
	}
}
