package config_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
)

func TestPagingSettingsLoadAcrossSources(t *testing.T) {
	path := writeTOML(t, "[search]\nmax_page_depth = 700\n[paging]\nranked_ttl = '45s'\n")
	for _, tc := range []struct {
		name       string
		env, args  []string
		depth, ttl string
	}{
		{"file", nil, nil, "700", "45s"},
		{"environment", []string{"REMEM_SEARCH_MAX_PAGE_DEPTH=800", "REMEM_PAGING_RANKED_TTL=50s"}, nil, "800", "50s"},
		{"flags", []string{"REMEM_SEARCH_MAX_PAGE_DEPTH=800", "REMEM_PAGING_RANKED_TTL=50s"}, []string{"--search.max_page_depth=900", "--paging.ranked_ttl=55s"}, "900", "55s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := config.Load([]string{path}, tc.env, tc.args)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"search.max_page_depth = " + tc.depth + "\n", "paging.ranked_ttl = " + tc.ttl + "\n"} {
				if !strings.Contains(got.Redacted(), want) {
					t.Fatalf("loaded configuration does not expose %q", want)
				}
			}
		})
	}
}

func TestPagingSettingsRejectNonpositiveValues(t *testing.T) {
	for _, setting := range []string{"search.max_page_depth=0", "search.max_page_depth=-1", "paging.ranked_ttl=0s", "paging.ranked_ttl=-1s"} {
		_, err := config.Load(nil, nil, []string{"--" + setting})
		key, _, _ := strings.Cut(setting, "=")
		if err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "must be") {
			t.Errorf("%s: want value validation naming key, got %v", setting, err)
		}
	}
}
