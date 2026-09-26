package config_test

import (
	"path/filepath"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
)

func TestLoadUsesFirstReadableSearchPath(t *testing.T) {
	local := writeTOML(t, "[storage]\npath = 'local'\n")
	system := writeTOML(t, "[storage]\npath = 'system'\n")
	empty := writeTOML(t, "")
	missing := filepath.Join(t.TempDir(), "absent.toml")
	malformed := writeTOML(t, "[broken")
	for _, tc := range []struct {
		name  string
		paths []string
		want  string
	}{
		{"local wins", []string{local, system}, "local"},
		{"missing is skipped", []string{missing, system}, "system"},
		{"empty is selected", []string{empty, system}, config.Default().Storage.Path},
		{"later malformed is ignored", []string{local, malformed}, "local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := config.Load(tc.paths, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.Storage.Path != tc.want {
				t.Fatalf("storage path = %q, want %q", got.Storage.Path, tc.want)
			}
		})
	}
	got, err := config.Load([]string{local}, nil, []string{"--config", system})
	if err != nil {
		t.Fatal(err)
	}
	if got.Storage.Path != "system" {
		t.Fatalf("explicit config ignored: %q", got.Storage.Path)
	}
}
