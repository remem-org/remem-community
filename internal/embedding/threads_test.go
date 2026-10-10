package embedding_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/embedding"
)

// Zero used to mean ONNX Runtime's own default, which reads the host's cores
// and ignores a container's CPU quota: a 2-CPU container on a 32-core host ran
// 32 threads and was throttled. Zero now means the CPUs this process may use,
// which the caller passes in as available.
func TestIntraOpThreadsAreDerivedFromTheCPUsAvailable(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		configured, available int
		want                  int
		wantSource            embedding.ThreadSource
	}{
		{"zero takes the available CPUs", 0, 2, 2, embedding.ThreadsDerived},
		{"zero on a large host takes all of them", 0, 32, 32, embedding.ThreadsDerived},
		{"a configured count is used as given", 4, 32, 4, embedding.ThreadsConfigured},
		{"a configured count may exceed the CPUs", 8, 2, 8, embedding.ThreadsConfigured},
		{"never fewer than one", 0, 0, 1, embedding.ThreadsDerived},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, source := embedding.IntraOpThreads(tc.configured, tc.available)
			if got != tc.want || source != tc.wantSource {
				t.Fatalf("IntraOpThreads(%d, %d) = %d, %v; want %d, %v",
					tc.configured, tc.available, got, source, tc.want, tc.wantSource)
			}
		})
	}
}

func TestThreadSourceNamesItself(t *testing.T) {
	if got := embedding.ThreadsDerived.String(); got != "derived" {
		t.Errorf("ThreadsDerived = %q, want %q", got, "derived")
	}
	if got := embedding.ThreadsConfigured.String(); got != "configured" {
		t.Errorf("ThreadsConfigured = %q, want %q", got, "configured")
	}
}
