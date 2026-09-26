package obs

import (
	"regexp"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
)

// fqNameInDesc reads a descriptor's fully qualified name out of its String form.
// client_golang exposes no accessor for it, and the String form is stable: it is
// what the library prints when two collectors collide.
var fqNameInDesc = regexp.MustCompile(`fqName: "([^"]+)"`)

// DeclaredNames returns every metric family this process registers, sorted.
//
// A scrape cannot answer the question this exists for. A family with no
// samples — a counter nothing has incremented, a histogram nothing has
// observed — does not appear in the exposition at all, so "declared and never
// recorded" is invisible from /metrics alone. The storage and search collectors
// were exactly that for ten phases. TestEveryDeclaredMetricIsRecorded compares
// this list with what an ordinary workload actually produces.
func (m *Metrics) DeclaredNames() []string {
	ch := make(chan *prometheus.Desc, 64)
	go func() {
		for _, c := range m.collectors {
			c.Describe(ch)
		}
		close(ch)
	}()

	seen := map[string]bool{}
	for d := range ch {
		if match := fqNameInDesc.FindStringSubmatch(d.String()); match != nil {
			seen[match[1]] = true
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
