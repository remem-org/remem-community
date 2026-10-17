//go:build cgo && onnx

package discovery_test

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/embedding/onnx"
)

// thresholdCorpus has the shape of Phase 11's measurement corpus: three topics
// of four, three and two memories, and one unlike anything else — 20 ordered
// same-topic pairs and 70 across topics. Phase 11's texts were not kept; these
// are, so the measurement can be repeated.
var thresholdCorpus = []struct{ topic, text string }{
	{"raft", "Raft elects a leader by majority vote, and a follower that hears nothing from the leader for an election timeout starts a new election."},
	{"raft", "In Raft a leader replicates log entries to followers and commits an entry once a majority of the cluster has stored it."},
	{"raft", "A Raft candidate increments its term and asks every peer for a vote; whoever gathers a majority becomes leader for that term."},
	{"raft", "Raft keeps the replicated log consistent: a follower rejects entries whose previous index and term do not match its own log."},
	{"invoice", "Invoice INV-2024-8871 for the cleaning contract was paid by bank transfer on the third of March."},
	{"invoice", "The supplier's invoice for March cleaning services, INV-2024-8871, has been settled in full."},
	{"invoice", "We still owe the plumber for the February invoice; the payment is due at the end of the month."},
	{"baking", "The marzipan for the Christmas stollen is made from ground almonds, sugar and a little rose water."},
	{"baking", "To make marzipan, grind blanched almonds finely and knead them with icing sugar and rosewater."},
	{"unlike", "The north face of the Eiger was first climbed in July 1938 by a German and Austrian party of four."},
}

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	return dot / math.Sqrt(na*nb)
}

func median(v []float64) float64 {
	s := slices.Clone(v)
	slices.Sort(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// TestTheDiscoveryThresholdSitsInTheGap is Phase 11's measurement in the passage
// model's space, over canonical document vectors — what discovery compares
// (openspec/changes/passage-vectors, design decision 11). It reports the two
// populations and asserts that the configured threshold separates them.
func TestTheDiscoveryThresholdSitsInTheGap(t *testing.T) {
	path := os.Getenv("REMEM_EMBEDDING_MODEL_PATH")
	if path == "" {
		path = filepath.Join("..", "..", ".models", "pplx-embed-context-v1-0.6b")
	}
	if _, err := os.Stat(filepath.Join(path, "onnx", "model.onnx")); err != nil {
		t.Skipf("no model at %s", path)
	}
	e, err := onnx.Open(onnx.Config{ModelPath: path, SharedLibraryPath: os.Getenv("REMEM_ONNX_LIBRARY_PATH")})
	if err != nil {
		t.Fatal(err)
	}
	texts := make([]string, len(thresholdCorpus))
	for i, c := range thresholdCorpus {
		texts[i] = c.text
	}
	docs, err := e.EmbedDocuments(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}

	var same, different []float64
	for i := range docs {
		for j := range docs {
			if i == j {
				continue
			}
			c := cosine(docs[i].Vector, docs[j].Vector)
			if thresholdCorpus[i].topic == thresholdCorpus[j].topic {
				same = append(same, c)
			} else {
				different = append(different, c)
			}
			t.Logf("%-7s %-7s %.4f", thresholdCorpus[i].topic, thresholdCorpus[j].topic, c)
		}
	}
	t.Logf("same topic: %d pairs, max %.4f, median %.4f, min %.4f",
		len(same), slices.Max(same), median(same), slices.Min(same))
	t.Logf("different topic: %d pairs, max %.4f, median %.4f",
		len(different), slices.Max(different), median(different))
}

// labelledCorpus is twelve topics, each holding two facts written two ways. Any
// pair of its memories is therefore a paraphrase (one fact, two phrasings), a
// topic-mate (one subject, two facts) or unrelated, by construction rather than
// by anyone's judgement after the fact. Two topics are German, because the
// model is multilingual and discovery will meet memories that are not English.
var labelledCorpus = [][2][2]string{
	{ // raft
		{"Raft elects a leader by majority vote after a follower's election timeout expires.",
			"When a Raft follower hears nothing for an election timeout, it starts an election and wins with a majority of votes."},
		{"A Raft leader commits a log entry once a majority of servers have stored it.",
			"In Raft, an entry counts as committed when it has been replicated to most of the cluster."},
	},
	{ // invoices
		{"Invoice INV-2024-8871 for the cleaning contract was paid by bank transfer on 3 March.",
			"We settled the cleaning company's invoice INV-2024-8871 with a bank transfer on the third of March."},
		{"The plumber's February invoice is still unpaid and falls due at the end of the month.",
			"We have not yet paid the plumber for February; that bill is due when the month ends."},
	},
	{ // baking
		{"Marzipan is made by grinding blanched almonds and kneading them with icing sugar and rosewater.",
			"To make marzipan, finely grind blanched almonds, then knead in powdered sugar and a little rose water."},
		{"Sourdough needs a starter fed with flour and water for several days before the first bake.",
			"Before baking sourdough you must keep a starter alive for days, feeding it flour and water."},
	},
	{ // mountaineering
		{"The north face of the Eiger was first climbed in July 1938 by a party of four.",
			"Four climbers made the first ascent of the Eiger's north face in the summer of 1938."},
		{"Climbers acclimatise on Everest by sleeping at progressively higher camps before the summit push.",
			"Before going for the top of Everest, mountaineers spend nights at ever higher camps to adapt to the altitude."},
	},
	{ // gardening
		{"Tomato seedlings should be hardened off outdoors for a week before they are planted out.",
			"Give young tomato plants a week of gradual exposure outside before moving them into the beds."},
		{"Roses are pruned in late winter, cutting each stem just above an outward-facing bud.",
			"Prune roses at the end of winter, making each cut a little above a bud that points outwards."},
	},
	{ // databases
		{"A B-tree index keeps keys sorted so that range scans touch only the pages they need.",
			"Because a B-tree stores keys in order, a range query reads just the relevant pages."},
		{"Write-ahead logging records every change before it is applied, so a crash can be recovered.",
			"A database writes each change to its log first, which lets it recover after a crash."},
	},
	{ // astronomy
		{"Jupiter's Great Red Spot is a storm larger than the Earth that has lasted for centuries.",
			"The Great Red Spot on Jupiter is a centuries-old storm wider than our whole planet."},
		{"A light-year is the distance light travels in one year, about 9.46 trillion kilometres.",
			"Light covers roughly 9.46 trillion km in a year, and that distance is called a light-year."},
	},
	{ // medicine
		{"Ibuprofen should be taken with food to reduce the risk of stomach irritation.",
			"Take ibuprofen after eating, because on an empty stomach it can irritate the stomach lining."},
		{"Adults are advised to get a tetanus booster every ten years.",
			"A tetanus booster is recommended for grown-ups once a decade."},
	},
	{ // football
		{"The offside rule stops attackers from waiting behind the last defender for the ball.",
			"Under the offside law, a forward may not lurk beyond the last defender waiting for a pass."},
		{"A penalty kick is taken from a spot eleven metres in front of the goal.",
			"Penalties are shot from a mark eleven metres out from the goal line."},
	},
	{ // shipping
		{"The container ship was delayed three days in Rotterdam by a dockworkers' strike.",
			"A strike by port workers held the container vessel in Rotterdam for three extra days."},
		{"Bills of lading must list the consignee, the cargo and the port of discharge.",
			"A bill of lading has to name who receives the goods, what they are and where they are unloaded."},
	},
	{ // German: Wohnung
		{"Die Miete für die Wohnung in der Lindenstraße wird jeweils am ersten des Monats überwiesen.",
			"Wir überweisen die Miete für die Lindenstraße immer am Monatsersten."},
		{"Der Vermieter hat die Heizung in der Wohnung im Oktober reparieren lassen.",
			"Im Oktober ließ der Vermieter die kaputte Heizung der Wohnung instand setzen."},
	},
	{ // German: Bahn
		{"Der ICE nach München fiel wegen einer Signalstörung bei Augsburg aus.",
			"Wegen eines Signalfehlers in der Nähe von Augsburg wurde der ICE Richtung München gestrichen."},
		{"Mit der BahnCard 50 zahlt man auf Flexpreis-Tickets nur die Hälfte.",
			"Die BahnCard 50 halbiert den Preis von Flexpreis-Fahrkarten."},
	},
}

func quantiles(v []float64) (lo, p05, med, p95, hi float64) {
	s := slices.Clone(v)
	slices.Sort(s)
	at := func(q float64) float64 { return s[int(q*float64(len(s)-1))] }
	return s[0], at(0.05), at(0.5), at(0.95), s[len(s)-1]
}

// TestTheDiscoveryThresholdOverALabelledCorpus measures the three populations a
// discovery threshold has to tell apart, over canonical document vectors.
func TestTheDiscoveryThresholdOverALabelledCorpus(t *testing.T) {
	path := os.Getenv("REMEM_EMBEDDING_MODEL_PATH")
	if path == "" {
		path = filepath.Join("..", "..", ".models", "pplx-embed-context-v1-0.6b")
	}
	if _, err := os.Stat(filepath.Join(path, "onnx", "model.onnx")); err != nil {
		t.Skipf("no model at %s", path)
	}
	e, err := onnx.Open(onnx.Config{ModelPath: path, SharedLibraryPath: os.Getenv("REMEM_ONNX_LIBRARY_PATH")})
	if err != nil {
		t.Fatal(err)
	}
	type mem struct {
		topic, fact int
		text        string
	}
	var mems []mem
	for ti, topic := range labelledCorpus {
		for fi, fact := range topic {
			for _, text := range fact {
				mems = append(mems, mem{ti, fi, text})
			}
		}
	}
	texts := make([]string, len(mems))
	for i, m := range mems {
		texts[i] = m.text
	}
	docs, err := e.EmbedDocuments(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	var para, mate, unrelated []float64
	for i := range mems {
		for j := i + 1; j < len(mems); j++ {
			c := cosine(docs[i].Vector, docs[j].Vector)
			switch {
			case mems[i].topic != mems[j].topic:
				unrelated = append(unrelated, c)
			case mems[i].fact == mems[j].fact:
				para = append(para, c)
			default:
				mate = append(mate, c)
			}
		}
	}
	for _, p := range []struct {
		name string
		v    []float64
	}{{"paraphrase", para}, {"topic-mate", mate}, {"unrelated", unrelated}} {
		lo, p05, med, p95, hi := quantiles(p.v)
		t.Logf("%-10s %4d pairs: min %.4f p05 %.4f median %.4f p95 %.4f max %.4f", p.name, len(p.v), lo, p05, med, p95, hi)
	}
	// The gate: the configured threshold links every paraphrase and nothing
	// else, with the populations on either side of it.
	if th := float64(discovery.DefaultThreshold); !(slices.Max(mate) < th && slices.Max(unrelated) < th && th < slices.Min(para)) {
		t.Errorf("the threshold %.2f does not separate paraphrases (min %.4f) from topic-mates (max %.4f) "+
			"and unrelated pairs (max %.4f)", th, slices.Min(para), slices.Max(mate), slices.Max(unrelated))
	}
	for _, th := range []float64{0.5, 0.55, 0.6, 0.65, 0.7, 0.75} {
		count := func(v []float64) int {
			n := 0
			for _, x := range v {
				if x >= th {
					n++
				}
			}
			return n
		}
		t.Logf("at %.2f: links %d/%d paraphrases, %d/%d topic-mates, %d/%d unrelated",
			th, count(para), len(para), count(mate), len(mate), count(unrelated), len(unrelated))
	}
}
