package mcp

// The measurements behind item 11 of the server speed plan, "encode MCP
// tool results once". They are kept rather than thrown away with the decision:
// the conclusion was that the shipping path stays as it is, and the next person
// to look at it should be able to re-run the comparison rather than redo it.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// benchSearchPayload is a search_memories answer the size a model actually
// receives: explain is always on, so every hit carries its matched list and its
// per-source breakdown, and the content is a paragraph rather than a word.
func benchSearchPayload(n int) searchPayload {
	out := searchPayload{Results: make([]searchHit, n), HasMore: true,
		NextCursor: "AwJhY21lkPuOq8zMS9uUzMzLn5jMzEsAAA"}
	for i := range out.Results {
		out.Results[i] = searchHit{
			Memory: memoryPayload{
				ID: fmt.Sprintf("0192f3a4-%04x-7b2c-8d41-2f6a8b9c0d1e", i),
				Content: fmt.Sprintf("Memory %d: the leader election in Raft needs a quorum of the "+
					"configured peers, and a candidate that cannot reach one steps down rather than "+
					"splitting the cluster. Noted after the %dth incident review.", i, i),
				Tags:      []string{"infra", "consensus", "incident"},
				Source:    "incident-review",
				CreatedAt: "2026-02-11T09:14:33Z",
			},
			Score:   0.8123 - float32(i)*0.001,
			Matched: []string{"semantic", "keyword"},
			Sources: []sourceScore{
				{Source: "semantic", Score: 0.71, Rank: i, FusedScore: 0.8123, Distance: 0.29},
				{Source: "keyword", Score: 0.52, Rank: i, FusedScore: 0.8123, Distance: 0},
			},
		}
	}
	return out
}

// TestBenchSearchPayloadIsRealisticallySized keeps the benchmark honest: a
// decision taken on a payload a tenth of the real size is no decision.
func TestBenchSearchPayloadIsRealisticallySized(t *testing.T) {
	for n, min := range map[int]int{10: 4 << 10, 200: 80 << 10} {
		rec := httptest.NewRecorder()
		writeRPC(rec, httptest.NewRequest(http.MethodPost, "/mcp", nil), http.StatusOK,
			toolResult(json.RawMessage(`1`), benchSearchPayload(n)))
		if got := rec.Body.Len(); got < min {
			t.Errorf("%d memories encoded to %d bytes, want at least %d", n, got, min)
		}
		var resp struct {
			Result CallToolResult `json:"result"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%d memories: %v", n, err)
		}
		var round searchPayload
		if err := json.Unmarshal([]byte(resp.Result.Content[0].Text), &round); err != nil {
			t.Fatalf("%d memories, inner: %v", n, err)
		}
		if len(round.Results) != n {
			t.Errorf("%d memories round-tripped to %d", n, len(round.Results))
		}
	}
}

func benchEncode(b *testing.B, n int, encode func(http.ResponseWriter, *http.Request, searchPayload)) {
	out := benchSearchPayload(n)
	r := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	rec := httptest.NewRecorder()
	encode(rec, r, out)
	b.SetBytes(int64(rec.Body.Len()))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rec.Body.Reset()
		encode(rec, r, out)
	}
}

// BenchmarkToolResultEncode is the path as it ships: marshal the answer, copy it
// into a string, and let the envelope encoder escape that string into the
// response.
func BenchmarkToolResultEncode(b *testing.B) {
	for _, n := range []int{10, 200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			benchEncode(b, n, func(w http.ResponseWriter, r *http.Request, out searchPayload) {
				writeRPC(w, r, http.StatusOK, toolResult(json.RawMessage(`1`), out))
			})
		})
	}
}

// BenchmarkToolResultEncodeMarshalOnly is the inner marshal alone: the floor,
// the part that encodes the answer at all.
func BenchmarkToolResultEncodeMarshalOnly(b *testing.B) {
	for _, n := range []int{10, 200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			benchEncode(b, n, func(w http.ResponseWriter, r *http.Request, out searchPayload) {
				body, err := json.Marshal(out)
				if err != nil {
					b.Fatal(err)
				}
				_, _ = w.Write(body)
			})
		})
	}
}

// BenchmarkToolResultEncodeBuffered is the upper bound on candidate one: one
// buffer reused for the inner marshal, so the answer is not allocated afresh per
// call. It is not shippable — a handler serves requests concurrently and cannot
// hold a single buffer — and it is measured only to separate what reuse saves
// from what the pool below costs. The string copy survives either way, because
// Content.Text is a string.
func BenchmarkToolResultEncodeBuffered(b *testing.B) {
	for _, n := range []int{10, 200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			var buf bytes.Buffer
			inner := json.NewEncoder(&buf)
			benchEncode(b, n, func(w http.ResponseWriter, r *http.Request, out searchPayload) {
				buf.Reset()
				if err := inner.Encode(out); err != nil {
					b.Fatal(err)
				}
				// Encode appends a newline the marshalled form does not have.
				text := strings.TrimSuffix(buf.String(), "\n")
				writeRPC(w, r, http.StatusOK, result(json.RawMessage(`1`),
					CallToolResult{Content: textContent(text)}))
			})
		})
	}
}

// BenchmarkToolResultEncodePooled is candidate one as it would have to ship: the
// buffer comes from a pool, because a handler serves requests concurrently and
// cannot hold one buffer. It is the only candidate that is both faster and
// correct under concurrency.
func BenchmarkToolResultEncodePooled(b *testing.B) {
	pool := sync.Pool{New: func() any { return new(bytes.Buffer) }}
	for _, n := range []int{10, 200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			benchEncode(b, n, func(w http.ResponseWriter, r *http.Request, out searchPayload) {
				buf := pool.Get().(*bytes.Buffer)
				buf.Reset()
				enc := json.NewEncoder(buf)
				if err := enc.Encode(out); err != nil {
					b.Fatal(err)
				}
				text := strings.TrimSuffix(buf.String(), "\n")
				pool.Put(buf)
				writeRPC(w, r, http.StatusOK, result(json.RawMessage(`1`),
					CallToolResult{Content: textContent(text)}))
			})
		})
	}
}

// TestBufferedCandidateIsByteIdentical holds candidate one to the bar the
// decision was taken against: json.Encoder appends a newline json.Marshal does
// not, and otherwise escapes identically, so the wire format must not move.
func TestBufferedCandidateIsByteIdentical(t *testing.T) {
	for _, n := range []int{10, 200} {
		out := benchSearchPayload(n)
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(out); err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.TrimSuffix(buf.String(), "\n"); got != string(body) {
			t.Errorf("%d memories: the buffered encoding differs from json.Marshal", n)
		}
	}
}

// rawContent is candidate two's content: the text pre-escaped into a JSON string
// literal, so the envelope encoder copies it rather than escaping it again.
type rawContent struct {
	Type string          `json:"type"`
	Text json.RawMessage `json:"text,omitempty"`
}

type rawCallToolResult struct {
	Content []rawContent `json:"content"`
	IsError bool         `json:"isError,omitempty"`
}

// BenchmarkToolResultEncodePreEscaped is candidate two: escape the answer into a
// JSON string literal once, by hand, and hand the envelope a json.RawMessage. It
// removes the string copy and the envelope's own string encoder; it cannot
// remove the escaping pass, which is what MCP's string-typed content costs.
func BenchmarkToolResultEncodePreEscaped(b *testing.B) {
	for _, n := range []int{10, 200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			var body, quoted []byte
			benchEncode(b, n, func(w http.ResponseWriter, r *http.Request, out searchPayload) {
				var err error
				body, err = json.Marshal(out)
				if err != nil {
					b.Fatal(err)
				}
				quoted, err = json.Marshal(string(body))
				if err != nil {
					b.Fatal(err)
				}
				writeRPC(w, r, http.StatusOK, result(json.RawMessage(`1`),
					rawCallToolResult{Content: []rawContent{{Type: "text", Text: quoted}}}))
			})
		})
	}
}
