package http_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
)

// passageMemory is a memory of eight passages under the fake embedder whose
// fifth is the word "zanzibar" alone.
func passageMemory() string {
	words := make([]string, 8*embeddingtest.PassageWords)
	for i := range words {
		words[i] = "harbour"
		if i >= 4*embeddingtest.PassageWords && i < 5*embeddingtest.PassageWords {
			words[i] = "zanzibar"
		}
	}
	return strings.Join(words, " ")
}

// A semantic result names its passage as an object with snake_case fields; a
// keyword result has no passage key at all, rather than a null or an empty
// object; and under explain, the vector source carries the passage index and
// no other source does.
func TestASearchResultCarriesItsPassage(t *testing.T) {
	srv := newServer(t)
	content := passageMemory()
	body, _ := json.Marshal(map[string]string{"content": content})
	if rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories", strings.NewReader(string(body))); rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}

	var out struct {
		Results []struct {
			Passage *struct {
				Index, Start, End int
			} `json:"passage"`
			Sources []struct {
				Source  string `json:"source"`
				Passage *int   `json:"passage"`
			} `json:"sources"`
		} `json:"results"`
	}
	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories/search",
		strings.NewReader(`{"query":"zanzibar","search_type":"hybrid","limit":1,"explain":true}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("search: %d %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	p := out.Results[0].Passage
	if p == nil || p.Index != 4 || content[p.Start:p.End] != strings.TrimSpace(strings.Repeat("zanzibar ", embeddingtest.PassageWords)) {
		t.Fatalf("passage = %+v, want index 4 bounding the fifth passage: %s", p, rr.Body.String())
	}
	for _, src := range out.Results[0].Sources {
		if (src.Source == "vector") != (src.Passage != nil) {
			t.Fatalf("source %s carries passage %v; only the vector source names one", src.Source, src.Passage)
		}
		if src.Source == "vector" && *src.Passage != 4 {
			t.Fatalf("the vector source names passage %d, want 4", *src.Passage)
		}
	}

	rr = send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories/search",
		strings.NewReader(`{"query":"zanzibar","search_type":"keyword","limit":1}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("keyword search: %d %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), `"passage"`) {
		t.Fatalf("a keyword result carries a passage key: %s", rr.Body.String())
	}
}
