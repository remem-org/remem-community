package text

// stopWords are the English function words dropped from content and from a
// query that has something else to say.
//
// The list is deliberately small. A long stop list is a recall decision
// dressed as a size optimisation: "The Who", "to be or not to be" and "IT
// department" are all queries a long list answers with nothing. What is here
// is the set that appears in nearly every document, carries almost no
// selectivity, and costs a posting per record for it.
//
// There is no stemming. Stemming changes recall in ways that must be measured
// before they are adopted (plan §Phase 8 task 1), and it would be a second
// durable decision baked into every posting on disk.
var stopWords = func() map[string]bool {
	list := []string{
		"a", "an", "and", "are", "as", "at",
		"be", "but", "by",
		"for", "from",
		"had", "has", "have", "he", "her", "his",
		"in", "into", "is", "it", "its",
		"of", "on", "or",
		"she", "so",
		"that", "the", "their", "then", "there", "these", "they", "this", "to",
		"was", "were", "which", "will", "with",
	}
	m := make(map[string]bool, len(list))
	for _, w := range list {
		m[w] = true
	}
	return m
}()
