//go:build !business

package version

// Edition is the product edition this binary was built as, decided by the
// `business` build tag.
//
// It gates nothing today. Nothing in the binary is business-only: Rust gated
// its metrics endpoint behind its business feature, and Go serves /metrics to
// every edition behind an operator credential (plan §II.10 row 15). The tag is
// the seam a business-only capability would be compiled behind, and this
// constant is how a running deployment says which side of it it is on.
const Edition = "community"
