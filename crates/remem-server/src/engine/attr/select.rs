//! Predicate evaluation and access-path selection.
//!
//! An access path narrows the candidate set; the sidecar row decides. Every
//! candidate is verified against its row before it is returned, which is what
//! makes a stale index entry harmless (spec D3) and what lets unindexed slots
//! participate in a filter for free. Verification catches a *stale* entry —
//! one whose row changed or vanished — but nothing here detects a *missing*
//! one: a record whose index entry was never written or was lost simply does
//! not surface as a candidate.

use std::ops::Bound;

use bytes::Bytes;

use super::row::AttrRow;
use super::value::AttrValue;
use crate::engine::index::IndexPosition;

/// A request for one bounded page of an ordered selection.
pub struct OrderedSelect<'a> {
    /// Conditions every returned record satisfies. A predicate on
    /// `order_slot` also narrows the walk itself.
    pub preds: &'a [AttrPred],
    /// The slot whose index supplies the order. Named by the caller, not
    /// chosen by a cost model: the order is part of the contract.
    pub order_slot: u16,
    /// Resume strictly after this position. `None` starts at the range edge.
    pub after: Option<IndexPosition>,
    /// Walk from the high end of the range towards the low one.
    pub descending: bool,
    /// How many matching records to return.
    pub limit: usize,
    /// How many candidates may be examined before the walk gives up and
    /// reports itself truncated. Without it a selective predicate over a
    /// large range is unbounded again by the back door.
    pub effort: usize,
}

/// One page of an ordered selection, and what the walk learned about the rest.
///
/// `exhausted` and `truncated` are deliberately separate: a short page means
/// "that was everything" in the first case and "we stopped looking" in the
/// second, and a caller that cannot tell them apart reports one as the other.
pub struct OrderedPage {
    pub keys: Vec<Bytes>,
    /// How many candidates this walk looked at, matching or not.
    ///
    /// What the effort bound is denominated in, so a caller running several
    /// walks under one budget can subtract honestly. The number of keys
    /// returned says nothing about the work done to find them.
    pub examined: usize,
    /// The position of the last candidate examined. Resuming from it visits
    /// what follows without revisiting what this page already covered.
    pub next: Option<IndexPosition>,
    pub exhausted: bool,
    pub truncated: bool,
}

impl OrderedPage {
    /// The answer when attribute support is switched off entirely: no keys,
    /// and nothing left to resume into.
    pub fn empty() -> Self {
        Self {
            keys: Vec::new(),
            examined: 0,
            next: None,
            exhausted: true,
            truncated: false,
        }
    }

    /// Whether a further page could exist. False only when the walk reached
    /// the end of its range.
    pub fn has_more(&self) -> bool {
        !self.exhausted
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AttrPred {
    Eq(u16, AttrValue),
    Range(u16, Bound<AttrValue>, Bound<AttrValue>),
}

impl AttrPred {
    pub fn slot(&self) -> u16 {
        match self {
            AttrPred::Eq(s, _) | AttrPred::Range(s, _, _) => *s,
        }
    }

    /// Inclusive `order_key` bounds for an index range walk.
    ///
    /// `order_key` is a bijection on distinct values, so nudging an exclusive
    /// bound in by one encoded unit is exact for narrowing the walk;
    /// `matches` re-checks the bound precisely against the row regardless.
    pub fn order_bounds(&self) -> (u64, u64) {
        match self {
            AttrPred::Eq(_, v) => (v.order_key(), v.order_key()),
            AttrPred::Range(_, lo, hi) => {
                let start = match lo {
                    Bound::Included(v) => v.order_key(),
                    Bound::Excluded(v) => v.order_key().saturating_add(1),
                    Bound::Unbounded => 0,
                };
                let end = match hi {
                    Bound::Included(v) => v.order_key(),
                    Bound::Excluded(v) => v.order_key().saturating_sub(1),
                    Bound::Unbounded => u64::MAX,
                };
                (start, end)
            }
        }
    }

    fn matches_row(&self, row: &AttrRow) -> bool {
        let Some(actual) = row.get(self.slot()) else {
            return false;
        };
        match self {
            AttrPred::Eq(_, want) => actual == *want,
            AttrPred::Range(_, lo, hi) => {
                let k = actual.order_key();
                let lo_ok = match lo {
                    Bound::Included(v) => k >= v.order_key(),
                    Bound::Excluded(v) => k > v.order_key(),
                    Bound::Unbounded => true,
                };
                let hi_ok = match hi {
                    Bound::Included(v) => k <= v.order_key(),
                    Bound::Excluded(v) => k < v.order_key(),
                    Bound::Unbounded => true,
                };
                lo_ok && hi_ok
            }
        }
    }
}

/// Whether `row` satisfies every predicate.
pub fn matches(row: &AttrRow, preds: &[AttrPred]) -> bool {
    preds.iter().all(|p| p.matches_row(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::attr::row::AttrRow;
    use crate::engine::attr::schema::{AttrSchema, SlotDef};
    use crate::engine::attr::value::AttrType;
    use crate::engine::storage::engine::{EngineConfig, StorageEngine};
    use bytes::Bytes;
    use std::ops::Bound;

    const ARCHIVED: u16 = 0;
    const IMPORTANCE: u16 = 2;
    const CREATED_AT: u16 = 3;

    fn schema() -> AttrSchema {
        AttrSchema {
            version: 1,
            slots: vec![
                SlotDef {
                    slot: ARCHIVED,
                    name: "archived".into(),
                    ty: AttrType::Bool,
                    indexed: false,
                    retired: false,
                },
                SlotDef {
                    slot: IMPORTANCE,
                    name: "importance".into(),
                    ty: AttrType::F32,
                    indexed: true,
                    retired: false,
                },
                SlotDef {
                    slot: CREATED_AT,
                    name: "created_at".into(),
                    ty: AttrType::U64,
                    indexed: true,
                    retired: false,
                },
            ],
        }
    }

    fn row(archived: bool, importance: f32, created_at: u64) -> AttrRow {
        let mut r = AttrRow::new(1);
        r.set(ARCHIVED, AttrValue::Bool(archived));
        r.set(IMPORTANCE, AttrValue::F32(importance));
        r.set(CREATED_AT, AttrValue::U64(created_at));
        r
    }

    async fn engine_with(records: &[(&str, bool, f32, u64)]) -> (tempfile::TempDir, StorageEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            attr_schema: Some(schema()),
            ..Default::default()
        })
        .await
        .unwrap();
        for (key, archived, importance, created_at) in records {
            engine
                .put_with_attrs(
                    Bytes::from(key.to_string()),
                    Bytes::from("payload"),
                    &row(*archived, *importance, *created_at),
                )
                .await
                .unwrap();
        }
        (dir, engine)
    }

    fn keys(result: &[Bytes]) -> Vec<String> {
        let mut v: Vec<String> = result
            .iter()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .collect();
        v.sort();
        v
    }

    #[tokio::test]
    async fn a_range_predicate_selects_through_the_ordered_index() {
        let (_d, engine) = engine_with(&[
            ("memory:a", false, 0.1, 100),
            ("memory:b", false, 0.5, 200),
            ("memory:c", false, 0.9, 300),
        ])
        .await;

        let hits = engine
            .select(
                &[AttrPred::Range(
                    IMPORTANCE,
                    Bound::Included(AttrValue::F32(0.4)),
                    Bound::Included(AttrValue::F32(1.0)),
                )],
                None,
            )
            .await
            .unwrap();

        assert_eq!(keys(&hits), vec!["memory:b", "memory:c"]);
    }

    #[tokio::test]
    async fn an_eq_predicate_on_an_unindexed_slot_still_filters() {
        // archived carries no index (spec §3.1); it is evaluated from the row
        // during the verification the access path already pays for.
        let (_d, engine) = engine_with(&[
            ("memory:live", false, 0.5, 100),
            ("memory:gone", true, 0.5, 200),
        ])
        .await;

        let hits = engine
            .select(&[AttrPred::Eq(ARCHIVED, AttrValue::Bool(false))], None)
            .await
            .unwrap();

        assert_eq!(keys(&hits), vec!["memory:live"]);
    }

    #[tokio::test]
    async fn predicates_are_anded() {
        let (_d, engine) = engine_with(&[
            ("memory:a", false, 0.9, 100),
            ("memory:b", true, 0.9, 200),
            ("memory:c", false, 0.1, 300),
        ])
        .await;

        let hits = engine
            .select(
                &[
                    AttrPred::Eq(ARCHIVED, AttrValue::Bool(false)),
                    AttrPred::Range(
                        IMPORTANCE,
                        Bound::Included(AttrValue::F32(0.5)),
                        Bound::Unbounded,
                    ),
                ],
                None,
            )
            .await
            .unwrap();

        assert_eq!(keys(&hits), vec!["memory:a"]);
    }

    #[tokio::test]
    async fn a_stale_index_entry_is_never_returned() {
        // The regression test for spec D3. Delete the row out from under the
        // index and assert verification catches it: an index entry is a
        // candidate, never an answer.
        let (_d, engine) = engine_with(&[("memory:ghost", false, 0.7, 100)]).await;

        engine
            .delete(crate::engine::attr::attr_key(b"memory:ghost"))
            .await
            .unwrap();

        let hits = engine
            .select(
                &[AttrPred::Range(
                    IMPORTANCE,
                    Bound::Unbounded,
                    Bound::Unbounded,
                )],
                None,
            )
            .await
            .unwrap();

        assert!(
            hits.is_empty(),
            "a candidate whose row is gone must be dropped"
        );
    }

    #[tokio::test]
    async fn exclusive_bounds_are_honoured() {
        let (_d, engine) = engine_with(&[
            ("memory:at", false, 0.5, 100),
            ("memory:above", false, 0.6, 200),
        ])
        .await;

        let hits = engine
            .select(
                &[AttrPred::Range(
                    IMPORTANCE,
                    Bound::Excluded(AttrValue::F32(0.5)),
                    Bound::Unbounded,
                )],
                None,
            )
            .await
            .unwrap();

        assert_eq!(keys(&hits), vec!["memory:above"]);
    }

    #[tokio::test]
    async fn an_eq_predicate_on_an_indexed_slot_is_chosen_as_the_access_path() {
        // No Range predicate targets an indexed slot here, so tier 2 of the
        // access-path chain applies: the first predicate on an indexed slot,
        // which is this Eq.
        //
        // The predicate deliberately targets CREATED_AT rather than
        // IMPORTANCE: IMPORTANCE is the schema's first indexed slot, which
        // is exactly what the tier-3 fallback enumerator
        // (`schema.indexed_slots().next()`) walks when no predicate targets
        // an indexed slot at all. A predicate on IMPORTANCE would pass this
        // test even if tier-2 selection were completely broken and every
        // query fell through to tier-3 — both would walk the same index.
        // Targeting CREATED_AT means tier-2 selection must genuinely pick
        // out this predicate's slot rather than the schema's first one.
        let (_d, engine) = engine_with(&[
            ("memory:a", false, 0.3, 100),
            ("memory:b", false, 0.7, 200),
            ("memory:c", false, 0.7, 300),
        ])
        .await;

        let hits = engine
            .select(&[AttrPred::Eq(CREATED_AT, AttrValue::U64(200))], None)
            .await
            .unwrap();

        assert_eq!(keys(&hits), vec!["memory:b"]);
    }

    #[tokio::test]
    async fn an_included_bound_matches_a_value_stored_exactly_at_the_bound() {
        // The existing range test uses bounds that coincide with nothing
        // stored; this pins down that `Included` really means "or equal to"
        // rather than silently behaving as `Excluded` at the boundary.
        let (_d, engine) = engine_with(&[
            ("memory:below", false, 0.4, 100),
            ("memory:at", false, 0.5, 200),
            ("memory:above", false, 0.6, 300),
        ])
        .await;

        let hits = engine
            .select(
                &[AttrPred::Range(
                    IMPORTANCE,
                    Bound::Included(AttrValue::F32(0.5)),
                    Bound::Included(AttrValue::F32(0.5)),
                )],
                None,
            )
            .await
            .unwrap();

        assert_eq!(keys(&hits), vec!["memory:at"]);
    }

    #[tokio::test]
    async fn an_eq_predicate_matches_a_value_stored_as_negative_zero() {
        // Regression for the access-path/verification disagreement at signed
        // zero: `AttrValue::F32(-0.0) == AttrValue::F32(0.0)` under
        // `PartialEq` (IEEE-754), so `matches_row` accepts a stored `-0.0`
        // row against a query for `0.0`. `order_key` must agree, or the
        // `Eq` access path narrows the index walk to a single point that
        // never visits the `-0.0` entry, and a live, correctly-indexed,
        // unchanged record is silently omitted from the result.
        let (_d, engine) = engine_with(&[("memory:neg_zero", false, -0.0, 100)]).await;

        let hits = engine
            .select(&[AttrPred::Eq(IMPORTANCE, AttrValue::F32(0.0))], None)
            .await
            .unwrap();

        assert_eq!(keys(&hits), vec!["memory:neg_zero"]);
    }

    #[tokio::test]
    async fn a_schema_with_no_indexed_slot_is_an_explicit_error_not_a_silent_empty_result() {
        // Attribute support is on (schema + indexes both exist) but there is
        // no indexed slot anywhere, so no access path can enumerate
        // candidates. That is a query that cannot be answered, which must
        // not be confused with "no matches" — unlike the "no schema
        // registered at all" case, which legitimately returns Ok(empty).
        let dir = tempfile::tempdir().unwrap();
        let no_index_schema = AttrSchema {
            version: 1,
            slots: vec![SlotDef {
                slot: ARCHIVED,
                name: "archived".into(),
                ty: AttrType::Bool,
                indexed: false,
                retired: false,
            }],
        };
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            attr_schema: Some(no_index_schema),
            ..Default::default()
        })
        .await
        .unwrap();

        let err = engine
            .select(&[AttrPred::Eq(ARCHIVED, AttrValue::Bool(false))], None)
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("no indexed slot"),
            "error must name the problem, got: {err}"
        );
    }

    #[tokio::test]
    async fn limit_caps_the_result() {
        let (_d, engine) = engine_with(&[
            ("memory:a", false, 0.1, 100),
            ("memory:b", false, 0.2, 200),
            ("memory:c", false, 0.3, 300),
        ])
        .await;

        let hits = engine
            .select(
                &[AttrPred::Range(
                    IMPORTANCE,
                    Bound::Unbounded,
                    Bound::Unbounded,
                )],
                Some(2),
            )
            .await
            .unwrap();

        assert_eq!(hits.len(), 2);
    }

    /// Result keys in walk order — unlike `keys()`, which sorts and so
    /// cannot see an ordering bug at all.
    fn ordered_keys(result: &[Bytes]) -> Vec<String> {
        result
            .iter()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .collect()
    }

    #[tokio::test]
    async fn an_ordered_select_returns_keys_ascending_by_the_named_slot() {
        // Insertion order, importance order and created_at order are all
        // different here, so a result in created_at order cannot be an
        // accident of any of the others.
        let (_d, engine) = engine_with(&[
            ("memory:b", false, 0.9, 200),
            ("memory:c", false, 0.1, 300),
            ("memory:a", false, 0.5, 100),
        ])
        .await;

        let hits = engine
            .select_ordered(
                &[AttrPred::Eq(ARCHIVED, AttrValue::Bool(false))],
                CREATED_AT,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            ordered_keys(&hits),
            vec!["memory:a", "memory:b", "memory:c"]
        );
    }

    #[tokio::test]
    async fn an_ordered_select_narrows_the_walk_with_a_predicate_on_the_order_slot() {
        let (_d, engine) = engine_with(&[
            ("memory:a", false, 0.5, 100),
            ("memory:b", false, 0.5, 200),
            ("memory:c", false, 0.5, 300),
        ])
        .await;
        engine.reset_attr_reads();

        let hits = engine
            .select_ordered(
                &[AttrPred::Range(
                    CREATED_AT,
                    Bound::Included(AttrValue::U64(200)),
                    Bound::Included(AttrValue::U64(300)),
                )],
                CREATED_AT,
                None,
            )
            .await
            .unwrap();

        assert_eq!(ordered_keys(&hits), vec!["memory:b", "memory:c"]);
        // Plain row-filtering (walk every entry, keep the ones that match)
        // would also return `[b, c]` here, so the output alone cannot tell
        // narrowing apart from a full walk over all three records. Pinning
        // `attr_reads` at 2 -- one lookup per candidate the range walk
        // actually visits -- is what makes the narrowing itself observable:
        // a full walk would read `memory:a`'s row too and land on 3.
        assert_eq!(
            engine.attr_reads(),
            2,
            "a predicate on the order slot must narrow the walk to the two records in range"
        );
    }

    #[tokio::test]
    async fn an_ordered_select_still_filters_on_slots_it_is_not_walking() {
        // The order slot and the filtering slot are different: the walk is
        // over created_at, but importance still has to be honoured, from the
        // row rather than from the access path.
        let (_d, engine) = engine_with(&[
            ("memory:a", false, 0.9, 100),
            ("memory:b", false, 0.1, 200),
            ("memory:c", false, 0.8, 300),
        ])
        .await;

        let hits = engine
            .select_ordered(
                &[AttrPred::Range(
                    IMPORTANCE,
                    Bound::Included(AttrValue::F32(0.5)),
                    Bound::Unbounded,
                )],
                CREATED_AT,
                None,
            )
            .await
            .unwrap();

        assert_eq!(ordered_keys(&hits), vec!["memory:a", "memory:c"]);
    }

    #[tokio::test]
    async fn a_superseded_index_entry_does_not_misplace_a_record() {
        // `accessed_at` moves on every read, and `SegmentedBTreeIndex::remove`
        // strips only the first match it finds, so a superseded entry can
        // outlive the update that replaced it. Honouring one would sort a
        // live record by a value it no longer holds. The record's current
        // entry sits elsewhere in the same walk, so dropping the stale one
        // loses nothing.
        let (_d, engine) = engine_with(&[
            ("memory:early", false, 0.5, 100),
            ("memory:late", false, 0.5, 900),
        ])
        .await;

        // Seal the growing segment first: `BTreeIndex::insert` dedups by key
        // within one segment, so injecting a second entry for `memory:late`
        // into the *same* (still-growing) segment would just move it rather
        // than duplicate it. Sealing moves the real entry into a sealed
        // chunk and starts a fresh growing segment, so the raw insert below
        // lands as a genuine second, superseded entry — the split this test's
        // module doc describes ("sealed segments on disk" alongside the
        // growing one).
        engine.attr_indexes().unwrap().save_if_dirty().unwrap();

        // Plant a second, superseded entry putting `late` at the front.
        engine
            .attr_indexes()
            .unwrap()
            .insert_raw(CREATED_AT, 1, Bytes::from("memory:late"))
            .unwrap();

        let hits = engine
            .select_ordered(
                &[AttrPred::Eq(ARCHIVED, AttrValue::Bool(false))],
                CREATED_AT,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            ordered_keys(&hits),
            vec!["memory:early", "memory:late"],
            "the superseded entry must be skipped, and must not suppress the live one"
        );
    }

    #[tokio::test]
    async fn ordering_by_a_slot_with_no_index_is_an_explicit_error() {
        // ARCHIVED carries no index (spec §3.1), so no walk can produce this
        // order. Returning an empty list would report "no matches" for a
        // question that was never asked — the same call the schema-with-no-
        // indexed-slot case makes.
        let (_d, engine) = engine_with(&[("memory:a", false, 0.5, 100)]).await;

        let err = engine
            .select_ordered(&[], ARCHIVED, None)
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("no ordered index"),
            "error must name the problem, got: {err}"
        );
    }

    #[tokio::test]
    async fn an_ordered_select_honours_limit_from_the_front_of_the_order() {
        // Importance runs opposite to created_at: ascending by IMPORTANCE
        // puts `c` then `b` then `a` first, while ascending by CREATED_AT
        // puts `a` then `b` then `c` first. An implementation that ignored
        // `order_slot` and fell back to the schema's first indexed slot
        // (IMPORTANCE) would return `[c, b]` here, not `[a, b]` -- so only a
        // genuine `created_at` walk can produce the asserted result.
        let (_d, engine) = engine_with(&[
            ("memory:a", false, 0.9, 100),
            ("memory:b", false, 0.5, 200),
            ("memory:c", false, 0.1, 300),
        ])
        .await;

        let hits = engine
            .select_ordered(&[], CREATED_AT, Some(2))
            .await
            .unwrap();

        assert_eq!(ordered_keys(&hits), vec!["memory:a", "memory:b"]);
    }
}
