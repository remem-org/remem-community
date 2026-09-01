//! Translation from the service-level `MemoryFilters` onto the attribute
//! store's predicate vocabulary.
//!
//! This is half of the mapping the attribute schema defines:
//! `services/attrs.rs` says which slot holds what, and this says which slot a
//! filter field asks about. Tags are the one field with no slot — see
//! `to_attr_preds`.
//!
//! `to_attr_preds` is the system's only filter translation. Both retrieval
//! paths reach the same predicates through it: listing walks an attribute
//! index and verifies each candidate's row (`MemoryManager::list`), and search
//! carries them into `QueryEngine`, which settles every candidate from its row
//! before a payload is read (REM-78). Search used to evaluate filters against
//! whole deserialized records instead, after ranking, which is what made a
//! selective filter silently return a short page.

use std::ops::Bound;

use crate::engine::attr::select::AttrPred;
use crate::engine::attr::value::AttrValue;
use crate::services::attrs::{
    memory_type_code, SLOT_ARCHIVED, SLOT_CREATED_AT, SLOT_IMPORTANCE, SLOT_MEMORY_TYPE,
};
use crate::services::types::MemoryFilters;
#[cfg(test)]
use crate::services::types::StoredMemory;

/// Drop a NaN bound, keep everything else.
///
/// A comparison against NaN is always false, so a NaN bound constrains
/// nothing when a filter is evaluated by comparison. `order_key` places NaN
/// above every finite value instead, so carrying it into a range would
/// narrow the walk to an empty span. Dropping it is what makes the two agree.
/// Infinities need no such treatment: they compare and order-encode alike.
fn drop_nan(bound: Option<f32>) -> Option<f32> {
    bound.filter(|f| !f.is_nan())
}

fn f32_bound(v: Option<f32>) -> Bound<AttrValue> {
    v.map_or(Bound::Unbounded, |f| Bound::Included(AttrValue::F32(f)))
}

fn u64_bound(v: Option<u64>) -> Bound<AttrValue> {
    v.map_or(Bound::Unbounded, |x| Bound::Included(AttrValue::U64(x)))
}

/// The attribute predicates equivalent to `filters` for the listing path.
///
/// Both bounds of a field collapse into a single `Range`: `order_bounds()`
/// reads one predicate to narrow the index walk, so a split pair would leave
/// one end of the range applied only during row verification.
///
/// `filters.tags` produces nothing. Tags are a variable-length list and so
/// carry no slot; the caller narrows with the tag index and settles the
/// question against the payload.
///
/// The `archived` predicate is unconditional — listing is a user-facing read,
/// and archived records are not part of it.
pub fn to_attr_preds(filters: &MemoryFilters) -> Vec<AttrPred> {
    let mut preds = vec![AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))];

    if let Some(mt) = &filters.memory_type {
        preds.push(AttrPred::Eq(
            SLOT_MEMORY_TYPE,
            AttrValue::U8(memory_type_code(mt)),
        ));
    }

    let min = drop_nan(filters.min_importance);
    let max = drop_nan(filters.max_importance);
    if min.is_some() || max.is_some() {
        preds.push(AttrPred::Range(
            SLOT_IMPORTANCE,
            f32_bound(min),
            f32_bound(max),
        ));
    }

    if filters.created_after.is_some() || filters.created_before.is_some() {
        preds.push(AttrPred::Range(
            SLOT_CREATED_AT,
            u64_bound(filters.created_after),
            u64_bound(filters.created_before),
        ));
    }

    preds
}

/// Whether `stored` satisfies `filters`, evaluated against the record itself.
///
/// Test-only. This was the search path's filter evaluator until REM-78 gave
/// search the same pushed-down predicates listing uses; no production code
/// path evaluates a filter against a deserialized record any more.
///
/// It survives as an *oracle*: a deliberately obvious, deliberately slow
/// implementation that equivalence tests measure the real path against
/// (`memory_manager_list_tests::scan_oracle`, and the parity assertion in this
/// module's tests). Reimplementing it inside each test would lose exactly the
/// property that makes it useful — that it is written once, plainly, and
/// shares no code with the thing it checks.
#[cfg(test)]
pub(crate) fn matches_filters(stored: &StoredMemory, filters: &MemoryFilters) -> bool {
    if let Some(ref mt) = filters.memory_type {
        if &stored.memory_type != mt {
            return false;
        }
    }
    if let Some(min) = filters.min_importance {
        if stored.metadata.importance < min {
            return false;
        }
    }
    if let Some(max) = filters.max_importance {
        if stored.metadata.importance > max {
            return false;
        }
    }
    if let Some(after) = filters.created_after {
        if stored.metadata.created_at < after {
            return false;
        }
    }
    if let Some(before) = filters.created_before {
        if stored.metadata.created_at > before {
            return false;
        }
    }
    if !filters.tags.is_empty() {
        let tag_set: std::collections::HashSet<&str> =
            stored.metadata.tags.iter().map(|s| s.as_str()).collect();
        if !filters.tags.iter().all(|t| tag_set.contains(t.as_str())) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};

    #[test]
    fn every_listing_result_is_constrained_to_live_records() {
        // `archived` carries no index, so this predicate never becomes the
        // access path — it is evaluated from the row during the verification
        // the walk already pays for.
        let preds = to_attr_preds(&MemoryFilters::default());
        assert_eq!(
            preds,
            vec![AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))]
        );
    }

    #[test]
    fn memory_type_becomes_an_equality_on_its_stable_code() {
        let f = MemoryFilters {
            memory_type: Some(MemoryType::LongTerm),
            ..Default::default()
        };
        assert!(to_attr_preds(&f).contains(&AttrPred::Eq(SLOT_MEMORY_TYPE, AttrValue::U8(1))));
    }

    #[test]
    fn importance_bounds_become_one_inclusive_range() {
        // One predicate, not two: `order_bounds()` reads a single predicate,
        // so a split pair would leave half the range unapplied to the walk.
        let f = MemoryFilters {
            min_importance: Some(0.25),
            max_importance: Some(0.75),
            ..Default::default()
        };
        assert!(to_attr_preds(&f).contains(&AttrPred::Range(
            SLOT_IMPORTANCE,
            Bound::Included(AttrValue::F32(0.25)),
            Bound::Included(AttrValue::F32(0.75)),
        )));
    }

    #[test]
    fn a_half_open_importance_bound_leaves_the_other_end_unbounded() {
        let f = MemoryFilters {
            min_importance: Some(0.25),
            ..Default::default()
        };
        assert!(to_attr_preds(&f).contains(&AttrPred::Range(
            SLOT_IMPORTANCE,
            Bound::Included(AttrValue::F32(0.25)),
            Bound::Unbounded,
        )));
    }

    #[test]
    fn created_bounds_become_one_inclusive_range() {
        let f = MemoryFilters {
            created_after: Some(1000),
            created_before: Some(2000),
            ..Default::default()
        };
        assert!(to_attr_preds(&f).contains(&AttrPred::Range(
            SLOT_CREATED_AT,
            Bound::Included(AttrValue::U64(1000)),
            Bound::Included(AttrValue::U64(2000)),
        )));
    }

    #[test]
    fn a_nan_importance_bound_is_dropped() {
        // `?min_importance=NaN` parses to f32::NAN, and every comparison
        // against NaN is false, so a scan treats the bound as absent and
        // returns everything. `order_key` does not: NaN's encoding sits above
        // every finite value, so keeping the bound would narrow the walk to
        // nothing and return an empty list for a query a scan answers in
        // full.
        let f = MemoryFilters {
            min_importance: Some(f32::NAN),
            max_importance: Some(f32::NAN),
            ..Default::default()
        };
        assert_eq!(
            to_attr_preds(&f),
            vec![AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))],
            "a NaN bound constrains nothing, so it must produce no predicate"
        );
    }

    #[test]
    fn a_one_sided_nan_importance_bound_drops_only_the_nan_side() {
        // The symmetric case above (both bounds NaN) would still pass if
        // `drop_nan` only handled one of the two fields correctly, since
        // both ends land on the same "no predicate at all" outcome either
        // way. This pins each side independently: a NaN `min_importance`
        // paired with a finite `max_importance` must drop only the min, and
        // the mirror must drop only the max.
        let min_nan = MemoryFilters {
            min_importance: Some(f32::NAN),
            max_importance: Some(0.5),
            ..Default::default()
        };
        assert_eq!(
            to_attr_preds(&min_nan),
            vec![
                AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false)),
                AttrPred::Range(
                    SLOT_IMPORTANCE,
                    Bound::Unbounded,
                    Bound::Included(AttrValue::F32(0.5)),
                ),
            ],
            "a NaN min_importance must drop only the min bound"
        );

        let max_nan = MemoryFilters {
            min_importance: Some(0.5),
            max_importance: Some(f32::NAN),
            ..Default::default()
        };
        assert_eq!(
            to_attr_preds(&max_nan),
            vec![
                AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false)),
                AttrPred::Range(
                    SLOT_IMPORTANCE,
                    Bound::Included(AttrValue::F32(0.5)),
                    Bound::Unbounded,
                ),
            ],
            "a NaN max_importance must drop only the max bound"
        );
    }

    #[test]
    fn an_infinite_importance_bound_is_kept() {
        // Unlike NaN, infinities compare and order-encode consistently, so
        // they mean the same thing on both paths: an infinite bound matches
        // (or excludes) the same records under a scan as under the walk, and
        // must survive `to_attr_preds` unchanged on either side, in either
        // direction.
        let min_pos_inf = MemoryFilters {
            min_importance: Some(f32::INFINITY),
            ..Default::default()
        };
        assert!(to_attr_preds(&min_pos_inf).contains(&AttrPred::Range(
            SLOT_IMPORTANCE,
            Bound::Included(AttrValue::F32(f32::INFINITY)),
            Bound::Unbounded,
        )));

        let max_pos_inf = MemoryFilters {
            max_importance: Some(f32::INFINITY),
            ..Default::default()
        };
        assert!(to_attr_preds(&max_pos_inf).contains(&AttrPred::Range(
            SLOT_IMPORTANCE,
            Bound::Unbounded,
            Bound::Included(AttrValue::F32(f32::INFINITY)),
        )));

        let min_neg_inf = MemoryFilters {
            min_importance: Some(f32::NEG_INFINITY),
            ..Default::default()
        };
        assert!(to_attr_preds(&min_neg_inf).contains(&AttrPred::Range(
            SLOT_IMPORTANCE,
            Bound::Included(AttrValue::F32(f32::NEG_INFINITY)),
            Bound::Unbounded,
        )));
    }

    #[test]
    fn tags_produce_no_predicate() {
        // Tags are a variable-length list and deliberately not a slot; the
        // listing path settles them from the payload.
        let f = MemoryFilters {
            tags: vec!["rust".into()],
            ..Default::default()
        };
        assert_eq!(
            to_attr_preds(&f),
            vec![AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))]
        );
    }

    fn make_stored(
        memory_type: MemoryType,
        importance: f32,
        tags: Vec<String>,
        created_at: u64,
    ) -> StoredMemory {
        StoredMemory {
            id: uuid::Uuid::new_v4(),
            content: "test content".into(),
            memory_type,
            metadata: StoredMetadata {
                created_at,
                updated_at: created_at,
                accessed_at: created_at,
                access_count: 0,
                source: None,
                tags,
                importance,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        }
    }

    fn no_filters() -> MemoryFilters {
        MemoryFilters::default()
    }

    /// The two filter evaluators agree (REM-78).
    ///
    /// `matches_filters` settles the search path's conditions against a whole
    /// record; `to_attr_preds` + `select::matches` settles the listing path's
    /// against the attribute row. REM-78 retires the first in favour of the
    /// second, and nothing asserted they agreed before this.
    ///
    /// They are not term-for-term equal, and the assertion accounts for the
    /// two places the shapes differ rather than papering over them:
    ///
    /// * `to_attr_preds` injects `archived = false` unconditionally, where
    ///   `matches_filters` never looks at `archived` — the search path
    ///   checked it separately, one line above the call.
    /// * `to_attr_preds` deliberately emits nothing for `filters.tags`. Tags
    ///   carry no slot and are settled by the tag index or the payload.
    ///
    /// So the invariant is over what the search path did *as a whole*:
    ///
    /// ```text
    /// !archived && matches_filters(s, f) == select::matches(row, preds) && tags_ok(s, f)
    /// ```
    ///
    /// The inputs are enumerated rather than sampled: what matters here is
    /// boundary values (bound exactly on the value, NaN, the infinities),
    /// not a wide random space, so a product of interesting values covers
    /// more of the risk than a generator would and adds no dependency.
    #[test]
    fn the_two_filter_evaluators_agree_on_every_reachable_record() {
        use crate::engine::attr::select::matches as row_matches;
        use crate::services::attrs::project;

        // Importance as it can actually be stored: `MemoryManager` clamps to
        // 0.0..=1.0 on create and update. NaN is excluded deliberately and
        // pinned separately below — see
        // `nan_importance_would_break_parity_and_cannot_be_stored`.
        let importances = [0.0f32, 0.25, 0.5, 0.75, 1.0];
        let created_ats = [0u64, 1_000, 2_000, u64::MAX];
        let tag_sets = [
            vec![],
            vec!["rust".to_string()],
            vec!["rust".to_string(), "test".to_string()],
        ];
        let types = [MemoryType::ShortTerm, MemoryType::LongTerm];

        // Filter bounds include the non-comparable and infinite values, which
        // are the cases `drop_nan` exists for.
        let bounds = [
            None,
            Some(0.0f32),
            Some(0.5),
            Some(1.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
        ];
        let time_bounds = [None, Some(0u64), Some(1_000), Some(u64::MAX)];
        let filter_tag_sets = [vec![], vec!["rust".to_string()], vec!["python".to_string()]];
        let filter_types = [
            None,
            Some(MemoryType::ShortTerm),
            Some(MemoryType::LongTerm),
        ];

        let mut checked = 0usize;
        for mt in &types {
            for &imp in &importances {
                for &created in &created_ats {
                    for tags in &tag_sets {
                        for archived in [false, true] {
                            let mut stored = make_stored(mt.clone(), imp, tags.clone(), created);
                            stored.archived = archived;
                            let row = project(&stored);

                            for ft in &filter_types {
                                for &min in &bounds {
                                    for &max in &bounds {
                                        for &after in &time_bounds {
                                            for &before in &time_bounds {
                                                for ftags in &filter_tag_sets {
                                                    let f = MemoryFilters {
                                                        memory_type: ft.clone(),
                                                        tags: ftags.clone(),
                                                        min_importance: min,
                                                        max_importance: max,
                                                        created_after: after,
                                                        created_before: before,
                                                    };

                                                    // What the search path as a
                                                    // whole decided.
                                                    let search = !stored.archived
                                                        && matches_filters(&stored, &f);

                                                    // What the attribute row
                                                    // decides, plus the tag half
                                                    // it does not carry.
                                                    let tag_set: std::collections::HashSet<&str> =
                                                        stored
                                                            .metadata
                                                            .tags
                                                            .iter()
                                                            .map(|s| s.as_str())
                                                            .collect();
                                                    let tags_ok = f
                                                        .tags
                                                        .iter()
                                                        .all(|t| tag_set.contains(t.as_str()));
                                                    let attrs =
                                                        row_matches(&row, &to_attr_preds(&f))
                                                            && tags_ok;

                                                    assert_eq!(
                                                        search, attrs,
                                                        "evaluators disagree: \
                                                         type={mt:?} importance={imp} \
                                                         created={created} archived={archived} \
                                                         tags={tags:?} filters={f:?}"
                                                    );
                                                    checked += 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        assert!(checked > 100_000, "product collapsed to {checked} cases");
    }

    #[test]
    fn nan_importance_would_break_parity_and_cannot_be_stored() {
        use crate::engine::attr::select::matches as row_matches;
        use crate::services::attrs::project;

        // The one input the two evaluators genuinely disagree about, pinned
        // rather than quietly excluded from the product above.
        //
        // `project` maps a NaN importance to 0.0 (`attrs::finite`), because
        // NaN order-encodes above every finite value and would poison range
        // walks. `matches_filters` compares NaN directly, and every
        // comparison against NaN is false, so it rejects nothing. Against
        // `min_importance: 0.5` the row says "no" and the record says "yes".
        //
        // This is unreachable through the API: `serde_json` has no NaN
        // literal, so no request can carry one. It is not unreachable
        // *structurally* — `f32::clamp` returns NaN for a NaN input, so
        // `MemoryManager`'s `importance.clamp(0.0, 1.0)` does not remove it.
        // `attrs::finite` is the backstop that keeps the storage side sane
        // either way. If a future non-JSON write path appears, it must
        // reject NaN rather than rely on the clamp.
        let mut stored = make_stored(MemoryType::ShortTerm, f32::NAN, vec![], 1_000);
        stored.archived = false;
        let f = MemoryFilters {
            min_importance: Some(0.5),
            ..Default::default()
        };

        assert!(
            matches_filters(&stored, &f),
            "a NaN importance compares false against every bound, so the \
             record-side evaluator rejects nothing"
        );
        assert!(
            !row_matches(&project(&stored), &to_attr_preds(&f)),
            "the row-side evaluator sees the clamped 0.0 and rejects it"
        );
    }

    #[test]
    fn no_filters_matches_everything() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);
        assert!(matches_filters(&m, &no_filters()));

        let m2 = make_stored(MemoryType::LongTerm, 0.9, vec!["rust".into()], 9999);
        assert!(matches_filters(&m2, &no_filters()));
    }

    #[test]
    fn memory_type_filter_match() {
        let m = make_stored(MemoryType::LongTerm, 0.5, vec![], 1000);
        let f = MemoryFilters {
            memory_type: Some(MemoryType::LongTerm),
            ..Default::default()
        };
        assert!(matches_filters(&m, &f));
    }

    #[test]
    fn memory_type_filter_no_match() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);
        let f = MemoryFilters {
            memory_type: Some(MemoryType::LongTerm),
            ..Default::default()
        };
        assert!(!matches_filters(&m, &f));
    }

    #[test]
    fn min_importance_boundary() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);

        let pass = MemoryFilters {
            min_importance: Some(0.5),
            ..Default::default()
        };
        assert!(matches_filters(&m, &pass));

        let fail = MemoryFilters {
            min_importance: Some(0.51),
            ..Default::default()
        };
        assert!(!matches_filters(&m, &fail));
    }

    #[test]
    fn max_importance_boundary() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);

        let pass = MemoryFilters {
            max_importance: Some(0.5),
            ..Default::default()
        };
        assert!(matches_filters(&m, &pass));

        let fail = MemoryFilters {
            max_importance: Some(0.49),
            ..Default::default()
        };
        assert!(!matches_filters(&m, &fail));
    }

    #[test]
    fn importance_range_filter() {
        let m = make_stored(MemoryType::ShortTerm, 0.6, vec![], 1000);
        let f = MemoryFilters {
            min_importance: Some(0.5),
            max_importance: Some(0.7),
            ..Default::default()
        };
        assert!(matches_filters(&m, &f));

        let low = make_stored(MemoryType::ShortTerm, 0.4, vec![], 1000);
        assert!(!matches_filters(&low, &f));

        let high = make_stored(MemoryType::ShortTerm, 0.8, vec![], 1000);
        assert!(!matches_filters(&high, &f));
    }

    #[test]
    fn created_after_filter() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 2000);

        let pass = MemoryFilters {
            created_after: Some(1000),
            ..Default::default()
        };
        assert!(matches_filters(&m, &pass));

        let fail = MemoryFilters {
            created_after: Some(3000),
            ..Default::default()
        };
        assert!(!matches_filters(&m, &fail));
    }

    #[test]
    fn created_before_filter() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 2000);

        let pass = MemoryFilters {
            created_before: Some(3000),
            ..Default::default()
        };
        assert!(matches_filters(&m, &pass));

        let fail = MemoryFilters {
            created_before: Some(1000),
            ..Default::default()
        };
        assert!(!matches_filters(&m, &fail));
    }

    #[test]
    fn tags_single_match() {
        let m = make_stored(
            MemoryType::ShortTerm,
            0.5,
            vec!["rust".into(), "test".into()],
            1000,
        );

        let pass = MemoryFilters {
            tags: vec!["rust".into()],
            ..Default::default()
        };
        assert!(matches_filters(&m, &pass));

        let fail = MemoryFilters {
            tags: vec!["python".into()],
            ..Default::default()
        };
        assert!(!matches_filters(&m, &fail));
    }

    #[test]
    fn tags_all_must_match() {
        let m = make_stored(
            MemoryType::ShortTerm,
            0.5,
            vec!["rust".into(), "test".into()],
            1000,
        );

        // Both present → passes
        let both = MemoryFilters {
            tags: vec!["rust".into(), "test".into()],
            ..Default::default()
        };
        assert!(matches_filters(&m, &both));

        // One present, one missing → fails
        let partial = MemoryFilters {
            tags: vec!["rust".into(), "python".into()],
            ..Default::default()
        };
        assert!(!matches_filters(&m, &partial));
    }

    #[test]
    fn empty_tags_filter_matches_all() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);
        let f = MemoryFilters {
            tags: vec![],
            ..Default::default()
        };
        assert!(matches_filters(&m, &f));
    }

    #[test]
    fn combined_filters_all_conditions() {
        let m = make_stored(MemoryType::LongTerm, 0.8, vec!["important".into()], 5000);

        let pass = MemoryFilters {
            memory_type: Some(MemoryType::LongTerm),
            min_importance: Some(0.7),
            max_importance: Some(0.9),
            tags: vec!["important".into()],
            created_after: Some(1000),
            created_before: Some(9000),
        };
        assert!(matches_filters(&m, &pass));

        // Flip one condition — wrong type
        let fail = MemoryFilters {
            memory_type: Some(MemoryType::ShortTerm),
            ..pass.clone()
        };
        assert!(!matches_filters(&m, &fail));
    }
}
