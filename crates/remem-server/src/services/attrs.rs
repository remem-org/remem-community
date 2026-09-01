//! The attribute schema remem registers with the storage engine, and the
//! projection from a `StoredMemory` onto it.
//!
//! This is the only place that knows both halves: the engine's slots are
//! anonymous, and `StoredMemory` knows nothing about slots. It lives beside
//! the repository because the repository is where a record becomes bytes,
//! and a row must be derived wherever that happens.

use crate::engine::attr::row::AttrRow;
use crate::engine::attr::schema::{AttrSchema, SlotDef};
use crate::engine::attr::value::{AttrType, AttrValue};
use crate::services::types::{MemoryType, StoredMemory};

pub const SLOT_ARCHIVED: u16 = 0;
pub const SLOT_MEMORY_TYPE: u16 = 1;
pub const SLOT_IMPORTANCE: u16 = 2;
pub const SLOT_CREATED_AT: u16 = 3;
pub const SLOT_ACCESSED_AT: u16 = 4;
pub const SLOT_ACCESS_COUNT: u16 = 5;
pub const SLOT_HEALTH: u16 = 6;
pub const SLOT_VALENCE: u16 = 7;
pub const SLOT_AROUSAL: u16 = 8;
pub const SLOT_NEXT_ATTENTION_AT: u16 = 9;

/// Bumped whenever a slot is added or retired. Recorded in every row.
pub const MEMORY_SCHEMA_VERSION: u16 = 2;

/// A day, in milliseconds. The lifecycle sweeps all reason in whole days.
const DAY_MS: u64 = 86_400_000;

/// How long an archived memory waits before cleanup may delete it.
///
/// The value the cleanup task is dispatched with. It lives here because the
/// projection has to know when an archived memory next needs looking at, and
/// that is the only thing that decides it. `cleanup_archived` still takes the
/// age as a parameter and re-checks against it, so a run dispatched with a
/// different age corrects the records it finds rather than trusting this.
pub const CLEANUP_AGE_DAYS: u64 = 30;

/// When a memory next needs a lifecycle sweep to look at it.
///
/// One value shared by every sweep — the earliest instant at which any of them
/// could have work to do — rather than one per sweep. A memory may therefore
/// be woken by a sweep that turns out to have nothing for it; that costs one
/// row read and no payload read, which is much cheaper than maintaining four
/// mutable indexed attributes on the write path.
///
/// Each arm below mirrors the condition of the sweep it stands for, and the
/// mirroring is the fragile part: a sweep whose condition changes without this
/// changing with it would stop being scheduled for the memories it should act
/// on, silently. `every_sweeps_due_time_is_covered_by_the_projection` in the
/// lifecycle tests is what holds the two together.
///
/// `u64::MAX` means "nothing will ever be due", which no live memory should
/// reach — every non-archived memory is at least a candidate for active
/// forgetting.
pub fn next_attention_at(stored: &StoredMemory) -> u64 {
    let m = &stored.metadata;

    // An archived memory has exactly one thing left to happen to it, and the
    // other sweeps all skip it. Scheduling it for its cleanup date is what
    // keeps archived volume out of every other sweep's walk.
    if stored.archived {
        return m.updated_at.saturating_add(CLEANUP_AGE_DAYS * DAY_MS);
    }

    // Flashbulb protection defers decay and forgetting wholesale, so nothing
    // they would do can come due before it lapses.
    let protected_until = m.flashbulb_until.unwrap_or(0);
    let mut due = u64::MAX;

    // expire_short_term: due when the TTL runs out.
    if stored.memory_type == MemoryType::ShortTerm {
        if let Some(ttl_secs) = m.ttl {
            due = due.min(m.created_at.saturating_add(ttl_secs.saturating_mul(1000)));
        }
    }

    // apply_importance_decay: a whole day after the last decay check.
    if stored.memory_type == MemoryType::LongTerm {
        let last = m.last_decay_at.unwrap_or(m.created_at);
        due = due.min(last.saturating_add(DAY_MS).max(protected_until));
    }

    // active_forgetting: a whole day after the last genuine reinforcement or
    // health check, whichever is later.
    let reinforced = m.last_recalled_at.unwrap_or(m.accessed_at);
    let checked = m.last_health_check_at.unwrap_or(m.created_at);
    due = due.min(
        reinforced
            .max(checked)
            .saturating_add(DAY_MS)
            .max(protected_until),
    );

    due
}

/// The next-attention time recorded in `row`, or "due now" when it carries
/// none.
///
/// A row written before the slot existed, or one whose value cannot be read,
/// says nothing about when the memory needs attention — and the safe reading
/// of "nothing" is *now*. Treating it as "never" would drop the memory out of
/// maintenance silently and permanently, which is the one outcome no amount of
/// later correction reaches.
pub fn recorded_attention_time(row: &AttrRow) -> u64 {
    match row.get(SLOT_NEXT_ATTENTION_AT) {
        Some(AttrValue::U64(at)) => at,
        _ => 0,
    }
}

fn def(slot: u16, name: &str, ty: AttrType, indexed: bool) -> SlotDef {
    SlotDef {
        slot,
        name: name.to_string(),
        ty,
        indexed,
        retired: false,
    }
}

/// The registered schema. Slot ids are permanent: retire, never reuse.
pub fn memory_schema() -> AttrSchema {
    AttrSchema {
        version: MEMORY_SCHEMA_VERSION,
        slots: vec![
            def(SLOT_ARCHIVED, "archived", AttrType::Bool, false),
            def(SLOT_MEMORY_TYPE, "memory_type", AttrType::U8, false),
            def(SLOT_IMPORTANCE, "importance", AttrType::F32, true),
            def(SLOT_CREATED_AT, "created_at", AttrType::U64, true),
            // Recall metadata: projected into the row so it travels with the
            // record, but deliberately unindexed. Nothing filters on these
            // three, and nothing orders by them either -- `created_at` is the
            // only ordering key, because an ordering key has to be immutable
            // (REM-79). Indexing them would buy no access path, and no reader
            // to use one.
            def(SLOT_ACCESSED_AT, "accessed_at", AttrType::U64, false),
            def(SLOT_ACCESS_COUNT, "access_count", AttrType::U32, false),
            def(SLOT_HEALTH, "health", AttrType::F32, false),
            def(SLOT_VALENCE, "emotional_valence", AttrType::F32, true),
            def(SLOT_AROUSAL, "arousal", AttrType::F32, true),
            // Indexed because background work selects on it: this is the
            // access path that lets a sweep visit what is due instead of
            // everything that exists.
            def(
                SLOT_NEXT_ATTENTION_AT,
                "next_attention_at",
                AttrType::U64,
                true,
            ),
        ],
    }
}

/// Stable on-disk encoding of `MemoryType`. Never renumber these.
pub fn memory_type_code(t: &MemoryType) -> u8 {
    match t {
        MemoryType::ShortTerm => 0,
        MemoryType::LongTerm => 1,
    }
}

/// NaN order-encodes above every finite value, so it would silently poison
/// range queries. Values are clamped upstream; this is the backstop.
fn finite(f: f32) -> f32 {
    if f.is_nan() {
        0.0
    } else {
        f
    }
}

/// Derive the queryable row for a record.
pub fn project(stored: &StoredMemory) -> AttrRow {
    let m = &stored.metadata;
    let mut row = AttrRow::new(MEMORY_SCHEMA_VERSION);
    row.set(SLOT_ARCHIVED, AttrValue::Bool(stored.archived));
    row.set(
        SLOT_MEMORY_TYPE,
        AttrValue::U8(memory_type_code(&stored.memory_type)),
    );
    row.set(SLOT_IMPORTANCE, AttrValue::F32(finite(m.importance)));
    row.set(SLOT_CREATED_AT, AttrValue::U64(m.created_at));
    row.set(SLOT_ACCESSED_AT, AttrValue::U64(m.accessed_at));
    row.set(SLOT_ACCESS_COUNT, AttrValue::U32(m.access_count));
    row.set(SLOT_HEALTH, AttrValue::F32(finite(m.health)));
    row.set(SLOT_VALENCE, AttrValue::F32(finite(m.emotional_valence)));
    row.set(SLOT_AROUSAL, AttrValue::F32(finite(m.arousal)));
    row.set(
        SLOT_NEXT_ATTENTION_AT,
        AttrValue::U64(next_attention_at(stored)),
    );
    row
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};

    fn stored() -> StoredMemory {
        StoredMemory {
            id: uuid::Uuid::new_v4(),
            content: "hello".into(),
            memory_type: MemoryType::LongTerm,
            metadata: StoredMetadata {
                created_at: 1_700_000_000_000,
                updated_at: 1_700_000_000_001,
                accessed_at: 1_700_000_000_002,
                access_count: 17,
                source: None,
                tags: vec!["a".into()],
                importance: 0.75,
                emotional_valence: -0.5,
                arousal: 0.9,
                health: 42.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: true,
        }
    }

    #[test]
    fn every_schema_slot_is_projected() {
        let row = project(&stored());
        for def in memory_schema().slots.iter().filter(|s| !s.retired) {
            assert!(
                row.get(def.slot).is_some(),
                "slot {} ({}) is in the schema but project() never sets it",
                def.slot,
                def.name
            );
        }
    }

    #[test]
    fn projected_values_match_the_record() {
        let s = stored();
        let row = project(&s);
        assert_eq!(row.get(SLOT_ARCHIVED), Some(AttrValue::Bool(true)));
        assert_eq!(row.get(SLOT_MEMORY_TYPE), Some(AttrValue::U8(1)));
        assert_eq!(row.get(SLOT_IMPORTANCE), Some(AttrValue::F32(0.75)));
        assert_eq!(
            row.get(SLOT_CREATED_AT),
            Some(AttrValue::U64(1_700_000_000_000))
        );
        assert_eq!(
            row.get(SLOT_ACCESSED_AT),
            Some(AttrValue::U64(1_700_000_000_002))
        );
        assert_eq!(row.get(SLOT_ACCESS_COUNT), Some(AttrValue::U32(17)));
        assert_eq!(row.get(SLOT_HEALTH), Some(AttrValue::F32(42.0)));
        assert_eq!(row.get(SLOT_VALENCE), Some(AttrValue::F32(-0.5)));
        assert_eq!(row.get(SLOT_AROUSAL), Some(AttrValue::F32(0.9)));
    }

    #[test]
    fn memory_type_codes_are_distinct_and_stable() {
        // These are persisted; changing one silently reinterprets every
        // existing row.
        assert_eq!(memory_type_code(&MemoryType::ShortTerm), 0);
        assert_eq!(memory_type_code(&MemoryType::LongTerm), 1);
    }

    #[test]
    fn projection_never_emits_nan() {
        // NaN order-encodes above every finite value and would corrupt range
        // results. Clamping happens upstream, so this guards the guarantee.
        let mut s = stored();
        s.metadata.importance = f32::NAN;
        s.metadata.health = f32::NAN;
        s.metadata.emotional_valence = f32::NAN;
        s.metadata.arousal = f32::NAN;
        let row = project(&s);
        for slot in [SLOT_IMPORTANCE, SLOT_HEALTH, SLOT_VALENCE, SLOT_AROUSAL] {
            match row.get(slot) {
                Some(AttrValue::F32(f)) => assert!(!f.is_nan(), "slot {slot} projected NaN"),
                other => panic!("slot {slot} projected {other:?}"),
            }
        }
    }

    /// Independent restatements of each sweep's own "is this due?" test,
    /// written from the sweep code rather than from `next_attention_at`. The
    /// projection is only trustworthy if it never schedules a memory *later*
    /// than the sweep that acts on it would want it.
    mod sweep_conditions {
        use super::*;

        pub fn expiry_due(s: &StoredMemory) -> Option<u64> {
            if s.archived || s.memory_type != MemoryType::ShortTerm {
                return None;
            }
            s.metadata.ttl.map(|ttl| s.metadata.created_at + ttl * 1000)
        }

        pub fn decay_due(s: &StoredMemory) -> Option<u64> {
            if s.archived || s.memory_type != MemoryType::LongTerm {
                return None;
            }
            let last = s.metadata.last_decay_at.unwrap_or(s.metadata.created_at);
            Some((last + DAY_MS).max(s.metadata.flashbulb_until.unwrap_or(0)))
        }

        pub fn forgetting_due(s: &StoredMemory) -> Option<u64> {
            if s.archived {
                return None;
            }
            let reinforced = s
                .metadata
                .last_recalled_at
                .unwrap_or(s.metadata.accessed_at);
            let checked = s
                .metadata
                .last_health_check_at
                .unwrap_or(s.metadata.created_at);
            Some((reinforced.max(checked) + DAY_MS).max(s.metadata.flashbulb_until.unwrap_or(0)))
        }

        pub fn cleanup_due(s: &StoredMemory) -> Option<u64> {
            s.archived
                .then(|| s.metadata.updated_at + CLEANUP_AGE_DAYS * DAY_MS)
        }
    }

    /// Every combination of the state that feeds a due time.
    fn state_matrix() -> Vec<StoredMemory> {
        let base = 1_700_000_000_000u64;
        let mut out = Vec::new();
        for archived in [false, true] {
            for memory_type in [MemoryType::ShortTerm, MemoryType::LongTerm] {
                for ttl in [None, Some(3600u64)] {
                    for flashbulb in [None, Some(base + 5 * DAY_MS)] {
                        for recalled in [None, Some(base + 60_000)] {
                            for checked in [None, Some(base + 120_000)] {
                                for decayed in [None, Some(base + 180_000)] {
                                    let mut s = stored();
                                    s.archived = archived;
                                    s.memory_type = memory_type.clone();
                                    s.metadata.ttl = ttl;
                                    s.metadata.flashbulb_until = flashbulb;
                                    s.metadata.last_recalled_at = recalled;
                                    s.metadata.last_health_check_at = checked;
                                    s.metadata.last_decay_at = decayed;
                                    out.push(s);
                                }
                            }
                        }
                    }
                }
            }
        }
        out
    }

    #[test]
    fn every_sweeps_due_time_is_covered_by_the_projection() {
        // The invariant that makes one shared attention time safe: it may wake
        // a memory earlier than a given sweep needs (cheap -- one row read),
        // but never later (the sweep silently stops running for it).
        for s in state_matrix() {
            let scheduled = next_attention_at(&s);
            for (sweep, due) in [
                ("expiry", sweep_conditions::expiry_due(&s)),
                ("decay", sweep_conditions::decay_due(&s)),
                ("forgetting", sweep_conditions::forgetting_due(&s)),
                ("cleanup", sweep_conditions::cleanup_due(&s)),
            ] {
                if let Some(due) = due {
                    assert!(
                        scheduled <= due,
                        "{sweep} wants attention at {due} but the memory is scheduled for \
                         {scheduled}: archived={} type={:?} ttl={:?} flashbulb={:?}",
                        s.archived,
                        s.memory_type,
                        s.metadata.ttl,
                        s.metadata.flashbulb_until
                    );
                }
            }
        }
    }

    #[test]
    fn a_live_memory_is_always_scheduled_for_something() {
        for s in state_matrix().into_iter().filter(|s| !s.archived) {
            assert!(
                next_attention_at(&s) < u64::MAX,
                "every live memory is at least a candidate for active forgetting"
            );
        }
    }

    #[test]
    fn a_recall_defers_attention_rather_than_leaving_it_due() {
        // Short-term with no TTL: expiry cannot fire and decay is long-term
        // only, so active forgetting is the sweep that binds -- and forgetting
        // is the one a recall defers. On a long-term memory the daily decay
        // date binds instead and a recall moves nothing, which is correct and
        // is why the stored value is a minimum across sweeps.
        let mut s = stored();
        s.archived = false;
        s.memory_type = MemoryType::ShortTerm;
        s.metadata.ttl = None;
        let before = next_attention_at(&s);

        s.metadata.last_recalled_at = Some(s.metadata.created_at + 10 * DAY_MS);
        let after = next_attention_at(&s);

        assert!(
            after > before,
            "using a memory postpones the work that would forget it"
        );
    }

    #[test]
    fn archiving_schedules_a_memory_for_cleanup_and_nothing_else() {
        let mut s = stored();
        s.archived = true;
        s.metadata.updated_at = 1_700_000_000_000;

        assert_eq!(
            next_attention_at(&s),
            s.metadata.updated_at + CLEANUP_AGE_DAYS * DAY_MS,
            "an archived memory has exactly one thing left to happen to it"
        );
    }

    #[test]
    fn a_row_without_a_recorded_time_reads_as_due_now() {
        let mut row = AttrRow::new(MEMORY_SCHEMA_VERSION);
        row.set(SLOT_ARCHIVED, AttrValue::Bool(false));

        assert_eq!(
            recorded_attention_time(&row),
            0,
            "absent must mean due now; 'never' would drop the memory out of \
             maintenance permanently"
        );
    }

    #[test]
    fn the_schema_is_self_consistent() {
        let s = memory_schema();
        let mut seen = std::collections::HashSet::new();
        for def in &s.slots {
            assert!(seen.insert(def.slot), "slot {} is defined twice", def.slot);
        }
        // Low-cardinality slots carry no index: a postings list for
        // `archived = false` covers the corpus and is useless as an access
        // path (spec §3.1).
        assert!(!s.get(SLOT_ARCHIVED).unwrap().indexed);
        assert!(!s.get(SLOT_MEMORY_TYPE).unwrap().indexed);
        // Recall metadata carries no index either: nothing filters on it, and
        // nothing orders by it -- an ordering key has to be immutable, and
        // these three change every time a memory is used (REM-79).
        assert!(!s.get(SLOT_ACCESSED_AT).unwrap().indexed);
        assert!(!s.get(SLOT_ACCESS_COUNT).unwrap().indexed);
        assert!(!s.get(SLOT_HEALTH).unwrap().indexed);
        // The exception to the rule above: this one is mutable *and* indexed,
        // because background work has to select on it. It is not an ordering
        // key for any caller-visible result, so REM-79's immutability rule
        // does not reach it.
        assert!(s.get(SLOT_NEXT_ATTENTION_AT).unwrap().indexed);
        assert_eq!(s.indexed_slots().count(), 5);
    }
}
