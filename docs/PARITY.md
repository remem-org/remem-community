# Parity with Rust Remem

Every behaviour `docs/BEHAVIOUR_BASELINE.md` records has one row here:
**reproduced**, naming the Go test that holds it, or **diverged**, naming the
plan §II.10 row that authorises the difference. A row with neither is work, not
documentation, and `internal/arch`'s `TestEveryParityRowIsReproducedOrDiverged`
fails on it. That test also checks every named test exists, every cited row
exists, and every baseline entry has a row.

Written in Phase 13 (plan Task 3.4) against the frozen reference at
`pre-go-freeze` (`1fa61f9`). Three rows changed Go on the way here, each decided
with the user and each marked below.

Where a behaviour is *also* compared live against the running reference, the
differential harness is named; `test/differential/COVERAGE.md` has the runs.

## The baseline

| Baseline | Behaviour | Status | Evidence |
|---|---|---|---|
| §1.1 | `score` and `fused_score` are different numbers, and results are ordered by `fused_score` | reproduced | `TestScoreAndFusedScoreAreDifferentNumbers`, `TestFusionMatchesTheRustRanking` |
| §1.2 | `score` comes from the best non-graph source, and graph proximity sets it only when alone | reproduced | `TestScoreIgnoresGraphSourcesUnlessAlone`, `TestScoreIgnoresTheGraphWhenSomethingElseMatched` |
| §1.3 | Relevance is recovered cosine, not `1/(1+d)`, with the fallback where cosine cannot be recovered | reproduced | `TestCosineRecoveryMatchesTheRustContract`, `TestScoreFloorsWhereRelevanceDoesNot`, `TestRelevanceFallsBackWhereCosineCannotBeRecovered` |
| §1.4 | A single-step search bypasses rank fusion, so `fused_score` is `1/(1+d)` | reproduced | `TestScoreAndFusedScoreAreDifferentNumbers` |
| §2 | `truncated` means the search stopped looking, never "there are none" | reproduced | `TestTruncatedIsSetAtTheWideningBound`, `TestTruncatedIsFalseWhenComplete`, `TestTruncatedIsNotSetForAnEmptyCorpus` |
| §3 | `access_count` counts recall sessions, not round trips | reproduced | `TestRecallsOutsideTheWindowEachCount`, `TestTheCoalescingWindowSurvivesARestart`; compared live by the harness's lifecycle surface |
| §4 | Recall is not durable: a crash loses the recalls in the flush window | diverged | §II.10 row 4 — Go records a recall as a durable event (`TestARecallCommitsDurably`) |
| §5.1 | A listing orders by `created_at` only | diverged | §II.10 row 3 — five orderings (`TestListOrdersByEachIndexedSlot`) |
| §5.2 | `order=desc` with a non-zero `offset` is refused | diverged | §II.10 row 12 — the redesigned surface has no offset at all; both directions page by cursor, which keeps the guarantee the refusal protects (`TestListPagesInBothDirections`) |
| §6 | Archiving retires a memory from retrieval, and the record survives until cleanup | reproduced | `TestArchivedMemoriesLeaveTheVectorIndex`, `TestListExcludesArchivedUnlessAsked`, `TestRelatedExcludesArchivedMemoriesByDefault` |
| §7 | MCP drops per-source evidence unless `explain` is set | reproduced | `TestSearchResultsOmitSourcesUnlessExplainIsSet` |
| §8 Daily importance decay factor | `0.995` a day | reproduced | `TestLifecycleConstantsAreRustsNumbers`, `TestBuiltinsMatchTheRustConstants` |
| §8 RECALL_HEALTH_BOOST | `10.0`, clamped into `[0, 100]` | reproduced | `TestLifecycleConstantsAreRustsNumbers`, `TestARecallReinforcesHealth` |
| §8 CLEANUP_AGE_DAYS | `30` days | reproduced | `TestLifecycleConstantsAreRustsNumbers`, `TestBuiltinsMatchTheRustConstants` |
| §8 FLASHBULB_AROUSAL_THRESHOLD | `arousal >= 0.8` | reproduced | `TestLifecycleConstantsAreRustsNumbers`, `TestFlashbulbPromotionOverridesTheRequestedPolicy`; compared live by the lifecycle surface |
| §8 FLASHBULB_PROTECTION_MS | thirty days | reproduced | `TestLifecycleConstantsAreRustsNumbers`, `TestFlashbulbPromotionOverridesTheRequestedPolicy` |
| §8 auto_discovery_threshold | `0.7` | reproduced | `TestDiscoveryDefaultsAreRustsNumbers`. The number is Rust's; the quantity compared against it is Go's cosine, not Rust's `1/(1+d)`, which is §II.10 row 17 |
| §8 auto_discovery_top_k | `5` | reproduced | `TestDiscoveryDefaultsAreRustsNumbers` |
| §8 rrf_k | `60` | reproduced | `TestFixedConstantsAreTheDefaults`, `TestFusionMatchesTheRustRanking` |
| §8 widen_max_factor | `32` | reproduced | `TestFixedConstantsAreTheDefaults`, `TestTruncatedIsSetAtTheWideningBound` |
| §8 list_max_factor | `128`, deliberately larger than the search bound | reproduced | `TestFixedConstantsAreTheDefaults`, `TestAListingWidensToItsOwnBound`, `TestAListingWidensFurtherThanASearch` — **changed in Phase 13**: Go shared the search's 32 until `4fb13c9` |
| §8 DEFAULT_RUN_BUDGET | `10_000` | reproduced | `TestLifecycleConstantsAreRustsNumbers` |
| §8 SWEEP_EFFORT_FACTOR | `8` | reproduced | `TestLifecycleConstantsAreRustsNumbers` |

## Found in Phase 13, beyond the baseline

The baseline records what a reader of the Rust source could see. Running the
reference showed three more behaviours a parity claim has to account for. Two
changed Go, and one is a divergence the plan now carries.

| Behaviour | Status | Evidence |
|---|---|---|
| A TTL belongs to a short-term memory only: a memory that ends up long-term keeps none, and a TTL under another policy never expires it | reproduced | `TestAMemoryThatIsNotShortTermKeepsNoTTL`, `TestATTLOnARecordThatIsNotShortTermIsInert`; found by the harness's lifecycle surface. **Changed in Phase 13** (`89fd5d8`): a flashbulb memory kept its requested TTL and was archived when it ran out |
| A graph walk reaches every node within its depth bound | reproduced | `TestTraversalReachesEveryNodeWithinMaxDepth`, `TestTraversalMatchesAHopBoundedReference`; compared live by the harness's graph surface. **Changed in Phase 13** (`fe51481`): a node reached strongly at two hops and weakly at one was expanded only at two |
| A short-term memory created without a TTL has none, where Rust's gets one hour | diverged | §II.10 row 18, added in Phase 13. Phase 10 had recorded this as a copy of Rust; the frozen image returns `ttl: 3600` (`memory_manager.rs:149`). Kept with the user as a divergence |
