use std::sync::Arc;

use axum::Router;
use tower_governor::{
    governor::GovernorConfigBuilder,
    key_extractor::GlobalKeyExtractor,
    GovernorLayer,
};

/// Apply a global (not per-IP) rate-limit layer to `router`, enforcing `rps`
/// requests per second with a burst allowance of `burst`. Returns `router`
/// unchanged when `rps == 0` (rate limiting disabled).
///
/// Uses `GlobalKeyExtractor` so the limit applies to all callers combined —
/// correct for remem-server which sits behind a Docker bridge where all
/// requests share the same peer address.
pub fn apply_if_configured(router: Router, rps: u32, burst: u32) -> Router {
    if rps == 0 {
        return router;
    }
    // period_ms = milliseconds between token replenishments → ≈ rps req/s
    let period_ms = 1000u64 / (rps as u64).max(1);
    let conf = Arc::new(
        GovernorConfigBuilder::default()
            .key_extractor(GlobalKeyExtractor)
            .per_millisecond(period_ms)
            .burst_size(burst.max(1))
            .finish()
            .expect("invalid rate limit config"),
    );
    tracing::info!(rps, burst, "rate limiting enabled");
    router.layer(GovernorLayer { config: conf })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_when_rps_zero() {
        let router: Router = Router::new();
        // should not panic and return a router unchanged
        let _ = apply_if_configured(router, 0, 50);
    }

    #[test]
    fn applies_layer_when_rps_positive() {
        let router: Router = Router::new();
        let _ = apply_if_configured(router, 100, 50);
    }

    #[test]
    fn clamps_burst_zero_to_one() {
        let router: Router = Router::new();
        let _ = apply_if_configured(router, 100, 0);
    }
}
