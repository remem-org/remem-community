use std::sync::Arc;

use axum::Router;
use tower_governor::{
    governor::GovernorConfigBuilder,
    key_extractor::{PeerIpKeyExtractor, SmartIpKeyExtractor},
    GovernorLayer,
};

/// Apply a per-client rate-limit layer to `router`, enforcing `rps` requests
/// per second with a burst allowance of `burst`. Returns `router` unchanged
/// when `rps == 0` (rate limiting disabled).
///
/// Keys by TCP peer IP (`PeerIpKeyExtractor`) by default — safe whether
/// remem-server is exposed directly or sits behind an unconfigured proxy
/// (worst case there is one shared bucket per proxy hop, an availability
/// question, not a bypass). When `trust_proxy_headers` is true, keys by
/// `X-Forwarded-For`/`Forwarded` instead (`SmartIpKeyExtractor`) — only
/// correct when a trusted reverse proxy strips/overwrites any
/// client-supplied value for these headers before forwarding. Trusting them
/// unconditionally would let any direct client spoof a distinct key per
/// request and bypass the limit entirely.
///
/// Requires the server to be served via
/// `into_make_service_with_connect_info::<SocketAddr>()` (see `main.rs`) —
/// both extractors read the peer address from the `ConnectInfo` request
/// extension; without it every request fails key extraction.
pub fn apply_if_configured(
    router: Router,
    rps: u32,
    burst: u32,
    trust_proxy_headers: bool,
) -> Router {
    if rps == 0 {
        return router;
    }
    // period_ms = milliseconds between token replenishments → ≈ rps req/s
    let period_ms = 1000u64 / (rps as u64).max(1);
    let burst = burst.max(1);

    if trust_proxy_headers {
        let conf = Arc::new(
            GovernorConfigBuilder::default()
                .key_extractor(SmartIpKeyExtractor)
                .per_millisecond(period_ms)
                .burst_size(burst)
                .finish()
                .expect("invalid rate limit config"),
        );
        tracing::info!(
            rps,
            burst,
            key_extractor = "smart_ip",
            "rate limiting enabled (trusting X-Forwarded-For/Forwarded)"
        );
        router.layer(GovernorLayer { config: conf })
    } else {
        let conf = Arc::new(
            GovernorConfigBuilder::default()
                .key_extractor(PeerIpKeyExtractor)
                .per_millisecond(period_ms)
                .burst_size(burst)
                .finish()
                .expect("invalid rate limit config"),
        );
        tracing::info!(rps, burst, key_extractor = "peer_ip", "rate limiting enabled");
        router.layer(GovernorLayer { config: conf })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_when_rps_zero() {
        let router: Router = Router::new();
        // should not panic and return a router unchanged
        let _ = apply_if_configured(router, 0, 50, false);
    }

    #[test]
    fn applies_peer_ip_layer_when_rps_positive() {
        let router: Router = Router::new();
        let _ = apply_if_configured(router, 100, 50, false);
    }

    #[test]
    fn applies_smart_ip_layer_when_trusted() {
        let router: Router = Router::new();
        let _ = apply_if_configured(router, 100, 50, true);
    }

    #[test]
    fn clamps_burst_zero_to_one() {
        let router: Router = Router::new();
        let _ = apply_if_configured(router, 100, 0, false);
    }
}
