#!/usr/bin/env python3
"""Generate 100,000 test memories for remem stress testing / benchmarking.

Usage:
    pip install httpx
    python scripts/generate_memories.py [--url URL] [--count N] [--concurrency C]

Defaults:
    --url         http://localhost:4545/api/v1/memories
    --count       100000
    --concurrency 100
"""

import argparse
import asyncio
from collections import Counter
import os
from pathlib import Path
import random
import sys
import time
from typing import Any

try:
    import httpx
except ImportError:
    print("httpx not installed. Run: pip install httpx")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Content templates
# ---------------------------------------------------------------------------

SUBJECTS = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Heidi",
    "Ivan", "Judy", "Kevin", "Laura", "Mallory", "Nancy", "Oscar", "Peggy",
    "Quinn", "Romeo", "Sybil", "Trent", "Ursula", "Victor", "Wendy", "Xander",
    "Yvonne", "Zach",
]

VERBS = [
    "visited", "called", "emailed", "reminded", "told", "learned", "discovered",
    "forgot", "remembered", "noted", "observed", "reported", "discussed",
    "mentioned", "shared", "suggested", "recommended", "warned", "confirmed",
    "denied", "agreed", "disagreed", "explained", "described", "summarised",
]

TOPICS = [
    "the quarterly budget review",
    "a new product launch strategy",
    "team onboarding procedures",
    "the security audit findings",
    "database performance issues",
    "customer feedback from last sprint",
    "the deployment pipeline failure",
    "a critical bug in production",
    "the upcoming conference presentation",
    "contract renewal deadlines",
    "the marketing campaign results",
    "user research findings",
    "infrastructure cost optimisation",
    "the API rate limiting policy",
    "GDPR compliance requirements",
    "the new hire's first day",
    "sprint retrospective action items",
    "the architecture decision record",
    "service level agreement breaches",
    "a memorable offsite team dinner",
    "the code review backlog",
    "the CEO's strategic priorities",
    "on-call rotation schedule changes",
    "a production incident postmortem",
    "the machine learning model accuracy drop",
    "data pipeline latency spikes",
    "the frontend redesign mockups",
    "mobile app crash reports",
    "the vendor evaluation matrix",
    "employee satisfaction survey results",
]

DETAILS = [
    "This needs follow-up before end of week.",
    "Action required by Friday.",
    "Flagged as high priority.",
    "Low urgency but worth tracking.",
    "Discussed in the weekly sync.",
    "Escalated to senior leadership.",
    "Requires cross-team coordination.",
    "Expected resolution in Q3.",
    "Blocked by external dependency.",
    "Resolved after three days of debugging.",
    "Documented in the internal wiki.",
    "Shared in the #general Slack channel.",
    "Part of the ongoing migration project.",
    "Tied to OKR key result 2.",
    "Budget already approved.",
    "Pending legal sign-off.",
    "Stakeholders notified.",
    "Root cause still unknown.",
    "Temporary workaround in place.",
    "Permanent fix scheduled for next release.",
    "Needs regression testing.",
    "Automated tests added.",
    "Manual QA completed.",
    "Rollback plan ready.",
    "Monitoring alert configured.",
]

TAGS_POOL = [
    "work", "personal", "urgent", "follow-up", "project", "bug", "feature",
    "meeting", "decision", "learning", "idea", "reminder", "incident",
    "people", "finance", "legal", "technical", "design", "research",
    "infrastructure", "security", "product", "marketing", "ops", "strategy",
    "hr", "vendor", "customer", "internal", "external",
]

SOURCES = [
    "slack", "email", "meeting_notes", "jira", "notion", "github",
    "linear", "confluence", "phone_call", "manual_entry", "api", "webhook",
]

MEMORY_TYPES = ["short_term", "long_term"]


def default_api_key() -> str:
    """Read the API key from the environment or the repository's .env file."""
    if api_key := os.environ.get("REMEM_API_KEY", "").strip():
        return api_key

    env_path = Path(__file__).resolve().parent.parent / ".env"
    try:
        for line in env_path.read_text().splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() == "REMEM_API_KEY":
                return value.strip().strip("'\"")
    except FileNotFoundError:
        pass
    return ""


def random_content() -> str:
    subject = random.choice(SUBJECTS)
    verb = random.choice(VERBS)
    topic = random.choice(TOPICS)
    detail = random.choice(DETAILS)
    return f"{subject} {verb} about {topic}. {detail}"


def random_memory() -> dict[str, Any]:
    memory_type = random.choices(
        MEMORY_TYPES, weights=[0.4, 0.6]  # slightly more long_term
    )[0]

    tags = random.sample(TAGS_POOL, k=random.randint(1, 4))
    importance = round(random.betavariate(2, 3), 3)  # skewed towards lower values
    source = random.choice(SOURCES)

    payload: dict[str, Any] = {
        "content": random_content(),
        "memory_type": memory_type,
        "tags": tags,
        "importance": importance,
        "source": source,
    }

    if memory_type == "short_term":
        # TTL between 1 hour and 7 days
        payload["ttl"] = random.randint(3600, 604800)

    return payload


# ---------------------------------------------------------------------------
# Async worker
# ---------------------------------------------------------------------------

async def post_memory(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    counter: list[int],  # mutable int via list
    errors: Counter[str],
    error_samples: dict[str, str],
    total: int,
    max_retries: int,
) -> None:
    async with semaphore:
        payload = random_memory()
        error_key: str | None = None
        error_detail = ""
        for attempt in range(max_retries + 1):
            try:
                response = await client.post(url, json=payload, timeout=30.0)
                if response.status_code in (200, 201):
                    error_key = None
                    break

                error_key = f"HTTP {response.status_code}"
                error_detail = response.text[:500]
                retryable = response.status_code == 429 or response.status_code in (502, 503, 504)
                if not retryable or attempt == max_retries:
                    break

                retry_after = response.headers.get("retry-after")
                delay = float(retry_after) if retry_after else min(0.25 * (2 ** attempt), 5.0)
                await asyncio.sleep(delay)
            except httpx.ConnectError as exc:
                error_key = type(exc).__name__
                error_detail = str(exc)
                if attempt == max_retries:
                    break
                await asyncio.sleep(min(0.25 * (2 ** attempt), 5.0))
            except Exception as exc:
                # Do not retry ambiguous failures such as read timeouts: the
                # server may already have committed the memory.
                error_key = type(exc).__name__
                error_detail = str(exc)
                break

        if error_key is not None:
            errors[error_key] += 1
            error_samples.setdefault(error_key, error_detail)

        counter[0] += 1
        done = counter[0]
        if done % 1000 == 0 or done == total:
            pct = done / total * 100
            error_count = sum(errors.values())
            err_rate = error_count / done * 100
            print(
                f"\r  {done:>7}/{total}  ({pct:5.1f}%)  errors: {error_count} ({err_rate:.1f}%)",
                end="",
                flush=True,
            )


async def run(url: str, count: int, concurrency: int, api_key: str, max_retries: int) -> None:
    semaphore = asyncio.Semaphore(concurrency)
    counter: list[int] = [0]
    errors: Counter[str] = Counter()
    error_samples: dict[str, str] = {}
    headers = {"X-API-Key": api_key} if api_key else {}

    print(f"Generating {count:,} memories -> {url}")
    print(f"Concurrency: {concurrency}")
    print()

    start = time.perf_counter()

    async with httpx.AsyncClient(headers=headers) as client:
        health_url = url.rsplit("/memories", 1)[0] + "/health"
        try:
            health = await client.get(health_url, timeout=5.0)
            health.raise_for_status()
        except Exception as exc:
            print(f"Cannot reach a healthy remem-server at {health_url}: {exc}", file=sys.stderr)
            print("Start remem-server and verify port 4545 before generating memories.", file=sys.stderr)
            raise SystemExit(2) from exc

        try:
            auth_check = await client.get(url, params={"limit": 1}, timeout=5.0)
            auth_check.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text[:500]
            print(
                f"remem-server rejected the API preflight: HTTP {exc.response.status_code} — {detail}",
                file=sys.stderr,
            )
            print("Set REMEM_API_KEY in .env, export it, or pass --api-key.", file=sys.stderr)
            raise SystemExit(2) from exc

        tasks = [
            post_memory(
                client, url, semaphore, counter, errors, error_samples, count, max_retries
            )
            for _ in range(count)
        ]
        await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start
    rps = count / elapsed

    print()  # newline after progress line
    print()
    print(f"Done in {elapsed:.1f}s  ({rps:.0f} req/s)")
    error_count = sum(errors.values())
    print(f"  Successes : {count - error_count:,}")
    print(f"  Errors    : {error_count:,}")
    for kind, amount in errors.most_common():
        detail = error_samples.get(kind) or "no response detail"
        print(f"    {kind}: {amount:,} — {detail}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate test memories for remem stress testing"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:4545/api/v1/memories",
        help="remem-server memories endpoint (default: http://localhost:4545/api/v1/memories)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100_000,
        help="Number of memories to generate (default: 100000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=100,
        help="Maximum concurrent requests (default: 100)",
    )
    parser.add_argument(
        "--api-key",
        default=default_api_key(),
        help="API key (default: REMEM_API_KEY environment variable or repository .env)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retries for 429/502/503/504 and connection failures (default: 5)",
    )
    args = parser.parse_args()

    asyncio.run(run(args.url, args.count, args.concurrency, args.api_key, args.max_retries))


if __name__ == "__main__":
    main()
