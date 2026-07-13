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
    errors: list[int],
    total: int,
) -> None:
    async with semaphore:
        payload = random_memory()
        try:
            response = await client.post(url, json=payload, timeout=30.0)
            if response.status_code not in (200, 201):
                errors[0] += 1
        except Exception:
            errors[0] += 1
        finally:
            counter[0] += 1
            done = counter[0]
            if done % 1000 == 0 or done == total:
                pct = done / total * 100
                err_rate = errors[0] / done * 100
                print(
                    f"\r  {done:>7}/{total}  ({pct:5.1f}%)  errors: {errors[0]} ({err_rate:.1f}%)",
                    end="",
                    flush=True,
                )


async def run(url: str, count: int, concurrency: int) -> None:
    semaphore = asyncio.Semaphore(concurrency)
    counter: list[int] = [0]
    errors: list[int] = [0]

    print(f"Generating {count:,} memories -> {url}")
    print(f"Concurrency: {concurrency}")
    print()

    start = time.perf_counter()

    async with httpx.AsyncClient() as client:
        tasks = [
            post_memory(client, url, semaphore, counter, errors, count)
            for _ in range(count)
        ]
        await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start
    rps = count / elapsed

    print()  # newline after progress line
    print()
    print(f"Done in {elapsed:.1f}s  ({rps:.0f} req/s)")
    print(f"  Successes : {count - errors[0]:,}")
    print(f"  Errors    : {errors[0]:,}")


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
    args = parser.parse_args()

    asyncio.run(run(args.url, args.count, args.concurrency))


if __name__ == "__main__":
    main()
