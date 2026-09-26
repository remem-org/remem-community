#!/usr/bin/env bash
# import-community-pr.sh — Apply a community repo PR to the dev repo.
#
# Usage:
#   ./scripts/import-community-pr.sh <PR-number> [--repo <owner/repo>] [--dry-run]
#
# Examples:
#   ./scripts/import-community-pr.sh 42
#   ./scripts/import-community-pr.sh 42 --repo myorg/remem-community
#   ./scripts/import-community-pr.sh 42 --dry-run
#
# The script:
#   1. Fetches the PR diff from the community repo via gh CLI
#   2. Applies it to the current working tree with git apply
#   3. Creates a commit crediting the original PR author
#
# Requirements: git, gh (GitHub CLI, authenticated)

set -euo pipefail

# ── Args ──────────────────────────────────────────────────────────────────────

PR_NUMBER="${1:-}"
COMMUNITY_REPO="remem-community"   # default: will be resolved from gh remote
DRY_RUN=false

if [[ -z "$PR_NUMBER" ]]; then
    echo "Usage: $0 <PR-number> [--repo <owner/repo>] [--dry-run]"
    exit 1
fi

shift
while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo)
            COMMUNITY_REPO="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# ── Resolve repo ──────────────────────────────────────────────────────────────

# If COMMUNITY_REPO has no slash, try to resolve org from the dev repo's remote
if [[ "$COMMUNITY_REPO" != */* ]]; then
    ORIGIN_URL=$(git remote get-url origin 2>/dev/null || true)
    if [[ -n "$ORIGIN_URL" ]]; then
        ORG=$(echo "$ORIGIN_URL" | sed -E 's|.*[:/]([^/]+)/[^/]+\.git.*|\1|')
        COMMUNITY_REPO="$ORG/$COMMUNITY_REPO"
    fi
fi

echo "==> Importing community PR #$PR_NUMBER from $COMMUNITY_REPO"

# ── Fetch PR metadata ─────────────────────────────────────────────────────────

echo "==> Fetching PR metadata..."

PR_TITLE=$(gh pr view "$PR_NUMBER" --repo "$COMMUNITY_REPO" --json title -q '.title')
PR_AUTHOR=$(gh pr view "$PR_NUMBER" --repo "$COMMUNITY_REPO" --json author -q '.author.login')
PR_AUTHOR_NAME=$(gh pr view "$PR_NUMBER" --repo "$COMMUNITY_REPO" --json author -q '.author.name // .author.login')
PR_URL=$(gh pr view "$PR_NUMBER" --repo "$COMMUNITY_REPO" --json url -q '.url')
PR_BODY=$(gh pr view "$PR_NUMBER" --repo "$COMMUNITY_REPO" --json body -q '.body // ""')

echo "  Title:  $PR_TITLE"
echo "  Author: $PR_AUTHOR_NAME (@$PR_AUTHOR)"
echo "  URL:    $PR_URL"

# ── Safety checks ─────────────────────────────────────────────────────────────

# Ensure working tree is clean before applying
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo ""
    echo "Error: working tree has uncommitted changes. Stash or commit them first."
    exit 1
fi

# ── Fetch and apply diff ──────────────────────────────────────────────────────

echo "==> Fetching PR diff..."
DIFF=$(gh pr diff "$PR_NUMBER" --repo "$COMMUNITY_REPO")

if [[ -z "$DIFF" ]]; then
    echo "Error: PR diff is empty. Is the PR already merged?"
    exit 1
fi

if [[ "$DRY_RUN" == true ]]; then
    echo ""
    echo "--- DRY RUN: patch that would be applied ---"
    echo "$DIFF"
    echo "--- END DRY RUN ---"
    echo ""
    echo "Run without --dry-run to apply."
    exit 0
fi

echo "==> Applying patch..."
echo "$DIFF" | git apply --index -

# ── Commit with attribution ────────────────────────────────────────────────────

echo "==> Creating commit..."

# Resolve author email (best-effort: gh API doesn't always expose it)
AUTHOR_EMAIL=$(gh api "users/$PR_AUTHOR" -q '.email // empty' 2>/dev/null || true)
if [[ -z "$AUTHOR_EMAIL" ]]; then
    AUTHOR_EMAIL="${PR_AUTHOR}@users.noreply.github.com"
fi

COMMIT_MSG="$(cat <<MSG
${PR_TITLE}

Imported from community PR: ${PR_URL}

${PR_BODY}

Co-authored-by: ${PR_AUTHOR_NAME} <${AUTHOR_EMAIL}>
MSG
)"

git commit --author="${PR_AUTHOR_NAME} <${AUTHOR_EMAIL}>" -m "$COMMIT_MSG"

echo ""
echo "Done! Commit created:"
git log -1 --oneline
echo ""
echo "Review the changes, then push to include in the next release."
