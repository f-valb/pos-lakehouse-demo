#!/usr/bin/env bash
# Manual approval gate for private repos on the GitHub Free plan (environment "required reviewers"
# needs Pro/Team there). Opens an issue, then waits until an approver comments "approve" (deploy)
# or "deny" (fail). Times out after APPROVAL_TIMEOUT_MIN minutes (fail = no deploy).
#
# Env: GH_TOKEN (needs issues: write), GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_SHA, GITHUB_SERVER_URL,
#      APPROVERS (comma-separated logins), APPROVAL_TIMEOUT_MIN (default 60), POLL_SECONDS (default 20)
set -euo pipefail
: "${APPROVERS:?set APPROVERS}"
TIMEOUT_MIN=${APPROVAL_TIMEOUT_MIN:-60}
POLL=${POLL_SECONDS:-20}
RUN_URL="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"
MENTIONS=$(echo "$APPROVERS" | tr ',' '\n' | sed 's/^ *//; s/^/@/' | paste -sd' ' -)

BODY=$(cat <<EOF
Production deploy of \`${GITHUB_SHA:0:7}\` is waiting for approval.

Staging deploy and integration tests passed: $RUN_URL

$MENTIONS: comment **approve** to deploy to prod, or **deny** to stop it.
Times out (no deploy) after $TIMEOUT_MIN minutes.
EOF
)
ISSUE_URL=$(gh issue create --repo "$GITHUB_REPOSITORY" --title "Approve prod deploy ${GITHUB_SHA:0:7}" --body "$BODY")
ISSUE=${ISSUE_URL##*/}
echo "Waiting for approval on $ISSUE_URL (approvers: $APPROVERS)"

deadline=$(( $(date +%s) + TIMEOUT_MIN * 60 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  # Latest decisive comment from an allowed approver wins.
  decision=$(gh api "repos/$GITHUB_REPOSITORY/issues/$ISSUE/comments" --paginate \
    --jq '.[] | [.user.login, (.body | ascii_downcase | gsub("^\\s+|\\s+$"; ""))] | @tsv' |
    awk -F'\t' -v approvers="$APPROVERS" '
      BEGIN { n = split(approvers, a, ","); for (i = 1; i <= n; i++) { gsub(/ /, "", a[i]); ok[a[i]] = 1 } }
      ok[$1] && ($2 ~ /^(approve|approved|lgtm)[.!]*$/) { d = "approve:" $1 }
      ok[$1] && ($2 ~ /^(deny|denied|reject)[.!]*$/)    { d = "deny:" $1 }
      END { print d }')
  case "$decision" in
    approve:*)
      gh issue close "$ISSUE" --repo "$GITHUB_REPOSITORY" --comment "Approved by @${decision#approve:}. Deploying to prod: $RUN_URL"
      echo "Approved by ${decision#approve:}"; exit 0 ;;
    deny:*)
      gh issue close "$ISSUE" --repo "$GITHUB_REPOSITORY" --reason "not planned" --comment "Denied by @${decision#deny:}. Prod deploy cancelled."
      echo "Denied by ${decision#deny:}"; exit 1 ;;
  esac
  sleep "$POLL"
done
gh issue close "$ISSUE" --repo "$GITHUB_REPOSITORY" --reason "not planned" --comment "No approval within $TIMEOUT_MIN minutes. Prod deploy cancelled."
echo "Timed out"; exit 1
