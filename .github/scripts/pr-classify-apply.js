//
// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
// SPDX-License-Identifier: Apache-2.0
//
// Applies the PR triage that the read-only classify job decided, for
// .github/workflows/classify-pr.yml: the area, severity, issue-link and triage labels, and one
// review comment whose first line is a machine-readable marker.
//
// This is the boundary between untrusted content and GitHub writes. The model read an untrusted
// PR, so nothing it returns reaches the API unchecked:
// - the verdict and severity must be one of the known levels, or they are dropped;
// - an area must already be a repository label and must not be a process, owned or reserved one;
// - the issue link is computed here from GraphQL, never taken from the model;
// - the marker line is written here, and the model's review is scrubbed of anything that could
//   forge a marker, ping people, or publish a secret before it is posted;
// - nothing is written when the PR head moved since the model looked at it.
//
// review-pr and resolve-issues trust the marker
//   ^<!-- classify-pr sha=([0-9a-f]{40}) verdict=(lgtm|needs-changes|unsafe|none) -->$
// so `markerLine` must keep producing exactly that shape.
//
// Tested by .github/scripts/tests/test-pr-workflow-scripts.js.

const {
  logSafe,
  SEVERITY_LEVELS,
  SEVERITY_BY_NAME,
  TRIAGE_LEVELS,
  ISSUE_LINK_LABELS,
  PR_MARKER_PREFIX,
  PR_REVIEW_LIMIT,
  RESERVED_LABEL,
  NON_MODULE_LABELS,
} = require("./issue-workflow-config.js");

const VERDICTS = new Set(TRIAGE_LEVELS.map((l) => l.verdict));
const MAX_AREAS = 5;
const SECURITY_REASON_LIMIT = 2000;
const DO_NOT_MERGE = "do not merge";

// review-pr's own labels. They are never areas, and this script never removes them.
const REVIEW_PR_LABELS = ["pr_approved", DO_NOT_MERGE, "security-risk", "needs-maintainer", "needs-changes"];

// Labels this workflow owns and creates on first use, with the colours the tests pin.
const OWNED = new Map(
  [...TRIAGE_LEVELS, ...SEVERITY_LEVELS, ISSUE_LINK_LABELS.has, ISSUE_LINK_LABELS.none].map((l) => [
    l.name.toLowerCase(),
    l,
  ]),
);

const NOT_AN_AREA = new Set(
  [...NON_MODULE_LABELS, RESERVED_LABEL, ...REVIEW_PR_LABELS, ...OWNED.keys()].map((n) => n.toLowerCase()),
);
const isArea = (key) => !NOT_AN_AREA.has(key) && !key.startsWith("severity:") && !key.startsWith("triage:");

/**
 * The classify job's structured output, validated. Anything that is not a JSON object with a
 * known verdict and a string review is `{verdict: 'none'}`: a wrong verdict is worse than none.
 * An unknown severity is dropped to null rather than guessed.
 */
const parseDecision = (raw) => {
  const none = { verdict: "none" };
  if (typeof raw !== "string" || raw.trim() === "") return none;
  let d;
  try {
    d = JSON.parse(raw);
    // Tolerate one level of double encoding; a job output is a string either way.
    if (typeof d === "string") d = JSON.parse(d);
  } catch {
    return none;
  }
  if (d === null || typeof d !== "object" || Array.isArray(d)) return none;
  if (typeof d.verdict !== "string" || !VERDICTS.has(d.verdict)) return none;
  if (typeof d.review !== "string" || d.review.trim() === "") return none;
  const severity =
    typeof d.severity === "string" ? (SEVERITY_BY_NAME.get(d.severity.toLowerCase())?.name ?? null) : null;
  const areas = Array.isArray(d.areas) ? d.areas.filter((a) => typeof a === "string") : [];
  const securityReason = typeof d.security_reason === "string" ? d.security_reason : null;
  return { verdict: d.verdict, severity, areas, review: d.review, securityReason };
};

// Token shapes that are always secrets, whatever surrounds them.
const TOKEN_PATTERNS = [
  /\bgh[pousr]_[A-Za-z0-9]{36,}/g,
  /\bgithub_pat_[A-Za-z0-9_]{20,}/g,
  /\bsk-ant-[A-Za-z0-9_-]{10,}/g,
  /\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}/g,
];
// Bounded by non-hex rather than \b, so hex glued to a word character (`0x…`, `secret_…`) is
// still one run.
const HEX_RUN = /(?<![0-9a-fA-F])[0-9a-fA-F]{40,}(?![0-9a-fA-F])/g;
const B64_RUN = /[A-Za-z0-9+/=_-]{40,}/g;
// Private-use characters stand in for the commit SHAs that are kept, so the base64 pass cannot
// swallow them into a longer run. Any already in the text are stripped first.
const SENTINEL_CHARS = /[-]/g;
const KEPT = /(\d+)/g;

const entropy = (s) => {
  const counts = new Map();
  for (const ch of s) counts.set(ch, (counts.get(ch) ?? 0) + 1);
  let h = 0;
  for (const c of counts.values()) {
    const p = c / s.length;
    h -= p * Math.log2(p);
  }
  return h;
};

// A 40+ character run of base64 alphabet is redacted only when it looks random: both cases and
// high entropy. Source paths and long identifiers share the alphabet but score lower (repo
// paths measured 4.05-4.27); a random 40-char base64 string scores under 4.4 about 0.3% of the
// time. Redacting every long path would make reviews unreadable.
const MIN_ENTROPY = 4.4;
const looksRandom = (run) => /[A-Z]/.test(run) && /[a-z]/.test(run) && entropy(run) >= MIN_ENTROPY;

/**
 * The model's review text, made safe to publish: secrets redacted, HTML comment openers (and so
 * any forged marker) made visible text, @-mentions defanged, and the length capped.
 */
const sanitizeReview = (text, prShas) => {
  let redactions = 0;
  const redact = () => {
    redactions++;
    return "[redacted]";
  };
  let out = String(text ?? "").replace(SENTINEL_CHARS, "");
  for (const re of TOKEN_PATTERNS) out = out.replace(re, redact);

  // A commit SHA quoted from this PR is kept; any other long hex run could be a key.
  const kept = [];
  out = out.replace(HEX_RUN, (run) => {
    if (run.length === 40 && prShas.has(run.toLowerCase())) {
      kept.push(run);
      return `${kept.length - 1}`;
    }
    return redact();
  });
  out = out.replace(B64_RUN, (run) => (looksRandom(run) ? redact() : run));
  out = out.replace(KEPT, (_, i) => kept[Number(i)]);

  // Every HTML comment opener becomes visible text. That neutralises a forged
  // `<!-- classify-pr …` marker in any spacing or case, and hides nothing from the reader.
  out = out.replace(/<!--/g, "&lt;!--");
  // A zero-width space after @ stops GitHub from notifying the named user or team.
  out = out.replace(/@(?=[A-Za-z0-9])/g, "@​");
  return { text: out.slice(0, PR_REVIEW_LIMIT), redactions };
};

/** The first line of the comment, the one review-pr and resolve-issues parse. */
const markerLine = (sha, verdict) => `${PR_MARKER_PREFIX} sha=${sha} verdict=${verdict} -->`;

const CLOSING_ISSUES_QUERY = `query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      closingIssuesReferences(first: 20) { nodes { number repository { nameWithOwner } } }
    }
  }
}`;

const apply = async ({ github, context, core }) => {
  const prNumber = Number(process.env.PR_NUMBER);
  const headSha = (process.env.HEAD_SHA ?? "").trim();
  const dryRun = process.env.DRY_RUN === "true";
  const { owner, repo } = context.repo;
  const decision = parseDecision(process.env.STRUCTURED_OUTPUT);

  const state = {
    verdict: decision.verdict,
    severity: decision.severity ?? "-",
    areas: "-",
    link: "-",
    redacted: 0,
  };
  const audit = (action) =>
    core.info(
      logSafe(
        `classify-pr: pr=${Number.isInteger(prNumber) ? prNumber : "-"}, verdict=${state.verdict}, ` +
          `severity=${state.severity}, areas=${state.areas}, link=${state.link}, ` +
          `redacted=${state.redacted}, action=${action}`,
        1000,
      ),
    );

  if (!Number.isInteger(prNumber) || prNumber <= 0 || !/^[0-9a-f]{40}$/.test(headSha)) {
    core.warning("classify-pr: PR_NUMBER or HEAD_SHA is missing or malformed");
    audit("errored");
    return;
  }

  // A newer push owns the new head, and its own run will label and comment.
  try {
    const { data: pr } = await github.rest.pulls.get({ owner, repo, pull_number: prNumber });
    if (pr?.head?.sha !== headSha) {
      audit("moved");
      return;
    }
  } catch (err) {
    core.warning(`classify-pr: pulls.get failed: ${logSafe(err.message, 200)}`);
    audit("errored");
    return;
  }

  let repoLabels;
  let prLabels;
  try {
    repoLabels = await github.paginate(github.rest.issues.listLabelsForRepo, { owner, repo, per_page: 100 });
    prLabels = await github.paginate(github.rest.issues.listLabelsOnIssue, {
      owner,
      repo,
      issue_number: prNumber,
      per_page: 100,
    });
  } catch (err) {
    // The head is still ours, so post a `verdict=none` marker without touching labels: the
    // engines then report no verdict at once instead of waiting out their timeout.
    core.warning(`classify-pr: listing labels failed: ${logSafe(err.message, 200)}`);
    state.verdict = "none";
    state.severity = "-";
    if (dryRun) {
      audit("errored");
      return;
    }
    const body = [
      markerLine(headSha, "none"),
      "**Triage**: no verdict",
      "",
      "Labels could not be read, so no labels were applied. This PR needs a manual review.",
    ].join("\n");
    try {
      await github.rest.issues.createComment({ owner, repo, issue_number: prNumber, body });
    } catch (commentErr) {
      core.warning(`classify-pr: createComment failed: ${logSafe(commentErr.message, 200)}`);
    }
    audit("errored");
    return;
  }
  const canonical = new Map(repoLabels.map((l) => [l.name.toLowerCase(), l.name]));

  // Issue link: computed from GitHub's own closing references, counting only this repository.
  let link = null;
  try {
    const res = await github.graphql(CLOSING_ISSUES_QUERY, { owner, repo, number: prNumber });
    const nodes = res?.repository?.pullRequest?.closingIssuesReferences?.nodes ?? [];
    const self = `${owner}/${repo}`.toLowerCase();
    const linked = nodes.some((n) => n?.repository?.nameWithOwner?.toLowerCase() === self);
    link = linked ? ISSUE_LINK_LABELS.has : ISSUE_LINK_LABELS.none;
  } catch (err) {
    core.warning(`classify-pr: closing issues lookup failed: ${logSafe(err.message, 200)}`);
  }
  state.link = link?.name ?? "unknown";

  // The PR's own commit SHAs may be quoted in the review; everything else hex-shaped is redacted.
  let prShas = new Set();
  try {
    const commits = await github.paginate(github.rest.pulls.listCommits, {
      owner,
      repo,
      pull_number: prNumber,
      per_page: 100,
    });
    prShas = new Set(commits.map((c) => String(c.sha).toLowerCase()));
  } catch (err) {
    core.warning(`classify-pr: listCommits failed: ${logSafe(err.message, 200)}`);
  }
  prShas.add(headSha);

  // Areas: existing, non-process labels only, canonical case, deduplicated, at most five.
  const areas = [];
  if (decision.verdict !== "none") {
    for (const requested of decision.areas) {
      const key = requested.trim().toLowerCase();
      if (!canonical.has(key) || !isArea(key)) continue;
      const name = canonical.get(key);
      if (!areas.includes(name)) areas.push(name);
      if (areas.length === MAX_AREAS) break;
    }
  }
  state.areas = areas.join("|") || "-";

  const triage = TRIAGE_LEVELS.find((l) => l.verdict === decision.verdict) ?? null;
  const severity = decision.severity ? SEVERITY_BY_NAME.get(decision.severity.toLowerCase()) : null;

  // Owned labels to apply, in the order they appear in the comment.
  const ownedToAdd = [triage, severity, link].filter((l) => l);
  const toAdd = [...ownedToAdd.map((l) => canonical.get(l.name.toLowerCase()) ?? l.name), ...areas];
  if (decision.verdict === "unsafe") toAdd.push(canonical.get(DO_NOT_MERGE) ?? DO_NOT_MERGE);

  // Stale labels of the same families, removed only when actually on the PR. `do not merge`
  // is in none of these families, so nothing here can ever remove it.
  const stale = new Set();
  for (const l of TRIAGE_LEVELS) if (l !== triage) stale.add(l.name.toLowerCase());
  if (severity) for (const l of SEVERITY_LEVELS) if (l !== severity) stale.add(l.name.toLowerCase());
  if (link) for (const l of Object.values(ISSUE_LINK_LABELS)) if (l !== link) stale.add(l.name.toLowerCase());
  const toRemove = prLabels.map((l) => l.name).filter((n) => stale.has(n.toLowerCase()));

  const toCreate = ownedToAdd.filter((l) => !canonical.has(l.name.toLowerCase()));

  // The comment. The marker is ours; everything the model wrote goes through sanitizeReview.
  const lines = [markerLine(headSha, decision.verdict)];
  const linkText = link ? `\`${link.name}\`` : "issue link: unknown";
  if (decision.verdict === "none") {
    lines.push(`**Triage**: no verdict · ${linkText}`, "");
    lines.push(
      "The classifier returned no usable output, so no `triage:*` label was applied. " +
        "This PR needs a manual review.",
    );
  } else {
    const sev = severity ? `\`${severity.name}\`` : "severity: none";
    const areaText = areas.length ? areas.map((a) => `\`${a}\``).join(", ") : "none";
    lines.push(`**Triage**: \`${triage.name}\` · ${sev} · areas: ${areaText} · ${linkText}`, "");
    if (decision.verdict === "unsafe" && decision.securityReason) {
      const reason = sanitizeReview(
        decision.securityReason.replace(/\s+/g, " ").trim().slice(0, SECURITY_REASON_LIMIT),
        prShas,
      );
      state.redacted += reason.redactions;
      lines.push(`**Security concern**: ${reason.text}`, "");
    }
    const review = sanitizeReview(decision.review, prShas);
    state.redacted += review.redactions;
    lines.push(review.text);
  }
  if (state.redacted > 0) lines.push("", `_${state.redacted} value(s) redacted._`);
  const body = lines.join("\n");

  if (dryRun) {
    audit("would_label");
    return;
  }

  let failed = false;
  for (const level of toCreate) {
    try {
      await github.rest.issues.createLabel({
        owner,
        repo,
        name: level.name,
        color: level.color,
        description: level.description,
      });
    } catch (err) {
      // 422 = lost the race to a concurrent run; the label now exists.
      if (err.status !== 422) core.warning(`classify-pr: createLabel failed: ${logSafe(err.message, 200)}`);
    }
  }

  // Labels first, comment last: when the engines see the marker, the labels are already there.
  if (toAdd.length > 0) {
    try {
      await github.rest.issues.addLabels({ owner, repo, issue_number: prNumber, labels: toAdd });
    } catch (err) {
      failed = true;
      core.warning(`classify-pr: addLabels failed: ${logSafe(err.message, 200)}`);
    }
  }
  for (const name of toRemove) {
    try {
      await github.rest.issues.removeLabel({ owner, repo, issue_number: prNumber, name });
    } catch (err) {
      // 404 = already gone, which is the state we wanted.
      if (err.status !== 404) {
        failed = true;
        core.warning(`classify-pr: removeLabel failed: ${logSafe(err.message, 200)}`);
      }
    }
  }
  try {
    await github.rest.issues.createComment({ owner, repo, issue_number: prNumber, body });
  } catch (err) {
    failed = true;
    core.warning(`classify-pr: createComment failed: ${logSafe(err.message, 200)}`);
  }
  audit(failed ? "errored" : "labeled");
};

module.exports = apply;
module.exports.parseDecision = parseDecision;
module.exports.sanitizeReview = sanitizeReview;
module.exports.markerLine = markerLine;
