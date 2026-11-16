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
// Shared constants for the issue-automation scripts in this directory.
//
// They live here rather than in the workflows' `env:` blocks because they are the part the
// tests have to agree with: a cap or a label name that exists in only one of the two places is
// a test that passes while the workflow does something else. The workflows pass only what the
// event supplies - the issue number, the dry-run flag, the execution log's path.

// The three triage levels, owned and created on demand by classify-issue.yml. Every other label
// it applies must already exist in the repository.
//
// ORDERED MOST SEVERE FIRST, and `highestSeverity` below depends on that: when related issues of
// different levels are folded into one umbrella, the umbrella has to carry the gravity of the
// worst of them, or consolidating a critical with two minors would quietly downgrade it.
const SEVERITY_LEVELS = [
  {
    name: "severity:critical",
    color: "8B0000",
    description: "Data loss, corruption, crash/hang, security hole, or product unusable",
  },
  {
    name: "severity:major",
    color: "D93F0B",
    description: "A feature is broken or returns wrong results, with no practical workaround",
  },
  {
    name: "severity:minor",
    color: "FBCA04",
    description: "Cosmetic, documentation, edge case, or a workaround exists",
  },
];

// The three verdicts of classify-pr.yml's PR triage, created on demand like the severity labels.
// `verdict` is what the reviewer returns; `name` is the label that carries it.
const TRIAGE_LEVELS = [
  {
    verdict: "lgtm",
    name: "triage:lgtm",
    color: "0E8A16",
    description: "PR triage: nothing blocking, ready for a maintainer",
  },
  {
    verdict: "needs-changes",
    name: "triage:needs-changes",
    color: "D93F0B",
    description: "PR triage: the PR needs changes before it can merge",
  },
  {
    verdict: "unsafe",
    name: "triage:unsafe",
    color: "B60205",
    description: "PR triage: the diff looks malicious or unsafe to run",
  },
];

// Whether a PR links the issue it resolves.
const ISSUE_LINK_LABELS = {
  has: { name: "has-issue", color: "0075CA", description: "The PR links an issue it resolves" },
  none: { name: "no-issue", color: "C5DEF5", description: "The PR links no issue" },
};

// Prefix of the hidden marker that identifies classify-pr's own comment on a PR.
const PR_MARKER_PREFIX = "<!-- classify-pr";

// Cap on the PR diff text handed to the reviewer.
const PR_REVIEW_LIMIT = 60000;

// Owned by the sponsor job; the classifier may never apply it.
const RESERVED_LABEL = "high_priority";

// The label that marks an umbrella issue.
const UMBRELLA_LABEL = "parent-issue";

const SEVERITY_BY_NAME = new Map(SEVERITY_LEVELS.map((l) => [l.name.toLowerCase(), l]));

/**
 * The severity carried by a set of label names, or null when none of them is a triage level.
 * An untriaged issue has no gravity to compare, which is why merge-related-issues.yml leaves
 * it alone rather than guessing one.
 */
const severityOf = (names) => {
  for (const level of SEVERITY_LEVELS)
    if (names.some((n) => n.toLowerCase() === level.name)) return level.name;
  return null;
};

/** The worst of several severities, for the umbrella that replaces them. */
const highestSeverity = (severities) => {
  for (const level of SEVERITY_LEVELS)
    if (severities.includes(level.name)) return level.name;
  return null;
};

// Everything that is a kind, a state, an ecosystem or a severity rather than a component of the
// product. A denylist rather than an allowlist: new module labels get added far more often than
// new process labels, and an allowlist that falls behind silently stops sweeping a whole module.
const NON_MODULE_LABELS = [
  "bug", "feature", "enhancement", "documentation", "question", "invalid", "wontfix",
  "duplicate", "security", "performance", "audit", "parent-issue", "high_priority",
  "in progress", "do not merge", "waiting for user", "unable to reproduce",
  "workaround-provided", "bolt-compat-regression", "dependencies", "dependency_approved",
  "github_actions", "java", "javascript", "docker", "go", "pre_commit", "python:uv",
  ...TRIAGE_LEVELS.map((l) => l.name), ISSUE_LINK_LABELS.has.name, ISSUE_LINK_LABELS.none.name,
];

// Caps. Two runs a day means anything left over is picked up twelve hours later, so these are
// deliberately small: they bound both the model's input and the damage a bad run can do.
const MAX_MODULES_PER_RUN = 3;
const MAX_CANDIDATES_PER_MODULE = 40;
const BODY_LIMIT = 1500;
const MAX_GROUPS_PER_RUN = 5;
const MAX_MEMBERS_PER_GROUP = 10;

// Written by issue-collect-related.js, read by the model step and by issue-merge-related.js.
// The name is repeated in merge-related-issues.yml, where it pins the model's one allowed Read.
const PAYLOAD_FILE = "related-candidates.json";

/**
 * Flattens untrusted text for a log line. `core.info` writes straight to stdout, and GitHub
 * Actions reads any line that STARTS with `::` as a workflow command, so text that came from an
 * issue - a title, a body, or a model's reply after reading one - is collapsed to a single line
 * with control characters stripped, `::` defused and a hard length cap. Both are needed: the
 * collapse removes the ability to start a line at all, and defusing `::` keeps it harmless even
 * if it is ever logged somewhere that does not prefix it.
 */
const logSafe = (text, max) =>
  (text ?? "")
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/::/g, ":")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, max) || "-";

module.exports = {
  logSafe,
  SEVERITY_LEVELS,
  SEVERITY_BY_NAME,
  TRIAGE_LEVELS,
  ISSUE_LINK_LABELS,
  PR_MARKER_PREFIX,
  PR_REVIEW_LIMIT,
  severityOf,
  highestSeverity,
  RESERVED_LABEL,
  UMBRELLA_LABEL,
  NON_MODULE_LABELS,
  MAX_MODULES_PER_RUN,
  MAX_CANDIDATES_PER_MODULE,
  BODY_LIMIT,
  MAX_GROUPS_PER_RUN,
  MAX_MEMBERS_PER_GROUP,
  PAYLOAD_FILE,
};
