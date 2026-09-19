// Shared constants for the issue-automation scripts in this directory.
//
// They live here rather than in the workflows' `env:` blocks because they are the part the
// tests have to agree with: a cap or a label name that exists in only one of the two places is
// a test that passes while the workflow does something else. The workflows pass only what the
// event supplies - the issue number, the dry-run flag, the execution log's path.

// The three triage levels, owned and created on demand by classify-issue.yml. Every other label
// it applies must already exist in the repository.
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

// Owned by the sponsor job; the classifier may never apply it.
const RESERVED_LABEL = "high_priority";

// The level whose issues merge-minor-issues.yml consolidates, and the label that marks the
// umbrella it consolidates them into.
const MINOR_LABEL = "severity:minor";
const UMBRELLA_LABEL = "parent-issue";

// Everything that is a kind, a state, an ecosystem or a severity rather than a component of the
// product. A denylist rather than an allowlist: new module labels get added far more often than
// new process labels, and an allowlist that falls behind silently stops sweeping a whole module.
const NON_MODULE_LABELS = [
  "bug", "feature", "enhancement", "documentation", "question", "invalid", "wontfix",
  "duplicate", "security", "performance", "audit", "parent-issue", "high_priority",
  "in progress", "do not merge", "waiting for user", "unable to reproduce",
  "workaround-provided", "bolt-compat-regression", "dependencies", "dependency_approved",
  "github_actions", "java", "javascript", "docker", "go", "pre_commit", "python:uv",
];

// Caps. Two runs a day means anything left over is picked up twelve hours later, so these are
// deliberately small: they bound both the model's input and the damage a bad run can do.
const MAX_MODULES_PER_RUN = 3;
const MAX_CANDIDATES_PER_MODULE = 40;
const BODY_LIMIT = 1500;
const MAX_GROUPS_PER_RUN = 5;
const MAX_MEMBERS_PER_GROUP = 10;

// Written by issue-collect-minor.js, read by the model step and by issue-merge-minor.js. The
// name is repeated in merge-minor-issues.yml, where it pins the model's one allowed Read.
const PAYLOAD_FILE = "minor-candidates.json";

module.exports = {
  SEVERITY_LEVELS,
  RESERVED_LABEL,
  MINOR_LABEL,
  UMBRELLA_LABEL,
  NON_MODULE_LABELS,
  MAX_MODULES_PER_RUN,
  MAX_CANDIDATES_PER_MODULE,
  BODY_LIMIT,
  MAX_GROUPS_PER_RUN,
  MAX_MEMBERS_PER_GROUP,
  PAYLOAD_FILE,
};
