// Applies the labels and the triage level that the classifier step decided, for
// .github/workflows/classify-issue.yml.
//
// The issue body is untrusted input and the model that read it cannot be trusted to have
// ignored it, so nothing it returns reaches the API unchecked: a label must already exist in
// the repository, the reserved sponsor label can never be applied, and the triage level travels
// on its own line so that exactly one level can ever land.
//
// Tested by .github/scripts/tests/test-issue-workflow-scripts.js.

const fs = require("fs");
const { SEVERITY_LEVELS, RESERVED_LABEL } = require("./issue-workflow-config.js");

module.exports = async ({ github, context, core }) => {
  const issueNumber = Number(process.env.ISSUE_NUMBER);
  const dryRun = process.env.DRY_RUN === "true";
  const reserved = RESERVED_LABEL.toLowerCase();
  const severityByName = new Map(SEVERITY_LEVELS.map((l) => [l.name.toLowerCase(), l]));

  const audit = (msg) =>
    core.info(`classify-issue: issue=${issueNumber}, ${msg}${dryRun ? " (dry_run)" : ""}`);

  // The execution log is the JSON array of SDK messages. The final `result` message carries
  // Claude's last reply; fall back to the last assistant text block.
  let text = "";
  try {
    const raw = JSON.parse(fs.readFileSync(process.env.EXECUTION_FILE, "utf8"));
    const messages = Array.isArray(raw) ? raw : [raw];
    for (const m of messages) {
      if (m?.type === "result" && typeof m.result === "string") text = m.result;
      else if (m?.type === "assistant" && Array.isArray(m.message?.content)) {
        for (const c of m.message.content)
          if (c?.type === "text" && typeof c.text === "string") text = c.text;
      }
    }
  } catch (err) {
    core.warning(`classify-issue: unreadable execution file: ${err.message}`);
    audit("action=errored");
    return;
  }

  // Last LABELS:/SEVERITY: line wins, so a line quoted from the issue body earlier in the
  // reply cannot outrank the decision Claude ends on.
  const lastLine = (keyword) => {
    const m = [...text.matchAll(new RegExp(`^\\s*${keyword}:\\s*(.*)$`, "gim"))];
    return m.length === 0 ? null : m[m.length - 1][1];
  };
  const clean = (s) => s.trim().replace(/^[`"']|[`"']$/g, "");

  const labelsLine = lastLine("LABELS");
  const severityLine = lastLine("SEVERITY");
  if (labelsLine === null && severityLine === null) {
    audit("action=no_decision");
    return;
  }
  const requested = (labelsLine ?? "")
    .split(",")
    .map(clean)
    .filter((s) => s.length > 0 && s.toLowerCase() !== "none");

  // An unknown or missing level is dropped rather than guessed at: a wrong gravity is worse
  // than none, and the audit line says which it was.
  const severityRequested = severityLine === null ? "" : clean(severityLine);
  const severity = severityByName.get(severityRequested.toLowerCase()) ?? null;

  if (requested.length === 0 && severity === null) {
    audit(`severity_requested=${severityRequested || "-"}, action=none_chosen`);
    return;
  }

  // Ensure the chosen triage label exists before it can be applied. Unlike the classification
  // labels, this workflow owns these three and creates them. A dry run still probes, because
  // getLabel is read-only and the answer is what makes the rehearsal's audit line honest about
  // whether the label would have to be created first.
  let severityName = null;
  let severityExists = "n/a";
  if (severity !== null) {
    let exists = false;
    try {
      await github.rest.issues.getLabel({
        owner: context.repo.owner,
        repo: context.repo.repo,
        name: severity.name,
      });
      exists = true;
      severityName = severity.name;
    } catch (err) {
      if (err.status !== 404) {
        core.warning(`classify-issue: getLabel failed: ${err.message}`);
      } else if (dryRun) {
        severityName = severity.name;
      } else {
        try {
          await github.rest.issues.createLabel({
            owner: context.repo.owner,
            repo: context.repo.repo,
            name: severity.name,
            color: severity.color,
            description: severity.description,
          });
          severityName = severity.name;
        } catch (createErr) {
          // 422 = lost the race to a concurrent run; the label now exists.
          if (createErr.status === 422) severityName = severity.name;
          else core.warning(`classify-issue: createLabel failed: ${createErr.message}`);
        }
      }
    }
    severityExists = exists ? "yes" : "no";
  }

  // Only labels that already exist may be applied, matched case-insensitively and mapped back
  // to their canonical name. This is what keeps a hallucinated or injected name from reaching
  // the API at all.
  let existing;
  try {
    existing = await github.paginate(github.rest.issues.listLabelsForRepo, {
      owner: context.repo.owner,
      repo: context.repo.repo,
      per_page: 100,
    });
  } catch (err) {
    core.warning(`classify-issue: listLabelsForRepo failed: ${err.message}`);
    audit(
      `requested=${requested.join("|") || "-"}, severity=${severityName ?? "-"}, action=errored`
    );
    return;
  }
  const canonical = new Map(existing.map((l) => [l.name.toLowerCase(), l.name]));

  const seen = new Set();
  const labels = [];
  const dropped = [];
  for (const name of requested) {
    const key = name.toLowerCase();
    // A severity name on the LABELS line is dropped: the SEVERITY line is the only way in, so
    // Claude cannot smuggle a second level past the single-level rule.
    if (key === reserved || severityByName.has(key) || !canonical.has(key) || seen.has(key)) {
      dropped.push(name);
      continue;
    }
    seen.add(key);
    labels.push(canonical.get(key));
  }
  if (severityName !== null) labels.push(severityName);

  const tail =
    `requested=${requested.join("|") || "-"}, ` +
    `severity_requested=${severityRequested || "-"}, ` +
    `severity=${severityName ?? "-"}, severity_exists=${severityExists}, ` +
    `applied=${labels.join("|") || "-"}, dropped=${dropped.join("|") || "-"}`;
  if (labels.length === 0) {
    audit(`${tail}, action=skipped`);
    return;
  }

  if (dryRun) {
    audit(`${tail}, action=would_label`);
    return;
  }

  try {
    await github.rest.issues.addLabels({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: issueNumber,
      labels,
    });
    audit(`${tail}, action=labeled`);
  } catch (err) {
    core.warning(`classify-issue: addLabels failed: ${err.message}`);
    audit(`${tail}, action=errored`);
  }
};
