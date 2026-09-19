// Collects the `severity:minor` issues that are eligible to be folded into an umbrella issue,
// for .github/workflows/merge-minor-issues.yml, and writes them out per module for the model
// step to read.
//
// Every filter here fails toward "leave the issue alone": being wrong in that direction costs a
// consolidation that happens twelve hours later, while being wrong in the other closes somebody's
// open report.
//
// Tested by .github/scripts/tests/test-issue-workflow-scripts.js.

const fs = require("fs");
const {
  MINOR_LABEL,
  UMBRELLA_LABEL,
  NON_MODULE_LABELS,
  MAX_MODULES_PER_RUN,
  MAX_CANDIDATES_PER_MODULE,
  BODY_LIMIT,
  PAYLOAD_FILE,
} = require("./issue-workflow-config.js");

module.exports = async ({ github, context, core }) => {
  const severityLabel = MINOR_LABEL.toLowerCase();
  const umbrellaLabel = UMBRELLA_LABEL.toLowerCase();
  const nonModule = new Set(NON_MODULE_LABELS.map((l) => l.toLowerCase()));
  const moduleFilter = (process.env.MODULE_FILTER || "").trim().toLowerCase();
  const maxModules = MAX_MODULES_PER_RUN;
  const maxPerModule = MAX_CANDIDATES_PER_MODULE;
  const bodyLimit = BODY_LIMIT;

  const audit = (msg) => core.info(`merge-minor: ${msg}`);
  // A run that decides to do nothing is the common case, so it has to say WHY it decided that,
  // issue by issue. Counters alone cannot tell "nothing qualified" apart from "nothing ran".
  const reason = (label, numbers) =>
    audit(`${label}=${numbers.length === 0 ? "-" : numbers.map((n) => `#${n}`).join("|")}`);

  let issues;
  try {
    issues = await github.paginate(github.rest.issues.listForRepo, {
      owner: context.repo.owner,
      repo: context.repo.repo,
      state: "open",
      labels: MINOR_LABEL,
      per_page: 100,
    });
  } catch (err) {
    core.warning(`merge-minor: listForRepo failed: ${err.message}`);
    core.setOutput("count", "0");
    return;
  }

  // First pass: the filters that cost nothing, straight off the list payload.
  const byModule = new Map();
  const skippedBusy = [];
  const skippedUmbrella = [];
  const skippedNoModule = [];
  for (const issue of issues) {
    if (issue.pull_request) continue;
    const names = issue.labels.map((l) => (typeof l === "string" ? l : l.name).toLowerCase());
    // An umbrella carries the module label and `severity:minor` itself, so without this it
    // would come back as a candidate on the next run, and two umbrellas of one module could be
    // merged into a third - closing the older one and orphaning everything it tracked.
    if (names.includes(umbrellaLabel)) {
      skippedUmbrella.push(issue.number);
      continue;
    }
    if (issue.assignees?.length > 0 || names.includes("in progress")) {
      skippedBusy.push(issue.number);
      continue;
    }
    const modules = names.filter(
      (n) =>
        n !== severityLabel &&
        !n.startsWith("severity:") &&
        !nonModule.has(n) &&
        (moduleFilter === "" || n === moduleFilter)
    );
    // An issue carrying two module labels belongs to the alphabetically first of them and to
    // no other pool, so that a single issue can never land in two umbrellas.
    if (modules.length === 0) {
      skippedNoModule.push(issue.number);
      continue;
    }
    const key = modules.sort()[0];
    if (!byModule.has(key)) byModule.set(key, []);
    byModule.get(key).push(issue);
  }

  audit(
    `open_minor=${issues.length}, module_filter=${moduleFilter || "-"}, ` +
    `bucketed=${[...byModule.entries()].map(([m, l]) => `${m}:${l.length}`).join("|") || "-"}`
  );
  reason("skipped_umbrella", skippedUmbrella);
  reason("skipped_busy", skippedBusy);
  reason("skipped_no_module_label", skippedNoModule);

  // A module with a single candidate has nothing to merge with.
  const eligible = [...byModule.entries()].filter(([, list]) => list.length >= 2);
  const tooSmall = [...byModule.entries()].filter(([, list]) => list.length < 2);
  const ordered = eligible.sort((a, b) => b[1].length - a[1].length).slice(0, maxModules);
  if (tooSmall.length > 0)
    audit(`modules_with_one_candidate=${tooSmall.map(([m]) => m).join("|")}`);
  if (eligible.length > ordered.length)
    audit(
      `modules_over_cap=${eligible.slice(maxModules).map(([m, l]) => `${m}:${l.length}`).join("|")}` +
      `, cap=${maxModules}`
    );

  // Second pass, on the capped pool only. Two reasons to leave an issue alone, both read off
  // the same timeline fetch:
  //
  //  - an open PR references it, or it is linked to one through the sidebar, so somebody is on
  //    it and closing the issue would orphan that PR's `Closes #N`. A `connected` link that was
  //    later removed (`disconnected`) does not count, and neither does a reference from a PR
  //    that has since been closed or merged without fixing the issue - otherwise one stale link
  //    would exclude the issue from this feature permanently;
  //  - an open umbrella issue already tracks it, which happens when a previous run created the
  //    umbrella but failed to close this member. Re-merging it would have two live umbrellas
  //    tracking the same issue.
  //
  // The check fails closed: an unreadable timeline means the issue is left alone.
  const spokenFor = async (number) => {
    try {
      const events = await github.paginate(github.rest.issues.listEventsForTimeline, {
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: number,
        per_page: 100,
      });
      let linked = false;
      for (const e of events) {
        if (e.event === "connected") {
          // The source is not always present on this event; when it is, a PR that is no longer
          // open is no reason to hold the issue back.
          const src = e.source?.issue;
          linked = src === undefined || src.state === "open";
          continue;
        }
        if (e.event === "disconnected") {
          linked = false;
          continue;
        }
        if (e.event !== "cross-referenced") continue;
        const src = e.source?.issue;
        if (src === undefined || src.state !== "open") continue;
        if (src.pull_request) return "open_pr";
        const names = (src.labels ?? []).map((l) =>
          (typeof l === "string" ? l : l.name).toLowerCase()
        );
        if (names.includes(umbrellaLabel)) return "umbrella";
      }
      return linked ? "open_pr" : null;
    } catch (err) {
      core.warning(`merge-minor: timeline for #${number} failed: ${err.message}`);
      return "errored";
    }
  };

  const truncate = (s) =>
    !s ? "" : s.length <= bodyLimit ? s : `${s.slice(0, bodyLimit)}\n[...truncated]`;

  const modules = [];
  let candidateCount = 0;
  const skippedSpokenFor = [];
  for (const [name, list] of ordered) {
    const candidates = [];
    for (const issue of list.slice(0, maxPerModule)) {
      const why = await spokenFor(issue.number);
      if (why !== null) {
        skippedSpokenFor.push(`${issue.number}=${why}`);
        continue;
      }
      candidates.push({
        number: issue.number,
        title: issue.title,
        labels: issue.labels.map((l) => (typeof l === "string" ? l : l.name)),
        body: truncate(issue.body),
      });
    }
    if (candidates.length < 2) {
      audit(`module=${name}, eligible=${candidates.length}, action=too_few_after_filtering`);
      continue;
    }
    modules.push({ module: name, candidates });
    candidateCount += candidates.length;
  }

  fs.writeFileSync(PAYLOAD_FILE, JSON.stringify({ modules }, null, 1));
  core.setOutput("count", String(candidateCount));
  audit(
    `skipped_spoken_for=${skippedSpokenFor.map((s) => `#${s}`).join("|") || "-"}`
  );

  // The exact set handed to the model, so the next log line can be read against what it saw.
  for (const m of modules)
    for (const c of m.candidates)
      audit(`candidate module=${m.module} #${c.number} ${c.title}`);

  if (candidateCount === 0) {
    audit("candidates=0, action=nothing_to_group");
    return;
  }
  audit(
    `candidates=${candidateCount}, ` +
    `modules=${modules.map((m) => `${m.module}:${m.candidates.length}`).join("|")}, ` +
    `action=sent_to_model`
  );

  core.summary
    .addHeading("Minor-issue consolidation: candidates", 3)
    .addTable([
      [
        { data: "Module", header: true },
        { data: "Issues", header: true },
      ],
      ...modules.map((m) => [m.module, m.candidates.map((c) => `#${c.number}`).join(", ")]),
    ]);
  await core.summary.write();
};
