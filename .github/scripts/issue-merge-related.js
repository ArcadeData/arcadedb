// Applies the groupings the model proposed, for .github/workflows/merge-related-issues.yml:
// one umbrella issue per accepted group, with every member closed as a duplicate of it.
//
// The umbrella inherits the HIGHEST severity of the members that actually land in it, read
// fresh at that moment: three issues folded into one, of which one is critical, produce a
// critical umbrella. Anything else would launder a critical defect into a lesser one by
// consolidating it.
//
// The proposal is treated as exactly that. The candidate set written by
// issue-collect-related.js is the whitelist, and a group is discarded whole rather than trimmed
// when anything about it fails to check out, because a group that has to be corrected is a
// group whose reasoning was not sound.
//
// Tested by .github/scripts/tests/test-issue-workflow-scripts.js.

const fs = require("fs");
const {
  logSafe,
  severityOf,
  highestSeverity,
  UMBRELLA_LABEL,
  MAX_GROUPS_PER_RUN,
  MAX_MEMBERS_PER_GROUP,
  PAYLOAD_FILE,
} = require("./issue-workflow-config.js");

module.exports = async ({ github, context, core }) => {
  const dryRun = process.env.DRY_RUN === "true";
  const umbrellaLabel = UMBRELLA_LABEL;
  const maxGroups = MAX_GROUPS_PER_RUN;
  const maxMembers = MAX_MEMBERS_PER_GROUP;
  const owner = context.repo.owner;
  const repo = context.repo.repo;

  const audit = (msg) => core.info(`merge-related: ${msg}${dryRun ? " (dry_run)" : ""}`);

  // The candidate set is the whitelist. Nothing outside it can be touched, whatever the model
  // was talked into writing.
  let pools;
  try {
    const payload = JSON.parse(fs.readFileSync(PAYLOAD_FILE, "utf8"));
    pools = new Map(
      payload.modules.map((m) => [
        m.module.toLowerCase(),
        new Set(m.candidates.map((c) => c.number)),
      ])
    );
  } catch (err) {
    core.warning(`merge-related: unreadable payload: ${err.message}`);
    audit("action=errored");
    return;
  }

  // The execution log is the JSON array of SDK messages. The final `result` message carries
  // Claude's last reply; fall back to the last assistant text block.
  let text = "";
  let envelope = null;
  try {
    const raw = JSON.parse(fs.readFileSync(process.env.EXECUTION_FILE, "utf8"));
    const messages = Array.isArray(raw) ? raw : [raw];
    for (const m of messages) {
      if (m?.type === "result") {
        envelope = m;
        if (typeof m.result === "string") text = m.result;
      } else if (m?.type === "assistant" && Array.isArray(m.message?.content)) {
        for (const c of m.message.content)
          if (c?.type === "text" && typeof c.text === "string") text = c.text;
      }
    }
  } catch (err) {
    core.warning(`merge-related: unreadable execution file: ${err.message}`);
    audit("action=errored");
    return;
  }

  audit(
    `model_result=${envelope?.subtype ?? "absent"}, is_error=${envelope?.is_error ?? "?"}, ` +
    `turns=${envelope?.num_turns ?? "?"}, reply_chars=${text.length}`
  );

  const lines = [...text.matchAll(/^\s*GROUP:\s*(.+)$/gim)].map((m) => m[1]);
  if (lines.length === 0) {
    // "It looked and found nothing" and "it never answered in the agreed shape" are different
    // events with different fixes, and the old single message could not tell them apart.
    // `\b` rather than end-of-line: "GROUPS: none (nothing coheres here)" is still a decline,
    // and warning about it would train the reader to ignore the warning that matters.
    if (/^\s*GROUPS:\s*none\b/im.test(text)) {
      // A decline is the common outcome, so its REASONING is the most valuable thing the run
      // produces: it is the only evidence of what the model made of the candidates, and the
      // action writes it to the execution file and nowhere else.
      const why = logSafe(text, 600);
      audit(`groups=0, action=declined_no_group_qualifies, reason="${why}"`);
      core.summary
        .addHeading("Issue consolidation: nothing merged", 3)
        .addRaw(
          "The model read the candidates and found no set that one umbrella issue would " +
            "faithfully replace. Every candidate was left untouched. It reported:"
        )
        .addCodeBlock(why);
      await core.summary.write();
      return;
    }
    audit(`groups=0, action=no_decision, reply_tail="${logSafe(text.slice(-400), 400)}"`);
    core.warning(
      "merge-related: the model's reply carried neither a GROUP: line nor `GROUPS: none`; " +
      "nothing was changed. See the reply_tail in the log."
    );
    return;
  }
  for (const line of lines) audit(`proposed ${logSafe(line, 200)}`);

  // A title is the only untrusted text that reaches GitHub, so it is stripped of anything that
  // could mention a team, break out of the line, or run long. The umbrella BODY quotes nothing:
  // it carries bare `#N` references, which GitHub renders with each issue's own real title.
  const sanitizeTitle = (raw, module, size) => {
    const clean = raw
      .replace(/[\u0000-\u001f\u007f]/g, " ")
      .replace(/[@`<>]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100)
      .trim();
    return clean.length > 0
      ? `[${module}] ${clean}`
      : `[${module}] ${size} related minor issues`;
  };

  const claimed = new Set();
  const groups = [];
  const rejected = [];
  for (const line of lines) {
    const parts = line.split("|");
    if (parts.length < 3) {
      rejected.push(`malformed:${line.slice(0, 40)}`);
      continue;
    }
    const module = parts[0].trim().replace(/^[`"']|[`"']$/g, "").toLowerCase();
    const pool = pools.get(module);
    if (!pool) {
      rejected.push(`unknown_module:${module}`);
      continue;
    }
    const numbers = [
      ...new Set(
        parts[1]
          .split(",")
          .map((n) => Number(n.trim().replace(/^#/, "")))
          .filter((n) => Number.isInteger(n) && n > 0)
      ),
    ];
    // Every member must come from this module's own pool and must not already belong to
    // another group of this run.
    const outside = numbers.filter((n) => !pool.has(n));
    const taken = numbers.filter((n) => claimed.has(n));
    if (outside.length > 0 || taken.length > 0) {
      rejected.push(
        `${module}:outside=${outside.join("/") || "-"}:taken=${taken.join("/") || "-"}`
      );
      continue;
    }
    if (numbers.length < 2 || numbers.length > maxMembers) {
      rejected.push(`${module}:size=${numbers.length}`);
      continue;
    }
    if (groups.length >= maxGroups) {
      rejected.push(`${module}:over_cap`);
      continue;
    }
    for (const n of numbers) claimed.add(n);
    groups.push({
      module,
      numbers,
      title: sanitizeTitle(parts.slice(2).join("|"), module, numbers.length),
    });
  }

  audit(`proposed=${lines.length}, accepted=${groups.length}, rejected=${rejected.length}`);
  for (const r of rejected) audit(`rejected ${r}`);
  for (const g of groups)
    audit(`accepted module=${g.module}, members=${g.numbers.map((n) => `#${n}`).join("|")}`);
  if (groups.length === 0) {
    core.warning(
      `merge-related: ${lines.length} grouping(s) proposed and every one was refused; ` +
      "nothing was changed. See the `rejected` lines in the log."
    );
    return;
  }

  // Only labels that already exist may be applied, same rule as the classifier.
  let existing = [];
  try {
    existing = await github.paginate(github.rest.issues.listLabelsForRepo, {
      owner,
      repo,
      per_page: 100,
    });
  } catch (err) {
    core.warning(`merge-related: listLabelsForRepo failed: ${err.message}`);
  }
  const canonical = new Map(existing.map((l) => [l.name.toLowerCase(), l.name]));
  const labelsFor = (module, severity) =>
    [module, severity, umbrellaLabel]
      .map((n) => canonical.get(n.toLowerCase()))
      .filter((n) => n !== undefined);

  // Re-read each member immediately before touching it: minutes passed while the model was
  // thinking, and an issue may have been assigned, closed or picked up. An umbrella issue is
  // refused outright, so a group proposed before one existed cannot close it.
  const stillEligible = async (number) => {
    try {
      const { data } = await github.rest.issues.get({ owner, repo, issue_number: number });
      if (data.state !== "open") return { reason: "closed" };
      if (data.assignees?.length > 0) return { reason: "assigned" };
      const names = data.labels.map((l) => (typeof l === "string" ? l : l.name).toLowerCase());
      if (names.includes(umbrellaLabel.toLowerCase())) return { reason: "umbrella" };
      if (names.includes("in progress")) return { reason: "in_progress" };
      // Read fresh rather than trusting the payload: the level may have been corrected while
      // the model was thinking, and it is what the umbrella inherits.
      const severity = severityOf(names);
      if (severity === null) return { reason: "untriaged" };
      return { severity };
    } catch (err) {
      core.warning(`merge-related: get #${number} failed: ${err.message}`);
      return { reason: "errored" };
    }
  };

  const merged = [];
  for (const group of groups) {
    const members = [];
    const severities = [];
    const dropped = [];
    for (const number of group.numbers) {
      const verdict = await stillEligible(number);
      if (verdict.reason === undefined) {
        members.push(number);
        severities.push(verdict.severity);
      } else dropped.push(`${number}=${verdict.reason}`);
    }
    const severity = highestSeverity(severities);
    // Re-verified down to one member, there is nothing left to consolidate.
    if (members.length < 2) {
      audit(
        `module=${group.module}, members=${group.numbers.join("|")}, ` +
        `dropped=${dropped.join("|") || "-"}, action=skipped`
      );
      continue;
    }

    const body =
      `Automated consolidation of related issues in **${group.module}**, carrying the ` +
      `highest severity of the issues it replaces (\`${severity}\`).\n\n` +
      members.map((n, i) => `- [ ] #${n} (\`${severities[i]}\`)`).join("\n") +
      `\n\nEach issue above was closed as a duplicate of this one. If a grouping is wrong, ` +
      `reopen that issue and strike it from this list.\n\n` +
      `<sub>Opened by the \`merge-related-issues\` workflow ` +
      `([run](${process.env.RUN_URL})).</sub>`;

    if (dryRun) {
      audit(
        `module=${group.module}, title=${group.title}, members=${members.join("|")}, ` +
        `severity=${severity}, dropped=${dropped.join("|") || "-"}, action=would_merge`
      );
      continue;
    }

    let umbrella;
    try {
      const { data } = await github.rest.issues.create({
        owner,
        repo,
        title: group.title,
        body,
        labels: labelsFor(group.module, severity),
      });
      umbrella = data.number;
    } catch (err) {
      core.warning(`merge-related: create umbrella failed: ${err.message}`);
      audit(`module=${group.module}, action=errored`);
      continue;
    }

    const closed = [];
    const failed = [];
    for (const number of members) {
      try {
        await github.rest.issues.createComment({
          owner,
          repo,
          issue_number: number,
          body:
            `Merged into #${umbrella}, which now tracks this together with the other related ` +
            `issues in ${group.module}. Closing here as a duplicate - reopen this issue if ` +
            `the grouping is wrong.`,
        });
        try {
          await github.rest.issues.update({
            owner,
            repo,
            issue_number: number,
            state: "closed",
            state_reason: "duplicate",
          });
        } catch (err) {
          // Older API surfaces reject the `duplicate` reason; `not_planned` is the closest one
          // every version accepts.
          if (err.status !== 422) throw err;
          await github.rest.issues.update({
            owner,
            repo,
            issue_number: number,
            state: "closed",
            state_reason: "not_planned",
          });
        }
        closed.push(number);
      } catch (err) {
        core.warning(`merge-related: closing #${number} failed: ${err.message}`);
        failed.push(number);
      }
    }
    // A member left open here is picked up by the umbrella cross-reference check in
    // issue-collect-related.js on the next run, so it is not swept into a second umbrella.
    audit(
      `module=${group.module}, umbrella=${umbrella}, severity=${severity}, ` +
      `closed=${closed.join("|") || "-"}, failed=${failed.join("|") || "-"}, ` +
      `dropped=${dropped.join("|") || "-"}, action=merged`
    );
    merged.push({ module: group.module, umbrella, severity, closed, failed, dropped });
  }

  if (merged.length > 0) {
    core.summary.addHeading("Issue consolidation", 3).addTable([
      [
        { data: "Umbrella", header: true },
        { data: "Module", header: true },
        { data: "Severity", header: true },
        { data: "Closed as duplicate", header: true },
        { data: "Left open", header: true },
      ],
      ...merged.map((m) => [
        `#${m.umbrella}`,
        m.module,
        m.severity,
        m.closed.map((n) => `#${n}`).join(", ") || "-",
        [...m.failed, ...m.dropped].map((n) => `#${n}`).join(", ") || "-",
      ]),
    ]);
    await core.summary.write();
  }
};
