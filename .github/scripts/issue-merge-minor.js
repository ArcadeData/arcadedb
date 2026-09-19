// Applies the groupings the model proposed, for .github/workflows/merge-minor-issues.yml:
// one umbrella issue per accepted group, with every member closed as a duplicate of it.
//
// The proposal is treated as exactly that. The candidate set written by
// issue-collect-minor.js is the whitelist, and a group is discarded whole rather than trimmed
// when anything about it fails to check out, because a group that has to be corrected is a
// group whose reasoning was not sound.
//
// Tested by .github/scripts/tests/test-issue-workflow-scripts.js.

const fs = require("fs");
const {
  MINOR_LABEL,
  UMBRELLA_LABEL,
  MAX_GROUPS_PER_RUN,
  MAX_MEMBERS_PER_GROUP,
  PAYLOAD_FILE,
} = require("./issue-workflow-config.js");

module.exports = async ({ github, context, core }) => {
  const dryRun = process.env.DRY_RUN === "true";
  const severityLabel = MINOR_LABEL;
  const umbrellaLabel = UMBRELLA_LABEL;
  const maxGroups = MAX_GROUPS_PER_RUN;
  const maxMembers = MAX_MEMBERS_PER_GROUP;
  const owner = context.repo.owner;
  const repo = context.repo.repo;

  const audit = (msg) => core.info(`merge-minor: ${msg}${dryRun ? " (dry_run)" : ""}`);

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
    core.warning(`merge-minor: unreadable payload: ${err.message}`);
    audit("action=errored");
    return;
  }

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
    core.warning(`merge-minor: unreadable execution file: ${err.message}`);
    audit("action=errored");
    return;
  }

  const lines = [...text.matchAll(/^\s*GROUP:\s*(.+)$/gim)].map((m) => m[1]);
  if (lines.length === 0) {
    audit("groups=0, action=no_groups");
    return;
  }

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

  audit(`proposed=${lines.length}, accepted=${groups.length}, rejected=${rejected.join(" ") || "-"}`);
  if (groups.length === 0) return;

  // Only labels that already exist may be applied, same rule as the classifier.
  let existing = [];
  try {
    existing = await github.paginate(github.rest.issues.listLabelsForRepo, {
      owner,
      repo,
      per_page: 100,
    });
  } catch (err) {
    core.warning(`merge-minor: listLabelsForRepo failed: ${err.message}`);
  }
  const canonical = new Map(existing.map((l) => [l.name.toLowerCase(), l.name]));
  const labelsFor = (module) =>
    [module, severityLabel, umbrellaLabel]
      .map((n) => canonical.get(n.toLowerCase()))
      .filter((n) => n !== undefined);

  // Re-read each member immediately before touching it: minutes passed while the model was
  // thinking, and an issue may have been assigned, closed or picked up. An umbrella issue is
  // refused outright, so a group proposed before one existed cannot close it.
  const stillEligible = async (number) => {
    try {
      const { data } = await github.rest.issues.get({ owner, repo, issue_number: number });
      if (data.state !== "open") return "closed";
      if (data.assignees?.length > 0) return "assigned";
      const names = data.labels.map((l) => (typeof l === "string" ? l : l.name).toLowerCase());
      if (names.includes(umbrellaLabel.toLowerCase())) return "umbrella";
      if (names.includes("in progress")) return "in_progress";
      if (!names.includes(severityLabel.toLowerCase())) return "not_minor";
      return null;
    } catch (err) {
      core.warning(`merge-minor: get #${number} failed: ${err.message}`);
      return "errored";
    }
  };

  for (const group of groups) {
    const members = [];
    const dropped = [];
    for (const number of group.numbers) {
      const reason = await stillEligible(number);
      if (reason === null) members.push(number);
      else dropped.push(`${number}=${reason}`);
    }
    // Re-verified down to one member, there is nothing left to consolidate.
    if (members.length < 2) {
      audit(
        `module=${group.module}, members=${group.numbers.join("|")}, ` +
        `dropped=${dropped.join("|") || "-"}, action=skipped`
      );
      continue;
    }

    const body =
      `Automated consolidation of related \`${severityLabel}\` issues in ` +
      `**${group.module}**.\n\n` +
      members.map((n) => `- [ ] #${n}`).join("\n") +
      `\n\nEach issue above was closed as a duplicate of this one. If a grouping is wrong, ` +
      `reopen that issue and strike it from this list.\n\n` +
      `<sub>Opened by the \`merge-minor-issues\` workflow ` +
      `([run](${process.env.RUN_URL})).</sub>`;

    if (dryRun) {
      audit(
        `module=${group.module}, title=${group.title}, members=${members.join("|")}, ` +
        `dropped=${dropped.join("|") || "-"}, action=would_merge`
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
        labels: labelsFor(group.module),
      });
      umbrella = data.number;
    } catch (err) {
      core.warning(`merge-minor: create umbrella failed: ${err.message}`);
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
            `\`${severityLabel}\` issues in ${group.module}. Closing here as a duplicate - ` +
            `reopen this issue if the grouping is wrong.`,
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
        core.warning(`merge-minor: closing #${number} failed: ${err.message}`);
        failed.push(number);
      }
    }
    // A member left open here is picked up by the umbrella cross-reference check in
    // issue-collect-minor.js on the next run, so it is not swept into a second umbrella.
    audit(
      `module=${group.module}, umbrella=${umbrella}, closed=${closed.join("|") || "-"}, ` +
      `failed=${failed.join("|") || "-"}, dropped=${dropped.join("|") || "-"}, action=merged`
    );
  }
};
