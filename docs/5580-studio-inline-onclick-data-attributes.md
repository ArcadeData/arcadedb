# 5580 - Studio: replace inline onclick handlers carrying schema names with data-* attributes

Follow-up to #5575. Branch `fix/5580-studio-inline-onclick-data-attributes`.

## Root cause

`studio-database.js` built roughly twenty controls by concatenating a schema object name into an inline
`onclick` attribute:

```js
html += "<button ... onclick='dropType(\"" + escapeHtml(row.name) + "\")'>...";
```

The name lands in three nested contexts at once and only the first is escaped:

1. HTML attribute value - handled by `escapeHtml`.
2. JS string literal - not handled. The browser HTML-decodes the attribute *before* parsing it as JavaScript,
   so `&quot;` turns back into `"` and terminates the string early.
3. Handler argument - never reached, because step 2 already failed.

Verified in Chrome against the pre-fix spelling with the name ``a"b'c`d\e<f>&g``: the browser parses the
attribute as `dropType("a"b'c`d\e<f>&g")` and raises
`SyntaxError: missing ) after argument list`. The button is inert - the action silently does nothing.

Several call sites were worse than the documented case and interpolated the name with **no escaping at all**
(`showTypeDetail` on super/sub type links, `browseType` in the query sidebar, `dropProperty`, `dropIndex`),
which makes a crafted type name a stored-XSS vector, not only a broken button.

All of these names are reachable: `CREATE DOCUMENT TYPE` accepts back-ticked names containing quotes,
back-ticks and backslashes.

## Fix

One escaping point, one dispatch point.

`schemaActionAttrs(action, name, parent)` is now the only place a schema name is spelled into an attribute.
It emits `data-action` plus `data-name` (and `data-parent` for the two actions that need the owning type),
each HTML-escaped exactly once. The value is read back through `dataset`, which the browser hands over as a
plain string with no further interpretation - one context instead of three.

Two delegated registries on `document` dispatch the actions (`schemaClickActions`, `schemaChangeActions`).
Delegation was chosen over per-container wiring because the fragments are produced by shared renderers
(`renderProperties`, `renderIndexes`, the sidebar badge builders) that are injected into more than one
container; a document-level handler survives every `.html()` replacement without re-wiring.

The registry doubles as the allowlist: an unknown `data-action` falls through untouched, so
`studio-security.js` keeps its own document-level `[data-action='…']` handlers unaffected.

Handlers read `this.dataset.*`, never jQuery's `.data()`. `.data()` coerces values that look numeric or
boolean, so a type legitimately named `123` or `true` would reach `quoteSqlName()` as a number or a boolean.

### Converted call sites (19 actions)

| Area | Actions |
|---|---|
| Schema sidebar / query sidebar badges | `show-type-detail`, `browse-type` |
| Type detail | `create-property`, `create-index`, `run-repartition`, `browse-records`, `browse-records-with-connections`, `count-records`, `drop-type` |
| Super / sub type and MV source-type links | `show-type-detail` |
| Property and index tables | `drop-property`, `drop-index` |
| Materialized views | `show-materialized-view-detail`, `refresh-materialized-view`, `alter-materialized-view`, `drop-materialized-view` |
| Graph analytical views | `show-gav-detail`, `rebuild-gav`, `drop-gav`, `alter-gav-update-mode` (a `change` on the update-mode select) |

The repartition button from #4087 was folded into the same mechanism, and its bespoke
`.js-repartition-btn` wiring removed, so the file now has exactly one pattern.

Section-header buttons keep their inline `onclick`, because none of them carries a user-controlled name.
`createTimeSeriesType()`, `createMaterializedView()` and `createGraphAnalyticalView()` pass no argument at
all. `createType(sec.key)` does pass one, but `sec.key` is a fixed section constant from the hardcoded
`sections` array (`vertex` / `edge` / `document`), never schema-derived, so it cannot contain a quote. The
tests assert the no-argument shape on the renderers they cover rather than banning `onclick` outright.

Small extractions were made so the generated HTML is reachable from tests without a DOM:
`renderTypeLink`, `renderTypeSidebarBadge` (which also de-duplicates two near-identical badge loops),
`renderTypeQuickActions`, `renderMaterializedViewQuickActions`.

## Verification

`studio/test/schema-action-attributes.test.js` - 17 new tests on Node's built-in runner:

- every renderer round-trips eight hostile names (`a"b`, `a'b`, `` a`b ``, `a\b`, `a<script>…`, `a&b`,
  `x"); alert(1); //`, `it's a "type"`) through the attribute and back, byte-exact;
- no attribute value contains an unencoded markup character;
- no inline handler carries an argument;
- source-level guards: every emitted `data-action` has a registry entry, and no registry entry is dead
  weight. A typo on either side renders a button that silently does nothing, which no runtime test would
  catch.

**Proof the tests fail without the fix:** reverting only `renderTypeLink` to the old inline spelling makes
`type links carry the name in a data attribute, not an onclick` fail (16/17), and restoring it returns 17/17.

**Full studio suite:** 41/41 pass (`npm test`). `render-indexes.test.js` needed two added `eval` lines in
its extraction harness because `renderIndexes` now calls `schemaActionAttrs`; no assertion was changed.

**Browser verification** (the issue asks for it; Chrome, jQuery 4 + the real dispatcher and renderers): all
22 rendered controls fired their handler and delivered the name ``a"b'c`d\e<f>&g`` byte-exact, with zero
corrupted arguments. The same page confirms the pre-fix spelling raises a `SyntaxError` and never fires.

## Review cycles

PR: https://github.com/ArcadeData/arcadedb/pull/5634

### Cycle 1 - `36a3cb88d`

`claude[bot]` reviewed and concluded **LGTM**, with four non-blocking notes. It independently confirmed the
two things most likely to have silently sunk the approach: that single-quoted attributes are safe because
`escapeHtml` escapes `'` as `&#039;`, and that the two-name argument order (`dropProperty(type, property)`,
`dropIndex(index, type)`) is preserved at every converted site.

| Note | Assessment | Action |
|---|---|---|
| Dead `id` on the repartition button | Verified against the code. The `id` is emitted but referenced nowhere in JS, HTML, CSS or the e2e module. `git show origin/main` shows it was **already** unreferenced before this change - the old wiring keyed on `.js-repartition-btn`, not the id - so the bot's "with `.js-repartition-btn` gone" framing is slightly off, but the conclusion holds | **Applied.** Removed `btnId` and the `id` attribute |
| `renderMaterializedViewsSidebarSection` dead + duplicated | Agreed, and the bot agreed the convert-not-delete choice was reasonable | No change; follow-up below |
| `studio-security.js` still uses `$(this).data("name")` | Correct, same coercion footgun | No change; left for the developer to file, since opening issues is outside this task's mandate |
| Remaining inline handlers confirmed benign | Matches the scope analysis | No change |

The bot noted it could not execute `node --test` in its sandbox and read the tests instead of running them.
The 41/41 result is reproduced locally, and the revert-one-renderer falsification check is recorded above.

### Cycle 2 - `6d83c305b`

**LGTM** again, with four non-blocking observations. Three of them (icon clicks inside buttons dispatching
via `this` rather than `event.target`; `preventDefault` without `stopPropagation`; the `change` handler
correctly omitting `preventDefault` so the native `<select>` dropdown still opens) were confirmations that
the current behaviour is right, not requests to change it.

The fourth was worth acting on: because the dispatch is bound to `document`, the `data-action` values form a
page-wide namespace, and a future control elsewhere in Studio reusing one of these names would be routed
here too. **Applied** - the registry comment now spells that out for the next contributor.

### Cycle 3 - `664bcbb1d`

**LGTM**, with four minor notes. Two were acted on:

- `schemaActionAttrs` does not escape its `action` parameter. Safe today because every caller passes a
  string literal, and the registry-coverage test fails at CI if that ever changes - but the assumption was
  undocumented. **Applied** as a comment.
- The writeup claimed section-header buttons "pass no argument". Checked against the code: **the bot is
  right and the claim was inaccurate**. `createType(sec.key)` at `studio-database.js:3497` does pass one.
  It is benign - `sec.key` is a fixed constant from the hardcoded `sections` array, never schema-derived -
  but the blanket statement was wrong. **Corrected** above and in the PR description.

The other two (delete the dead `renderMaterializedViewsSidebarSection`; open a tracking issue for the
`studio-security.js` `.data()` coercion) are follow-ups outside this change. No issue was filed, since
opening one is outside this task's mandate - see the list below.

### Cycle 4 - `4bd1906a4`

**LGTM.** Four non-blocking notes, none requiring a code change:

- `updateDatabaseSetting` (`studio-database.js:4004`) emits both `row[0]` and `row[1]` with zero escaping,
  and `row[1]` is the setting *value*, which is more free-form than the key. Out of scope here, but a
  sharper observation than the original follow-up note - carried into the list below.
- The click delegation itself has no automated test (the renderers do). Correct, and a conscious tradeoff:
  there is no jsdom in the suite, so the dispatch is covered by the Chrome pass recorded above.
- `studio-security.js` `.data()` coercion - already listed as a follow-up.
- A claimed styling delta on the MV source-type links. **Checked and rejected:** the bot stated the anchor
  "picks up `.link` styling it did not have before", but `git show origin/main` confirms the original
  anchor already carried `class='link'`. The only real change is that `font-weight:600` moved from the
  anchor's inline style to a wrapping span, and `.link` (`css/studio.css:228`) sets only `color`, no
  `font-weight` - so the inherited value applies unopposed and the rendering is identical. No change made.

Final state: **max-cycles-reached** (4 of 4), with four consecutive LGTMs and no outstanding code defects.
Every cycle after the first produced only documentation or comment changes.

### Unrelated CI

`Meterian Scanner workflow` fails on this branch, and also on the five most recent `main` commits. This
change touches no dependency manifest (only `studio-database.js`, two test files and this doc), so the
failure is pre-existing and unrelated. `Studio Security Audit` passes.

## Out of scope / follow-ups

- `restoreBackupAction` / `deleteBackupAction` (backup file names) and `updateDatabaseSetting` (setting
  keys) still use inline handlers. They carry server-generated or fixed-vocabulary values, not schema
  names, so they were left alone to keep this diff to the issue's subject. Note from review cycle 4:
  `updateDatabaseSetting` at `studio-database.js:4004` interpolates **both** `row[0]` (key) and `row[1]`
  (value) with no escaping at all, and the value is far more free-form than the key - this is the strongest
  candidate of the three for a follow-up.
- `studio-security.js` reads its delegated values with `$(this).data("name")`. For user and group names
  that looks numeric (`"123"`), jQuery coerces to a number. Not triggered by this issue, worth a separate
  look.
- `renderMaterializedViewsSidebarSection` is dead code (defined, never called) and an exact duplicate of
  `renderMaterializedViewsSidebarBadges` plus a section wrapper. Converted for consistency rather than
  deleted, to keep this change focused.
