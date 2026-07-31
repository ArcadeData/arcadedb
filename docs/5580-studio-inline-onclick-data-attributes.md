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

Section-header buttons (`createType`, `createMaterializedView`, `createGraphAnalyticalView`,
`createTimeSeriesType`) keep their inline `onclick`: they pass no argument, so they never enter the
nested-context problem. The tests assert this distinction rather than banning `onclick` outright.

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

## Out of scope / follow-ups

- `restoreBackupAction` / `deleteBackupAction` (backup file names) and `updateDatabaseSetting` (setting
  keys) still use inline handlers. They carry server-generated or fixed-vocabulary values, not schema
  names, so they were left alone to keep this diff to the issue's subject.
- `studio-security.js` reads its delegated values with `$(this).data("name")`. For user and group names
  that looks numeric (`"123"`), jQuery coerces to a number. Not triggered by this issue, worth a separate
  look.
- `renderMaterializedViewsSidebarSection` is dead code (defined, never called) and an exact duplicate of
  `renderMaterializedViewsSidebarBadges` plus a section wrapper. Converted for consistency rather than
  deleted, to keep this change focused.
