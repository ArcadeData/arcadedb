# Issue #5035 — Request/response hot-path allocations & Studio content serving

**Type:** bug (performance / GC pressure), server module audit 2026-07.
**Branch:** `fix/5035-hot-path-allocations`

## Scope of this PR

The issue groups six independent allocation/serving-efficiency defects on the HTTP
path and explicitly permits splitting them into separate PRs ("result streaming vs
command-heuristic vs static-content caching are independent"). This PR takes the
three contained, behavior-preserving, unit-testable ones:

- **Defect 2 — per-request full-command copies + regex in the LIMIT heuristic**
  (`PostCommandHandler`). The old code did `command.toLowerCase(...)` (full copy of
  every SQL command), compiled a `split("\\R")` `Pattern` per call, and did a second
  full `command.toUpperCase(...)` just to test a `"PROFILE "` prefix. Replaced with
  `regionMatches`-based prefix checks, a `static final Pattern` for the line break,
  an allocation-free case-insensitive scan for the existing-LIMIT test, and lowercasing
  only the last line when an explicit LIMIT may already be present. Logic extracted to
  the package-private static `appendAutomaticLimit(command, language, limit)` so it is
  unit-testable in isolation. Behavior is identical to the previous heuristic.

- **Defect 4 — `UUID.randomUUID()` per request when no `X-Request-Id`**
  (`AbstractServerHttpHandler`). The generated value is only a log/echo correlation id
  (it is NOT the idempotency key — that uses the raw client header, which is null when
  the client sends none). Replaced the shared-`SecureRandom` `UUID.randomUUID()` with a
  cheap non-cryptographic id (`ThreadLocalRandom` hex + a process-wide monotonic
  counter), extracted to the static `generateCorrelationId()`.

- **Defect 6 (safe subset) — static content re-read + copied on every request**
  (`GetDynamicContentHandler`). Non-templated Studio assets (js/css/svg/fonts/json/ico)
  are immutable for the process lifetime; their raw bytes are now cached in a bounded
  `ConcurrentHashMap` keyed by resource path, so repeated requests skip
  `getResourceAsStream` + full read + `toByteArray` copy. Templated `.html` pages are
  deliberately NOT cached because they embed per-request dynamic content (`now`, `uuid`,
  role-gated `protectBegin` sections). Loading + caching extracted to the static
  `loadStaticResource(resourcePath)`.

## Deferred to follow-up PRs (higher regression risk / more invasive)

- Defect 1 — incremental result serialization (removing the intermediate `List` + JSON
  tree). Large refactor of the serialization path; separate PR.
- Defect 3 — parse the POST body from bytes/Reader instead of `new String(...).trim()`.
- Defect 5 — respect the remaining `limit` budget inside the Studio edge-filter loops.

## Verification

- `PostCommandHandlerAutoLimitTest` — behavior-equivalence unit tests for the LIMIT
  heuristic across select/match, uppercase, existing-LIMIT, trailing `;`, non-SQL.
- `CorrelationIdGenerationTest` — format/uniqueness of the generated correlation id and
  that `sanitizeRequestId` leaves it unchanged.
- `GetDynamicContentHandlerCacheTest` — cache hit returns the same array instance;
  missing resource returns null.
- Existing `GetDynamicContentHandlerIT`, `PostCommandHandler*` tests must stay green.
</content>
</invoke>
