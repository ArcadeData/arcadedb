# Review notes - deferred / skipped items (HEAD f327302e)

Cycle 1 review from `claude` and `gemini-code-assist`. Items applied as new commits are omitted; this
file records only what was intentionally NOT changed, with rationale.

## Applied (for reference)
- Gemini HIGH (ServerSecurity.java:243) data race on mutable `long[]` -> `recordPasswordFailure` now
  publishes a fresh `long[]` on every update (also resolves Claude item 3).
- Gemini MEDIUM (ServerSecurity.java:212) NPE on null stored password -> added `su.getPassword() == null`
  guard folded into the invalid-credentials branch.
- Claude item 2 (unbounded failure map from attacker-controlled user names) -> failure map is now keyed
  by a bounded 64-bit SHA-256 prefix of the user name.

## Skipped with rationale
- **Claude item 1 - account-lockout DoS / make threshold+window configurable / let correct password
  through during lockout.** The issue explicitly asks to mirror the API-token lockout, which is
  hard-coded `5` / `30_000` and has the identical DoS property. "Let a correct password through during
  lockout" would contradict the acceptance criterion ("test asserts lockout after N failures") and the
  passing `shouldLockOutAfterRepeatedPasswordFailures` test. The tradeoff is now documented explicitly
  in `docs/5029-auth-hardening.md`; IP keying and configurable thresholds are recorded as follow-ups.
- **Claude item 4a - extract a shared `recordFailure(map, key, windowMs)` helper for the token and
  password paths.** Optional DRY refactor that would modify the already-tested API-token code path.
  Kept out of scope to limit blast radius; the two lambdas are now both small and documented.
- **Claude item 4b - username length now enforced by `validateCredentials` on the server-command
  create-user path.** This is an intended consistency improvement (both create-user paths share one
  policy), not a regression. Flagged only as a behavior note.
- **Claude item 4d - confirm `docs/5029-auth-hardening.md` is meant to ship.** The tracking doc under
  `docs/<issue>-<name>.md` is required by the resolve-issue skill contract and matches existing
  `docs/NNNN-*.md` files in the repo. Kept.

## Not reproduced / no action
- **Claude item 4c - no test for lockout expiry / cleanup-timer purge.** The window is a hard-coded 30s
  constant with no clock seam; a real-time test would be a 30s `@slow` test and reflection-based seeding
  is fragile. The reset-after-window branch is exercised indirectly by the counter-reset test. Left
  uncovered by design, matching the reviewer's own "understandable" note.
