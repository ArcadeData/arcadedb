# Issue #5029 - Authentication hardening

## Symptom
Server `security` module has four password/credential hardening gaps (2026-07 audit):

1. **MEDIUM** - `ServerSecurity.authenticate()` (Basic-auth / `/api/v1/login`) has no failed-attempt
   counting or lockout. Brute-force protection existed only for API tokens
   (`authenticateByApiToken`). Password guessing against `root` or any user was unthrottled.
2. **MEDIUM** - `DefaultCredentialsValidator.generateRandomPassword()` returned an 8-hex-char
   truncated UUID (~32 bits), used to auto-generate the `root` password on first run.
3. **LOW** - Inconsistent + off-by-one create-user policy: `PostServerCommandHandler.createUser`
   enforced `length < 4` with message "must be 5 minimum characters", while `PostUserHandler`
   enforced min length 8.
4. **LOW** - `passwordMatch()` used `String.equals` (short-circuits => timing side channel), while
   the cluster-token path already used `MessageDigest.isEqual`.

## Root cause
Brute-force / entropy / constant-time protections were added only to the newer API-token path and
never back-ported to the original password path; two create-user entry points evolved independently.

## Fix
1. Added a per-principal password-failure map (`passwordFailures`) mirroring the API-token
   `tokenFailures` mechanism: lockout after `MAX_PASSWORD_FAILURES` (5) failures for
   `PASSWORD_LOCKOUT_MS` (30s), keyed by user name. Checked before lookup, incremented on
   unknown-user and wrong-password, cleared on success. Stale entries purged by the existing
   cleanup timer.
2. `generateRandomPassword()` now draws 24 chars from a 62-char alphanumeric alphabet via
   `SecureRandom` (~143 bits, >=128 bits).
3. `PostServerCommandHandler.createUser` now routes through `credentialsValidator.validateCredentials`
   (min length 8, correct message) via a new `ServerSecurity.getCredentialsValidator()` accessor.
4. `passwordMatch()` compares encoded hashes with `MessageDigest.isEqual` (constant-time).

## Keying decision (Defect 1)
Lockout is keyed by user name, mirroring the API-token path exactly (which keys by token-hash
prefix, no IP). Threading source IP would require changing the `authenticate` signature across 9
call sites in 6 modules (http, bolt, postgresw, grpcw, mongodbw, mcp); the acceptance criterion only
requires per-principal lockout "like API tokens". IP-scoped keying is noted as a possible future
refinement.

## Tests
- `ServerSecurityAuthHardeningTest` (new): password lockout after N failures, throttle message,
  lockout does not apply to a different user, successful auth clears the counter, constant-time
  compare correctness.
- `DefaultCredentialsValidatorTest`: new entropy/length assertions for `generateRandomPassword`;
  updated the pre-existing `hasSize(8)` assertion (it codified the vulnerable behavior).

## Impact
Password brute-forcing is now throttled identically to API tokens; auto-generated root passwords
carry >=128 bits; both create-user paths share one min-length-8 policy; password comparison is
constant-time. No change to the wire/HTTP contract beyond a stronger generated password and the
corrected create-user error message.
