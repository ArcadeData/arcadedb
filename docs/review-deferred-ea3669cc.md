# Review cycle 1 (ea3669cc) - items not applied

One item from the `claude` review on `ea3669cc64be09ec6b787c69c1b5fe411fd89dc3` was deliberately not
applied. Everything else it raised was actionable and is in the follow-up commit.

## Not applied: a typed 504 for "connection refused" as well

> The "connection refused" (leader process down, port not listening) case still falls through to the
> generic `IOException`/500 path rather than getting a typed 504 like the connect-timeout and
> response-timeout cases do. [...] just flagging it as a possible follow-up if operators end up
> wanting a consistent 504 shape for every leader-unreachable case.

**Disposition: declined for this PR, and not filed as an issue either.** The reviewer flagged it as a
possible follow-up rather than a defect, and the reason it is outside this issue is structural rather
than a matter of effort.

This issue is about a wait that never ends. A refused connection is the opposite: the kernel answers
`ECONNREFUSED` on the first round trip, the worker thread is returned immediately, and nothing is
parked. It cannot violate the invariant this PR establishes, so it is not a blank row in the
completeness table - it is a row that cannot reach the bug.

What is left is a consistency argument about response shape: three ways of failing to reach the
leader, two of which now answer 504 and one of which answers 500. That is a real if minor wart, but
turning it into a 504 is a client-visible status change to a path that is behaving correctly today,
and it is not obviously right - a 500 for "the leader process is not running" is arguably more honest
than a gateway *timeout* for something that did not time out. That decision wants an operator with a
concrete complaint behind it, which is exactly the kind of speculative issue that should not be filed
pre-emptively.

If it is wanted, the change is one arm in `LeaderCommandForwarder.Transport.send`, next to the two
that already exist.

## Applied from the same review

- `HA_PROXY_CONNECT_TIMEOUT`'s rewritten description named two live readers and there are three:
  `RaftHAPlugin.authSessionRpcTimeoutMs()` reads it as the budget for a whole peer auth-session RPC.
  Verified (`grep -rn 'HA_PROXY_CONNECT_TIMEOUT' --include='*.java' */src/main/java`) and corrected -
  a description rewritten to be authoritative has to actually be exhaustive.
- The `HttpConnectTimeoutException` arm's position is now called out as load-bearing at the `if`,
  naming the test that would catch a reorder.
- The duplicated 504 description is now `SpecBuilders.LEADER_FORWARD_TIMEOUT_DESCRIPTION`.
- Import ordering in `LeaderCommandForwarder`.
