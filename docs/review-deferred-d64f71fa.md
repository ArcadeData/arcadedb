# Review cycle 3 (d64f71fa) - dispositions

Five items from the `claude` review on `d64f71fac0a704bf9db6194c4327061a6e898c69`, none blocking.
Three applied, one verified and turned into an assertion, one recorded.

## Applied

1. **Stray blank line splitting the `java.*` import block** in `LeaderCommandForwarder` - removed. It
   predated this PR, but the PR moved imports around it, so it is this PR's to tidy.
2. **`SpecBuilders.LEADER_FORWARD_TIMEOUT_DESCRIPTION` hardcoded the setting keys** as string
   literals while `Transport.gaveUp`/`couldNotConnect` a few lines away used `getKey()`. It now reads
   `GlobalConfiguration.HA_PROXY_READ_TIMEOUT.getKey()` and
   `HA_PROXY_LONG_COMMAND_TIMEOUT.getKey()`, so a rename cannot leave the OpenAPI string stale.
5. **The 0-clamps-to-1ms behaviour was silent.** It is deliberate - 0 must not be a way back to the
   unbounded wait - but an operator who typed 0 expecting "no timeout" would have got an unexplained
   wall of 504s. `Transport.bounded` now logs one WARNING naming the setting, the value and the clamp.
   Once per `Transport`, because the response deadline is re-read on every forward.

## Verified, and now asserted

3. **"Does `pending.cancel(true)` actually tear the connection down, or only release the worker?"**
   The right question, and it was not covered. It is now:
   `aLeaderThatAcceptsAndNeverAnswersIsGivenUpOnAndAnswered504` reads from the stalled leader's
   accepted socket after the 504 and asserts the client end closes within 10 s. It closes in about
   200 ms.

   Two mutations show why the assertion is not vacuous, and that the teardown has two independent
   causes rather than one:

   | Mutation | Socket still closed? |
   |---|---|
   | remove `pending.cancel(true)` | yes - the request-level `.timeout()` aborts the exchange at the JDK level |
   | remove `.timeout(...)` from `newRequest`, keep the cancel | yes - cancelling the future tears the exchange down |

   So neither mechanism is load-bearing alone for teardown, which is a stronger answer than the one
   the review asked for. Both are kept: the future's deadline is the only one that covers a body that
   stalls after the headers, and the request timeout is the only one that acts when this thread is not
   the one waiting.

## Recorded, no change

4. **One `HttpClient` per `HttpServer` instead of one JVM-wide static** - "in case any harness
   bypasses the normal stop path". Checked: `ArcadeDBServer.stop()` reaches `httpServer::stopService`
   through `CodeUtils.executeIgnoringExceptions`, and `Issue7507ForwarderClientLifecycleTest` drives a
   real start/stop and asserts the client is terminated (it fails if the wiring is removed). A harness
   that leaks a server without stopping it leaks the databases and the Undertow worker pool too, which
   is a much larger leak than one selector thread - so the client is not the thing that would surface
   such a harness, and making it a special case would not help. Left as is.
