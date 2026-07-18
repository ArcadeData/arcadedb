# Native image build

Stub created by Task 5 of the native-image spike (see `docs/superpowers/plans/2026-07-18-native-image.md`).
Task 9 expands this into the full build/run/size guide (prerequisites, target matrix, JVector SIMD
fallback note, Docker usage). This page currently covers only what Task 5 needed to lock in: the
polyglot-JS outcome and the Truffle blocklist relaxation it depends on.

## `package` builds the image, `compile` does not

`native/pom.xml` binds the `native-maven-plugin`'s `compile-no-fork` goal to the Maven `package`
phase. `mvn -Pnative -pl native -am -DskipTests package` therefore runs the actual `native-image`
build. `mvn -Pnative -pl native -am -DskipTests compile` only resolves and compiles the module's
(nonexistent) sources - the `package`-phase execution is never reached, so no `native-image`
process runs and no binary is produced. Use `compile` only as a fast dependency-resolution sanity
check, never to validate a native build.

## JS scripting: included and working

GraalJS ships embedded in the native binary and runs correctly - no fallback error, no missing
language. Verified against `native/target/arcadedb-26.8.1-SNAPSHOT-osx-aarch_64`:

```
$ curl -u root:PlayWithData123! -H 'Content-Type: application/json' \
    -X POST http://127.0.0.1:2480/api/v1/command/jstest \
    -d '{"language":"js","command":"40 + 2"}'
{"user":"root","result":[{"value":42}]}
```

This is now a permanent regression gate: `native/src/test/scripts/exercise.sh` sends this exact
request as a **hard** assertion (fails the script on a wrong/absent result), in the same style as
the SQL and Cypher round-trip checks - not a best-effort WARN-skip like the optional wire-protocol
checks further down the script. `smoke.sh` runs it against both the native binary and, since
GraalJS is also on the JVM build's classpath, the JVM `server.sh` distribution.

Known JVM-side quirk (unrelated to the native image itself, but relevant to keeping the shared
`exercise.sh` green on both paths): running the plain classpath-assembled Truffle/GraalJS jars
under `graalvm-community-openjdk-25`'s bundled JDK can throw `NullPointerException` from
`HotSpotTruffleRuntimeAccess.getCompilerVersion` while creating the shared polyglot `Engine`
(`Engine.create()`), because that JDK's built-in Graal compiler modules and the classpath
`truffle-compiler`/`truffle-runtime` jars don't resolve a compiler version consistently outside a
native image. `GraalPolyglotEngine.getSupportedLanguages()` swallows this as a caught `Throwable`
and logs "GraalVM Polyglot Engine: no languages found"
(`engine/src/main/java/com/arcadedb/query/polyglot/GraalPolyglotEngine.java:140-147`), so `js`
silently drops out of `Available query languages` and any `{"language":"js"}` command then fails
with `Query engine 'js' was not found`. The workaround is `-Dtruffle.UseFallbackRuntime=true`
on the JVM launch, which switches Truffle to its interpreter-only fallback runtime and avoids the
buggy compiler-version probe entirely (verified: `smoke.sh <server.sh> -Dtruffle.UseFallbackRuntime=true`
passes, including the JS assertion). This does not apply to the native image, where Truffle runs
through the SVM-integrated optimizing runtime built by the native-image Truffle feature, not
`HotSpotTruffleRuntimeAccess`.

## Truffle blocklist relaxation

`native/pom.xml` passes `-H:-TruffleCheckBlockListMethods -H:-TruffleCheckBlackListedMethods` to
the native-image build. Without these, the build aborts: six GraalJS builtins (`Atomics.*` and
`TypedArray.prototype.set`) reach `java.lang.invoke.MethodHandle.linkToStatic`, which Truffle's
partial-evaluation blocklist rejects as unsafe to runtime-compile ahead-of-time. The blocklist
exists because Truffle cannot statically prove those call targets deoptimize/compile soundly when
AOT-compiled into the image; relaxing it lets the build proceed with JS embedded, at the cost of
those specific builtins being unverified under Truffle's own soundness check when their call
targets get hot enough to runtime-compile.

### Are the flagged builtins actually reachable through ArcadeDB's polyglot surface?

Yes. `GraalPolyglotEngine`'s `Context.Builder` (`engine/src/main/java/com/arcadedb/query/polyglot/GraalPolyglotEngine.java:83-98`)
sandboxes host interop (denies `Class`/`ClassLoader`/reflection access, `IOAccess.NONE`,
`allowNativeAccess(false)`, `allowCreateProcess(false)`, `allowEnvironmentAccess(NONE)`,
`allowCreateThread(false)`, `allowPolyglotAccess(NONE)`, and a package allowlist for
`Java.type(...)` lookups) but sets no context/engine option that disables or restricts the
ECMAScript builtins themselves. `Atomics`, `SharedArrayBuffer`, and
`TypedArray.prototype.set` are standard JS globals, not host-interop surface, so none of that
sandboxing touches them.

Confirmed directly against the running native binary (`arcadedb-26.8.1-SNAPSHOT-osx-aarch_64`),
via the same `{"language":"js", ...}` command path used above:

| Script | Result |
|---|---|
| `typeof Atomics` | `"object"` |
| `var a = new Int32Array(4); Atomics.add(a, 0, 5); a[0]` | `5` |
| `var b = new Uint8Array(4); b.set([1,2,3],0); b[1]` | `2` |
| `typeof SharedArrayBuffer` | `"function"` |
| `var sab = new SharedArrayBuffer(16); var ia = new Int32Array(sab); Atomics.store(ia, 0, 99); Atomics.load(ia, 0)` | `99` |
| `var sab2 = new SharedArrayBuffer(16); var ia2 = new Int32Array(sab2); Atomics.wait(ia2, 0, 1, 100)` | `TypeError: Unsupported operation` (rejected cleanly - `allowCreateThread(false)` blocks the blocking-wait path; the server stayed up) |

**Conclusion: the residual risk from relaxing the Truffle blocklist is real, not moot.** Any
authenticated caller with query/command permissions can reach `Atomics.add`/`store`/`load`,
`SharedArrayBuffer` construction, and `TypedArray.prototype.set` through a plain `{"language":"js"}`
command or a polyglot SQL function - the sandboxing in `GraalPolyglotEngine` restricts Java/host
interop, not the JS language surface. `Atomics.wait` is the one operation that is not reachable in
practice, because it requires real thread blocking and `allowCreateThread(false)` denies that; it
fails with a clean `TypeError` rather than crashing the process. Functional testing here did not
turn up a crash or unsound result from the other five builtins - the process survived
`/api/v1/ready` checks after every call - but a single functional smoke test does not validate
Truffle's own soundness concern (unverified behavior once these call targets get hot enough for
runtime compilation under sustained/adversarial load). This should stay documented as an accepted,
non-blocking risk for this experimental native-image add-on rather than something the JS regression
gate can detect.
