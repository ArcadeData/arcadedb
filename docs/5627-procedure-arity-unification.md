# #5627 - `Procedure.validateArgs()` reports a wrong argument count differently from functions

Follow-up from #5602 (PR #5612), which unified the function side and deliberately left the procedure side alone.

## Root cause

`Procedure` (`engine/src/main/java/com/arcadedb/function/procedure/Procedure.java`) declares its own
`validateArgs()` default method that hand-writes the count check and raises `IllegalArgumentException` with
wording of its own:

```
algo.dijkstra() requires exactly 4 argument(s), got 2
algo.dijkstra() requires 4 to 5 arguments, got 2
```

`Function` has since been reduced to `checkArity()` phrased through `FunctionArity`, which raises
`CommandSemanticException` (HTTP 400):

```
Function 'text.hammingDistance' expects 2 arguments but got 1
```

`CallStep.executeProcedure` rethrows `CommandParsingException` untouched and wraps everything else in
`CommandExecutionException`, so the procedure's `IllegalArgumentException` was wrapped and surfaced as a 500
while the identical mistake against a function surfaced as a 400.

Two further defects came with the hand-written check:

- it compares against the raw `getMaxArgs()`, so a procedure spelling "unbounded" the registry's way (`-1`)
  would have had every call rejected - `FunctionArity.effectiveMax()` exists precisely to resolve that. No
  procedure declares `-1` today, so this was latent rather than live.
- it dereferences `args.length` without a null guard, so a null argument array raised `NullPointerException`
  rather than the arity error. `Function.checkArity` counts `null` as zero arguments.

## Fix

- `FunctionArity` gains a kind-aware `message`/`mismatch` pair. The existing three-argument forms stay as the
  function-flavoured shorthand, so no existing caller changes.
- `Procedure` gains `checkArity(Object[])`, mirroring `Function.checkArity`: it reads the declared
  `getMinArgs()`/`getMaxArgs()`, resolves the maximum through `FunctionArity.effectiveMax()`, counts a null
  array as zero arguments, and raises `FunctionArity.mismatch("Procedure", ...)`. `validateArgs()` is kept as
  the name `CallStep` calls and now delegates to it.
- `Function.validateArgs`'s javadoc note recording why procedures were left out is replaced by a pointer to the
  procedure-side `checkArity`.

Resulting message, same sentence and same exception type as the function side:

```
Procedure 'algo.dijkstra' expects 4-5 arguments but got 2
```

### Why "Procedure", not "Function"

`FunctionArity.message` hard-codes the word `Function`. Reusing it verbatim would tell a caller that
`algo.dijkstra` is a function, which it is not - it has its own registry, its own `CALL` handling and its own
interface. The sentence shape, the accepted-count phrasing and the exception type (and therefore the HTTP
status) are what the issue asks to unify, and all three now match; only the noun distinguishes them, which is
also what Neo4j does.

### Why the ~80 in-`execute()` `validateArgs(args)` calls are kept

The issue asks whether the hand-written `validateArgs(args)` at the top of each `execute()` can be dropped now
that the base interface runs the guard. They are kept:

- unlike the function-side duplication `Function.checkArity` replaced, these are not a second hand-written
  count check that can drift - they call the same default method, reading the same declared bounds. Keeping
  them cannot make two paths disagree.
- `CallStep` is the only production caller that goes through `validateArgs()` first. `Procedure.execute()` is
  a public interface method, and dropping the guard would leave any direct caller (tests, embedded usage) with
  an `ArrayIndexOutOfBoundsException` instead of the client error.

Dropping them would touch ~80 files to remove a guard, for no behavioural gain.

## Tests

- `ProcedureInterfaceTest` - the existing arity cases asserted the old `IllegalArgumentException` wording, so
  they encode the behaviour this issue removes. Updated to the new contract (see "Existing tests" below), and
  extended with the unbounded-maximum, null-array and identical-to-the-function-side cases.
- `ProcedureArityIssue5627Test` (new, `engine/src/test/java/com/arcadedb/query/opencypher/`) - the end-to-end
  `CALL` path: the error reaches the client as `CommandSemanticException` rather than wrapped in
  `CommandExecutionException`, survives `OPTIONAL CALL`, and reads with the same sentence a function gets.
  Includes a sweep over the whole procedure registry so a procedure added later cannot reintroduce a
  divergent wording.

### Existing tests

Per `CLAUDE.md`, existing tests are not modified. `ProcedureInterfaceTest`'s five arity cases are the
exception, surfaced here deliberately: they assert `IllegalArgumentException` and the literal strings
`"exactly 2"` / `"2 to 4"`, which *are* the behaviour #5627 asks to replace. They were testing the correct
thing (the arity guard fires, with the right numbers) against the wording of the day, so the assertions are
retargeted at the new wording rather than deleted or loosened to accept both.

### Results

Before the fix, 7 of the 8 cases in `ProcedureArityIssue5627Test` failed (the eighth asserts a correct call still
runs, and passed on both sides). After it:

```
ProcedureArityIssue5627Test   8 tests, 0 failures
ProcedureInterfaceTest       10 tests, 0 failures
ProcedureRegistryTest        15 tests, 0 failures
FunctionInterfaceTest        13 tests, 0 failures

mvn -pl engine test -Dtest='com.arcadedb.query.opencypher.**,com.arcadedb.function.**'
  Tests run: 8918, Failures: 0, Errors: 0, Skipped: 98
```

The full reactor compiles (`mvn -DskipTests compile`). No source outside these tests referenced the old wording -
checked with a repo-wide scan for `Error executing procedure`, `requires exactly N argument(s)` and
`requires N to M arguments, got`.

## Impact

- Behavioural change visible to clients: a wrong argument count on a `CALL` of a registered procedure now
  returns HTTP 400 with `Procedure 'x' expects N arguments but got M` instead of HTTP 500 with
  `Error executing procedure: x`. This is the point of the issue.
- `OPTIONAL CALL` no longer swallows a wrong argument count, matching what #5602 already did for functions.
- No change to any procedure implementation, to `CallStep`, or to the function side.
