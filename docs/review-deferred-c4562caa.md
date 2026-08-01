# Review notes - PR #5689, head `c4562caa`

Points from the `claude[bot]` review that were **not** applied, with the reasoning. Nothing here is
blocked on the developer; each was assessed and closed out.

## 3. `GlobalConfiguration.LOG_IMPL.reset()` does not re-install a logger - **skipped**

> Comment: "`reset()` leaving the swapped logger in place is a mild surprise for anyone expecting `reset()`
> to be a full undo - the doc note handles it, so this is informational only."

Correct, and the bot itself flagged it as informational. `GlobalConfiguration.reset()` deliberately does not
run callbacks for **any** setting (it assigns `defValue`, or `callbackIfNoSet` when one exists), so making
`LOG_IMPL` re-install a logger would mean either a special case in `reset()` or a callback contract change
touching every other entry. That is out of scope for this issue. The limitation is documented in
`docs/5543-log-impl-global-configuration.md` and the test's `tearDown` restores the original logger
explicitly.

## 4. Lambda callback vs anonymous `Callable` - **skipped, claim is inaccurate**

> Comment: "The new entry uses a lambda `value -> { ... }` while every other callback in
> `GlobalConfiguration` uses `new Callable<>() { ... }`."

Checked against the file: it has **5** lambda callbacks against **3** anonymous `Callable`s.
`DUMP_CONFIG_AT_STARTUP` (the very first entry), `DATE_IMPLEMENTATION` and `DATE_TIME_IMPLEMENTATION` all
use `value -> { ... }`. The lambda is the more common local convention, not a departure from it, so
"adhere to existing code" argues for keeping it.
