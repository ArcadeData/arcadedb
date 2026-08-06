# ArcadeDB v.26.9.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.9.1 development cycle, so the release notes are ready at tag time.

## A pathologically nested or long Cypher expression is now a parse error, not a StackOverflowError (#5851)

A ~2KB `WHERE` clause with about 1000 nested parentheses crashed the parsing thread with a
`StackOverflowError`: the ANTLR-generated Cypher parser re-enters its `expression` grammar rule, and walks
its full ~10-level operator-precedence cascade, once per nesting level, so a few thousand levels is enough
to exhaust the default JVM thread stack. `AbstractServerHttpHandler` catches `Throwable`, so this degraded
an HTTP request to a 500 rather than killing the worker, but a malformed query should fail with an ordinary
400, and letting an `Error` unwind through the engine on a path no database state has touched is worth
avoiding on its own.

Investigation found the same crash from two independent recursion sites, not one. Nested parentheses (and
equally, list/map literals or function arguments nested inside one another) recurse in the ANTLR-generated
parser itself. A long *flat* chain - thousands of `OR`'d or string-concatenated terms - does not: the
grammar's `(OR expression11)*` and `(PLUS expression5)*` productions are quantifier loops, not
self-referencing rules, so they parse without recursing. They still crashed, one level further down the
pipeline: `ExpressionRewriter`, the shared visitor every `WHERE` condition is normalized/folded/simplified
through, walks the resulting deep expression tree recursively.

Both sites are now bounded by the same new setting, `arcadedb.cypher.maxExpressionDepth` (default 200),
converting either crash into a `CommandParsingException` that names the limit and the setting to raise if a
legitimate query needs it. Real-world queries essentially never nest this deep, so the default leaves a
wide margin; the SQL parser was never at risk from the same input because its hand-written recursive-descent
implementation costs far fewer stack frames per nesting level, not because it enforces a limit of its own.
