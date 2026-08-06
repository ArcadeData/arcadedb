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

### Follow-up: the chain-length gap, and a matching guard for SQL

Two things turned out to be wrong in the paragraph above, found while auditing for the same class of bug
elsewhere in both engines.

First, the Cypher fix was incomplete. `ExpressionRewriter`'s guard only fires from
`CypherASTBuilder#visitWhereClause`, so a long OR/AND/NOT/comparison/arithmetic chain written anywhere
*other* than a top-level `WHERE` condition - a `RETURN` projection, an `ORDER BY` item, a function
argument - was never rewritten and so never hit that guard. A 30000-term OR chain in a `RETURN` projection
still overflowed the stack, this time inside `CypherSemanticValidator#checkExpressionScope` - a completely
different recursive walker from `ExpressionRewriter`, invoked during semantic validation on every clause.
Chasing down and patching every such walker individually is exactly the kind of fragile, incomplete-prone
fix that let this one through in the first place, so `CypherExpressionDepthGuard` (the same `ParseTreeListener`
already attached to the parser for nesting depth) now also bounds the term count of every `(OP operand)*`-shaped
grammar rule directly - OR, XOR, AND, `NOT*`, chained comparisons, and the three arithmetic precedence
levels - using each rule's own generated accessor, exactly as every AST builder does. This rejects an
oversized chain during parsing, before any tree is built, regardless of which clause it is in and regardless
of which pass would eventually have walked it.

Second, "the SQL parser was never at risk... not because it enforces a limit of its own" undersold the
actual risk: it isn't at risk of a `StackOverflowError` from nested parentheses (confirmed - its grammar
costs far fewer Java stack frames per nesting level, exactly as claimed), but it is at risk of something
worse. The production SQL parser is `SQLAntlrParser` (ANTLR4-based, like Cypher's), which resolves the
ambiguity between the several grammar rules that all start with a bare `(` - a parenthesized expression,
condition, or sub-statement - by trying a fast SLL prediction first and falling back to full ALL(*)
prediction on failure. For deeply nested parentheses that fallback's cost grows steeply enough that a query
of only a few KB (about 6000 nested parentheses) tied up a worker thread for well over two minutes of CPU
without ever crashing - worse than a fast error, since a slow hang is not distinguishable from a legitimately
slow query. `SQLAntlrParser` now counts parenthesis nesting on the token stream before attempting any parse,
rejecting a query past `arcadedb.sql.maxExpressionDepth` (default 200) in O(n) time - the same depth that
previously burned minutes of CPU is now rejected in single-digit milliseconds. Counting on the token stream
rather than raw characters means a `(` inside a string literal or comment is not miscounted.
