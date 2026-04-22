# Fix #3952: EXISTS { MATCH } subquery returns false for relationship types containing Cypher keyword fragments

## Summary

`EXISTS { MATCH (p)-[:WORKS_WITH]->(:Person) }` returned `false` for all rows even when the pattern matched,
while the equivalent `WHERE (p)-[:WORKS_WITH]->()` predicate worked correctly.

## Root Cause

`ExistsExpression.matchesKeywordAt()` used `Character.isLetterOrDigit()` to detect word boundaries when
scanning the subquery string for Cypher clause keywords (WITH, WHERE, RETURN, ...).

Because underscore `_` is NOT a letter or digit in Java, the check incorrectly accepted it as a word
boundary. This caused "WITH" inside "WORKS_WITH" to be falsely recognised as the Cypher `WITH` clause
keyword, making `injectWhereConditions()` split the relationship type name and produce an invalid query
such as:

```
MATCH (p), (p)-[:WORKS_WHERE id(p) = $__exists_p WITH]->(:Person)
```

The invalid query threw an exception that was silently caught in `evaluate()`, which returned `false`.

The bug affected any relationship type whose name ends with a Cypher keyword after an underscore:
`_WITH`, `_WHERE`, `_RETURN`, `_ORDER`, `_SKIP`, `_LIMIT`, `_UNION`.

## Fix

`engine/src/main/java/com/arcadedb/query/opencypher/ast/ExistsExpression.java`

Replaced `Character.isLetterOrDigit(c)` in the boundary checks of `matchesKeywordAt()` with a new helper
`isCypherIdentifierChar(c)` that also returns `true` for `_`, matching Cypher identifier rules.

## Tests

New test class: `engine/src/test/java/com/arcadedb/query/opencypher/CypherExistsUnderscoreRelationshipTypeTest.java`

- `existsWithUnderscoreKeywordWithInRelationshipType` - reproduces issue #3952 (WORKS_WITH embeds "WITH")
- `existsWithUnderscoreKeywordWhereInRelationshipType` - KNOWS_WHERE embeds "WHERE"
- `existsWithSimpleRelationshipTypeStillWorks` - control: KNOWS (no embedded keyword) still works

All 3 new tests pass. All 5835 existing Cypher/OpenCypher tests continue to pass.
