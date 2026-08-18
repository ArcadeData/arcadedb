# #6363: what a vertex's labels are, and what a label write is allowed to change

Issue: https://github.com/ArcadeData/arcadedb/issues/6363

## What happened

`Labels.getLabels(Vertex)` decided a vertex's labels from one question - "does the type have supertypes?" -
and answered:

- **no supertypes** -> the type's own name is the label,
- **supertypes** -> the supertype names are the labels, and the type's own name is not one.

That rule is right for exactly one shape, the synthetic `A~B` composite the multi-label support creates:
its own name is an encoding, and its supertypes really are the labels. Applied to ordinary ArcadeDB type
inheritance it is wrong in both directions:

| shape | `labels(n)` before | `MATCH (n:...)` matched |
|---|---|---|
| `Manager EXTENDS Employee` | `["Employee"]` | `:Manager` and `:Employee` |
| `Special EXTENDS ` \`Author~Topic\` | `["Author~Topic"]` | `:Author` and `:Topic` |

So the engine matched a node by a label and then reported a label list not containing it, and an internal
type name leaked into a result set.

`getLabels` is not only the `labels()` function. `SetStep`, `RemoveStep` and `MergeStep` build the **new**
type from it, so the wrong list was written back: `MATCH (n:Manager) SET n:Extra` moved the record to a
freshly invented `Employee~Extra`, and `MATCH (n:Manager)` then returned **0 rows**. Adding a label removed
one, silently.

## The rule now

Two different questions, two methods, because a relabelling does not want the same answer a reader does.

**`Labels.getLabels`** - what the node answers to. Its own type name and every ancestor's, sorted, minus
the synthetic composite names. `Manager` -> `[Employee, Manager]`; `Special` -> `[Author, Special, Topic]`;
a bare `Author~Topic` -> `[Author, Topic]`, exactly as before. The invariant is Neo4j's and is what the fix
is measured against: **`L IN labels(n)` and `n:L` answer the same question**, before and after a write.

Two names are special and only one of them is filtered here:

- a **composite** name (`A~B`) is an encoding, so the walk goes *through* it to the labels it encodes. Which
  types those are is decided **structurally**, not by looking for a `~`: a composite's name is exactly the
  deduplicated, sorted, separator-joined names of its own supertypes, which is what `ensureCompositeType`
  writes and what nothing else produces. A type somebody created and called `a~b` keeps its name and is a
  label like any other - under the name heuristic alone it would have lost its own name from `labels()` and,
  worse, from the set a relabelling rebuilds its type out of, so the next `SET` would have moved the vertex
  out of it.
- `V`/`Vertex` is the type a node lands in when it carries no label at all, so a node whose **own** type is
  `V` reports an empty list. A *supertype* called `V` is not filtered: `V` is an ordinary label a query may
  write, and the openCypher TCK does write it (`CREATE (b:U:V:W:X:Y:Z)`).

**`Labels.getOwnLabels`** - what a relabelling must reproduce. The minimal set that rebuilds the current
type: the type's own name, or, for a composite, the labels it encodes. `Manager` -> `[Manager]`, *not*
`[Employee, Manager]`, because `Employee` comes back on its own through the hierarchy and naming it in the
composite instead of the subtype that carries it is precisely what dropped the subtype.

So `MATCH (n:Manager) SET n:Extra` now builds `Extra~Manager`, extending both. The vertex is still a
`Manager`, therefore still an `Employee`, and now an `Extra`. Every label predicate that held before the
write still holds after it.

Adding a label the vertex already answers to - its own, or an inherited one - changes nothing and is not
counted in `labels-added`. It is not a no-op that hides a failure: the label *is* present afterwards.

## The one thing that cannot be done, and is now said out loud

`REMOVE n:Employee` on a vertex of type `Manager EXTENDS Employee` has no correct outcome. No type the
vertex could be moved to answers *no* to `:Employee` while still answering *yes* to `:Manager` - the schema
says one implies the other. The three candidate behaviours were:

- **move it to `Employee`'s sibling-less remainder** (what the old code effectively did, via a label list
  that had already lost `Manager`): destroys the subtype to satisfy a removal. Worst of the three.
- **silently do nothing**: leaves `n:Employee` true after `REMOVE n:Employee`, which is the same class of
  lie this issue is about, one clause over.
- **refuse, with a message naming both labels**: chosen. `CommandSemanticException` (HTTP 400), because the
  query is not honourable against this schema and no retry will change that.

Removing a label the vertex simply does not have stays a no-op, as in Neo4j. Removing *both* the subtype
and the label it implies (`REMOVE n:Manager, n:Employee`) is fine and leaves an unlabelled node: nothing
that remains implies `Employee`.

This interacts with [#6335](6335-cypher-label-write-cost.md): a label write still moves the record and is
still O(degree). What changes here is only *which* type it moves to.

## The two small items

**A disjunction's cardinality estimate.** `NodeByLabelDisjunctionScan` scans every type any alternative
accepts, but its estimate came from the anchor selector's single-label lookup - the first alternative
alone - so `(n:A|B)` was priced as if only `:A` existed and looked like the cheap side to drive a join
from. `AnchorSelector` now sums the count over exactly `Labels.matchingVertexTypes`, the same list the scan
walks, counted non-polymorphically so a subtype answering to two alternatives is charged once. A
disjunction anchor is also no longer offered an index seek it could never use: `IndexSelectionRule` builds
the disjunction scan whatever the estimate claims, so claiming a seek only mis-priced the plan it produces.

**`Schema.getTypeOrNull`.** `getType` throws on an absent type, so every caller that could tolerate absence
wrote `existsType(name) ? getType(name) : null` - two probes of the same map, and an exception used as
control flow whenever somebody forgot the guard. `Schema` now has a non-throwing accessor. It is a
`default` method spelled as that same pair, so an out-of-tree `Schema` keeps compiling; `LocalSchema` and
`RemoteSchema` override it with a single lookup. The `existsType(x) && getType(x) instanceof Y` idiom
collapses to `getTypeOrNull(x) instanceof Y`.

## Affected components

- `com.arcadedb.query.opencypher.Labels` - `getLabels`, new `getOwnLabels` and `impliedBy`,
  `matchingVertexTypes` made visible to the cost model
- `com.arcadedb.query.opencypher.executor.steps.SetStep` / `RemoveStep` / `MergeStep` - the write side
- `com.arcadedb.query.opencypher.optimizer.AnchorSelector`, `statistics.StatisticsProvider`,
  `statistics.CostModel` - the disjunction estimate
- `com.arcadedb.schema.Schema` / `LocalSchema`, `com.arcadedb.remote.RemoteSchema` - `getTypeOrNull`

## Tests

- `CypherLabelsInheritanceIssue6363Test` - the read side under inheritance and under a type extending a
  composite, `SET`/`REMOVE` on a subtype vertex asserted through `MATCH` and not only through `labels()`,
  the refused inherited removal, the `V`-as-a-label and `a~b`-as-a-type-name cases, and the plain composite,
  which must keep behaving exactly as it did
- `CypherDisjunctionCardinalityIssue6363Test` - the summed estimate pinned through `EXPLAIN`, and
  `getTypeOrNull` on a present and an absent type
