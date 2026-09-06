# Deferred review item - PR #7212 (issue #6988), head `ed21c54`

One review finding was assessed and **declined with justification** rather than applied. Recorded here so the
developer can overrule it rather than having to rediscover it.

## The comment (CodeRabbit, inline on `LocalSchema.java:456`, "Stability & Availability / Major")

> **Gate query lookups during replacement initialization.** At line 456, `replaceLoadedComponent()` publishes the new
> `IndexInternal` to `indexMap` and the file registry. `getIndexByName()` reads `indexMap` directly, while the file
> lock does not cover `readConfiguration()` or `onAfterSchemaLoad()`. A concurrent vector query can obtain the
> replacement before `LSMVectorIndexMutable.onAfterSchemaLoad()` loads its vectors. Stage the replacement or extend
> the schema-apply gate through `readConfiguration()` and all hooks, then add a regression test that blocks
> `onAfterSchemaLoad()`.

## Assessment

The mechanism described is real: `replaceLoadedComponent()` publishes into `indexMap` and `files` before the
`onAfterLoad()` loop, `readConfiguration()` and the `onAfterSchemaLoad()` loop run, and `LSMVectorIndexMutable` is
the one component whose *only* non-empty hook is `onAfterSchemaLoad()` (it loads its vectors there, once
`readConfiguration()` has set its dimensions). So a concurrent vector search on a follower could, in principle,
reach a vector index whose vectors are not loaded yet.

**It is not something this PR introduces, and this PR strictly narrows it.** The full `LocalSchema.load()` this
change replaces does the same publish-before-initialize, and does it far more widely:

- `load()` opens with `indexMap.clear()` and repopulates it component by component, calling `registerFile()` and
  `indexMap.put()` for **every** index in the database *before* `initComponents()`, `readConfiguration()` and the
  `onAfterSchemaLoad()` pass;
- so during a full reload a concurrent `getIndexByName()` sees, for the whole duration, either `null` (an
  "index not found" error) or an index that has not run its schema hook yet;
- and on an HA follower that reload ran once **per applied schema entry**, which is precisely what #6988 is about.

`loadIncremental` narrows that exposure from "every index in the database, on every entry" to "the one index this
entry wrote pages into". The second reviewer on this PR reached the same conclusion independently: *"replaceLoaded
Component's single synchronized files.set(...) swap mirrors what load() already does for every component, so it
doesn't introduce a new concurrency hazard versus before this PR - it just narrows the surface."*

## Why it is not fixed here

Making the publication atomic with respect to readers is a change to the schema-load contract, not to this method:

1. It has to be applied to `load()` as well, or the window simply stays open on the fallback path (a retired file, a
   compacted index, a restart) - and `load()` is the path every database open goes through.
2. `readConfiguration()` sits *between* publication and the schema hooks by design: the hooks read what it sets (a
   vector index cannot load vectors before its dimensions are known). Staging the whole set of replacements behind a
   barrier therefore means staging the logical schema rebuild too, i.e. building a second `types`/`indexMap` graph
   and swapping it, which is a substantially larger design change than this issue.
3. The suggested regression test ("block `onAfterSchemaLoad()` and verify concurrent lookups cannot obtain the
   partially initialized replacement") can only be written against that barrier; there is nothing to assert today
   that would not equally fail on `main`.

Fixing it under this issue would mean shipping an untested rework of the load contract inside a performance fix. It
belongs in its own issue, together with the same treatment for `load()`.

## Suggested follow-up issue

*"Schema load publishes index components before their schema hooks run"* - covering both `LocalSchema.load()` and
`LocalSchema.loadIncremental()`, with a staged-swap design and a test that pins the barrier.
