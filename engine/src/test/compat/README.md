# Backward-compatibility fixtures

`engine/src/test/resources/compat/db-<version>.zip` holds a database written by the **released** ArcadeDB engine of
that version. `com.arcadedb.graph.BackwardCompatibilityFixturesTest` opens every fixture with the current build and
checks edge counts, filtered and unfiltered walks, `isConnectedTo`, a clean `CHECK DATABASE`, and that new writes
survive a reopen (#9265).

Each fixture holds:

- two `Hub` vertices whose edge lists are promoted supernodes (a type-7 `StripeDirectory` head), mixing the edge
  types `Knows` (with a `w` property), `Likes`, `Parent` and the light edge type `Tags`. `hubIn` has one `Parent`
  edge among about 770 incoming entries;
- 500 ordinary low-degree `Person` vertices with a unique LSM index on `id`, and a 99-edge `Knows` chain between them.

The generator lowers `arcadedb.graph.supernodeThreshold` to 64 so the hubs promote while the fixture stays small,
and it fails if they did not promote.

## Regenerating a fixture, or adding the next release

Requirements: a JDK that can run the release (the single-file source launcher, `java Foo.java`, is used), Maven, and
network access to Maven Central.

```bash
engine/src/test/compat/generate-fixture.sh 26.9.1
```

The script builds a throwaway project that depends only on `com.arcadedb:arcadedb-engine:<version>`, so the
generator runs against that release's own classpath and never against the working tree. It refuses to run if the
engine on the classpath reports a different version. Extra arguments go to Maven, for example
`-Dmaven.repo.local=...`.

To add a release:

1. Run the script with the new version. It writes `engine/src/test/resources/compat/db-<version>.zip`.
2. Add the version to `BackwardCompatibilityFixturesTest.versions()`.

The generator only uses API that exists in every release it is run against. If the graph shape changes, change the
constants in `BackwardCompatFixtureGenerator.java` and `BackwardCompatibilityFixturesTest` together, then
regenerate every fixture. A fixture that was already published should normally stay as it is, because it records
what that release actually wrote. A regenerated fixture is not byte-identical to the committed one (the database
files carry timestamps and generated file names), even though the zip entry times are zeroed.

`.gitignore` ignores `*.zip` everywhere except in `engine/src/test/resources/compat/`.
