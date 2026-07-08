# Resolved Bolt driver-version matrix

Concrete driver versions consumed by `.github/workflows/bolt-nightly.yml`. The
band names come from `spec.yaml` `driver_version_bands`; the versions below were
resolved against each package registry. The newest patch of each maintained
minor line is chosen; the `latest`/`latest-6.x` band tracks the newest release
so a driver-side release that breaks compatibility is caught by the nightly run.

When a driver publishes a new release that shifts a band, update the version
here and in the matching workflow matrix.

This table is the single source of truth for the expected cell set: the nightly
`merge-matrix` step parses these rows (`merge_matrix.py --expect-from`) as the
`language:version` cells that must be present. Keep the `| language | band |
version |` column order intact - a cell in the job matrices but absent here is
reported as an unexpected cell, and vice versa as a missing cell.

| Language   | Band                  | Version  |
|------------|-----------------------|----------|
| java       | oldest-supported-4.x  | 4.4.20   |
| java       | latest-5.x            | 5.28.5   |
| java       | latest-6.x            | 6.2.0    |
| javascript | oldest-supported-4.x  | 4.4.11   |
| javascript | latest-5.x            | 5.28.3   |
| javascript | latest-6.x            | 6.2.0    |
| python     | lts                   | 5.28.4   |
| python     | current               | 6.1.0    |
| python     | latest                | 6.2.0    |
| csharp     | lts                   | 5.26.2   |
| csharp     | current               | 5.28.4   |
| csharp     | latest                | 6.2.1    |
| go         | lts                   | 5.27.0   |
| go         | current               | 5.28.0   |
| go         | latest                | 5.28.4   |

Notes:
- Java `oldest-supported-4.x` (4.4.20) runs through the `e2e`
  `bolt-driver-legacy` Maven profile (`RemoteBoltLegacyDriverIT`); the other two
  Java bands run `RemoteBoltDatabaseIT` with `-Dneo4j-driver.version=`.
- The Go driver ships a single current major line (`neo4j-go-driver/v5`); its
  three bands are three points along that line.
- The C# `lts` band floors at 5.26.2, not a 4.x line: the .NET `Neo4j.Driver`
  made breaking API changes in 5.0 (`ExecuteWriteAsync`, `Bookmarks`,
  `LastBookmarks`, `INode.Get<T>`, `IAsyncDisposable`), so the shared test suite
  cannot compile against 4.4.x. Do not lower this band below 5.0 without a
  dual-API rewrite of `e2e-csharp`.
- Repo PR-run pins (single version each): java 6.2.0, javascript 6.0.1,
  python 6.2.0, csharp 6.2.1, go 5.28.4. The nightly widens each to the full
  band set above.
