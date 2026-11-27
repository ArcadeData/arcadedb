# VEX statements

`arcadedb.openvex.json` is the OpenVEX document for ArcadeDB. It records vulnerabilities that a scanner reports in
something we ship but that do not affect ArcadeDB. It is used in two places:

- the release gate (`preflight` in `mvn-release.yml`) and the nightly scan (`supply-chain-scan.yml`) pass it to grype,
  so a vulnerability listed here as `not_affected` no longer fails them;
- every GitHub release publishes it as `arcadedb-<version>.openvex.json`, so customers can feed it to their own
  scanners and see our assessment next to the finding.

A statement is a public claim, made by Arcade Data, that customers rely on. Treat it like a security fix.

## Where findings come from

- **The nightly scan** opens (or comments on) the issue "Nightly supply-chain scan failed". The job summary and the
  `nightly-scan` artifact list every finding (the release gate uploads `preflight-scan`).
- **The release gate**: `preflight` fails before anything is published. Nothing needs undoing; fix the finding and
  dispatch the release again.

Only a Critical with a fix available fails either one. Everything else is reported in the job summary.

## Fix first, VEX second

Upgrade the dependency whenever a fixed version exists and the upgrade is safe for the release. That is always the
preferred outcome, because it needs no argument.

Add a VEX statement only when the upgrade is not possible in time (no compatible fixed version, the library is
embedded in a third-party jar, the release cannot wait) **and** ArcadeDB is genuinely not affected. If you cannot
explain why ArcadeDB is not affected, it is affected: fix it or postpone the release.

| Situation | Action |
|---|---|
| Fixed version available, upgrade is safe | Upgrade. No VEX statement. |
| Vulnerable code is never called by ArcadeDB | VEX `not_affected`, `vulnerable_code_not_in_execute_path` |
| The vulnerable function is not in the shipped jar | VEX `not_affected`, `vulnerable_code_not_present` |
| The component is listed but not shipped | VEX `not_affected`, `component_not_present` |
| Reachable, but no input an attacker controls gets there | VEX `not_affected`, `vulnerable_code_cannot_be_controlled_by_adversary` |
| A mitigation in ArcadeDB blocks the attack | VEX `not_affected`, `inline_mitigations_already_exist` |
| Still being analyzed, or ArcadeDB is affected | No VEX statement. `under_investigation` and `affected` do not silence the gate. |

## Add a statement

One pull request per vulnerability, so each assessment is reviewed, and reverted, on its own. A maintainer other
than the author approves it.

1. Take the vulnerability ID and the package identifier exactly as grype reports them (job summary or
   `grype ... -o json`: `.vulnerability.id` and `.artifact.purl`).
2. Add the statement:

   ```bash
   vexctl add --in-place --document vex/arcadedb.openvex.json \
     --product "pkg:maven/io.netty/netty-handler@4.1.136.Final" \
     --vuln GHSA-c4c3-7fpv-j4q5 \
     --status not_affected \
     --justification vulnerable_code_not_in_execute_path \
     --impact-statement "Explain, for a customer's security team, why ArcadeDB is not affected."
   ```

   The values above are an example of the format, not an assessment of that advisory.

3. Check it locally:

   ```bash
   .github/scripts/check-vex.sh vex/arcadedb.openvex.json
   grype sbom:package/target/sbom/full/arcadedb-<version>.cdx.json \
     --vex vex/arcadedb.openvex.json --only-fixed --fail-on critical
   ```

   The second command must no longer report the finding. Build the SBOM first with
   `./mvnw -Pdeploy -DskipTests -pl package -am package`, or run `.github/scripts/release-scan.sh <version>`, which
   also scans the images.

The `vex-validate` workflow rejects a pull request whose `not_affected` statement lacks a `justification` or an
`impact_statement`.

### The `--product` value

`--product` is the **vulnerable package**, not the ArcadeDB artifact that contains it:

- use the package URL grype reports, with the exact version: `pkg:maven/io.netty/netty-handler@4.1.136.Final`.
  Grype matches it with or without the `?type=jar` qualifier;
- do not use the ArcadeDB coordinate (`pkg:maven/com.arcadedb/arcadedb-engine@...`), with or without
  `--subcomponents`: grype ignores it and the finding stays;
- do not drop the version: `pkg:maven/io.netty/netty-handler` also matches, and silently covers every future version
  of the library, including ones where the assessment no longer holds.

This was checked with grype 0.122.1, the version the workflows pin, against both an SBOM scan and an image scan.
Re-check it when the grype pin changes.

### The `--vuln` value

Use the ID grype reports (often a `GHSA-...` ID for Java libraries). If the advisory also has a `CVE-...` alias, a
second statement with the CVE ID does no harm and helps scanners that report the CVE.

### The impact statement

Customers read it. Say what the vulnerable code does, why ArcadeDB never reaches it (or why the attacker cannot
control it), and how that was checked. "Not used" is not an impact statement.

## Remove a statement

Once the dependency is upgraded past the vulnerable version, the statement no longer matches anything. Remove it in
the upgrade pull request, so the file only lists assessments that are still in force. Also remove or revisit a
statement when the reason in its impact statement stops being true, for example when ArcadeDB starts using the
vulnerable feature.
