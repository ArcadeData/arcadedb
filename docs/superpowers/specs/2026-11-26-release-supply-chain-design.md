# Release supply chain: SBOM, signing, provenance, scan gate

Date: 2026-11-26
Status: draft, pending approval (see "Implementation decisions" below for where the build departed from the text above)

## Intent

A prospect serving US banks asked us to publish an SBOM and to sign release artifacts and container images. No
specific regime is mandated yet, so we target what both the EU and US regimes ask of the *pipeline*, where that costs
little:

- EU Cyber Resilience Act, as made concrete by BSI TR-03183-2 (SBOM format and fields).
- US EO 14028 / NIST SSDF (SP 800-218) / NTIA SBOM minimum elements / CISA guidance on VEX.

A bank's Java team consumes our jars from Maven Central; its platform team consumes our Docker images and release
archives. Both must be able to verify, offline from us, *what* is inside an artifact and *that we built it from tag X in
our CI*.

## Goals

1. Every release artifact has a machine-readable SBOM:
   - Maven Central: one CycloneDX SBOM (JSON + XML) per module, attached next to the jar, GPG-signed like every other
     attached artifact.
   - Distribution archives: the shipped assemblies (`full` and `-base`; `-minimal` and `-headless` are disabled in the
     `package` pom) each get their own SBOM of exactly what they bundle (jars + Studio npm tree where Studio is
     included, plus the components embedded inside shaded jars), shipped inside the archive (`sbom/`) and attached on
     Central. The GitHub release carries the same two archives, plus their SBOMs.
   - JVM image `arcadedata/arcadedb:<v>` and native images `<v>-native[-<arch>]`: an SBOM per image, attached in the
     registry as a signed attestation, and on the GitHub release.
   - Release-level SBOMs are published in both CycloneDX >= 1.5 JSON and SPDX 2.3 JSON.
2. SBOMs meet TR-03183-2 / NTIA minimum fields: supplier, component name, version, unique identifier (purl), hash
   (SHA-512 where the tool provides it, SHA-256 otherwise), license, dependency relationships, author, timestamp.
3. Every container image is signed with cosign keyless (Sigstore, GitHub OIDC identity), by digest, and carries SLSA
   build provenance (`actions/attest-build-provenance`, `push-to-registry: true`).
4. Every release archive (JVM distribution, native archives, OpenAPI/proto contract files) carries SLSA build provenance
   from the job that built it.
5. Every GitHub release has a `SHA256SUMS` file covering all its assets, signed twice: cosign keyless
   (`SHA256SUMS.sigstore.json`) and GPG with the existing Maven Central key (`SHA256SUMS.asc`). Banks that cannot use
   Sigstore still verify with GPG.
6. A vulnerability gate: Grype scans the aggregate SBOM and the JVM image before anything is published; a release fails
   on a **fixable Critical** that the VEX file does not mark `not_affected`. The same scan runs nightly on `main` so a
   release day is never the first time we see a finding.
7. A human-maintained OpenVEX file in the repo, published with each release and fed to the gate.
8. Docs: a "Verifying ArcadeDB releases" page (docs repo) with copy-paste `cosign verify`, `gh attestation verify`,
   `gpg --verify`, `sha256sum -c` commands; `SECURITY.md` gains a declared support period and a pointer to that page.

## Non-goals

- The CRA 24h exploited-vulnerability reporting process to ENISA and the CISA Secure Software Attestation form. These are
  company process / an officer's signature, not CI. Noted for the user; out of this design.
- Reproducible builds (`project.build.outputTimestamp`). Desirable follow-up, see "Known gaps".
- OpenSSF Scorecard, security.txt on the website. Follow-ups.
- GitHub "Immutable releases" (currently off for the repo). It forces a draft -> upload -> publish flow that the
  post-publish workflows do not follow. Follow-up issue (user decision, 2026-11-26).
- Signing snapshot / nightly builds. Only releases (and `attach_to_release` recovery runs) sign. Nightly scan only.
- Changing Maven Central jar signing: GPG stays as is.

## Anti-goals

- A release that publishes artifacts and then fails to sign them, leaving unsigned images under a release tag. Signing
  of an image happens in the same job, immediately after its push; a failure there fails the job, and the job's existing
  recovery path ("Re-run failed jobs") re-signs idempotently.
- A gate so noisy that the release manager disables it. Only fixable Criticals block; everything else is reported in the
  job summary. Suppression goes through the VEX file with a justification, never through a flag.
- Long-lived signing keys for cosign. Keyless only: no new secret to leak or rotate.
- Slowing every developer build: SBOM generation runs only under the `deploy` profile and in release/nightly workflows.

## Constraints

- Actions pinned by full SHA with a `# vX.Y.Z` comment (repo convention); Maven via `./mvnw`
  (`.github/scripts/check-maven-wrapper.py`).
- New tools must be Apache-2.0 compatible (CLAUDE.md allow-list): cyclonedx-maven-plugin (Apache-2.0),
  syft / grype / cosign / vexctl (Apache-2.0, CI-only, not shipped). The Studio SBOM is generated by a stdlib script
  (`.github/scripts/npm-lock-to-cdx.py`), not by `@cyclonedx/cyclonedx-npm`, whose transitive CC-BY-3.0 dependency is not
  on the license allow-list. ATTRIBUTIONS.md updated for the Maven plugin.
- `mvn-release.yml` two-job split (`central` then `docker-and-tag`) and its recovery story must hold. `docker-and-tag`
  must never be re-dispatched, only re-run.
- The GitHub release is created by a human after the tag exists; `native-image.yml` and `publish-contract.yml` react to
  `release: published` and upload their own assets concurrently. Anything that summarizes the release (SHA256SUMS) must
  run after they finish.
- Docker Hub is the registry; cosign signatures and attestations are stored there as OCI referrers / `sha256-<d>.sig`
  tags.

## Approaches considered

| | Approach | Trade-off |
|---|---|---|
| A | Maven-native only (cyclonedx-maven-plugin) | Best Java SBOM, lands on Central, auto GPG-signed. Covers neither images nor native builds; no provenance. |
| B | Workflow-only (syft + cosign + GitHub attestations on whatever is built) | No pom changes. Java SBOM inferred from jar contents: no dependency graph, weaker licenses. Nothing on Central. |
| **C** | **Hybrid: A for Maven + B for images/archives + aggregate release assets + scan gate** | More moving parts, but the only one that serves both Central consumers and image consumers and meets TR-03183 for every artifact. |

**Chosen: C** (user decision, 2026-11-26).

## Decisions (user, 2026-11-26)

- **The workflow, not a human, uploads the JVM archives to the GitHub release** (`release-assets.yml`, from the bytes
  the `central` job built and attested). The release manager creates the release with no files. Release runbook changes
  accordingly.
- **Accepted signer identity: the four signing workflows of this repo, on `refs/heads/main` or a release tag only.**
  Other workflows on `main` also hold `id-token: write` (load tests, resilience tests, benchmarks), so a `[^@]+`
  workflow match would accept certificates they mint. Every verify command (release-assets step 5, docs page) uses
  `--certificate-identity-regexp '^https://github\.com/ArcadeData/arcadedb/\.github/workflows/(mvn-release|native-image|release-assets|publish-contract)\.yml@refs/(heads/main|tags/[0-9][^/]*)$'`
  and `--certificate-oidc-issuer https://token.actions.githubusercontent.com`; `gh attestation verify` uses
  `--repo ArcadeData/arcadedb` and the same identity regexp. Adding a signing workflow later means updating the regexp
  and the docs. Consequence: `native-image.yml`
  signing/attesting steps refuse to run (fail with a clear error) when `attach_to_release` is dispatched from any ref
  other than `main`; the workflow comment recommending a feature branch for recovery is updated (the run still BUILDS
  the tag via `ref`, only the dispatch ref must be `main`).
- **Immutable releases: not now**, follow-up.

## Design

### 1. Maven: per-module and aggregate CycloneDX

- Add `cyclonedx-maven-plugin` to the root `deploy` profile:
  - `makeBom` per module at `package`: `outputFormat=all` (JSON + XML), `schemaVersion=1.6`, `includeLicenseText=false`,
    `includeTestScope=false`, `includeProvidedScope=false` for protocol modules so `arcadedb-server` (provided) is not
    reported as bundled. Attached as classifier `cyclonedx`, so `maven-gpg-plugin` signs it and
    `central-publishing-maven-plugin` ships it.
  - Organization / supplier metadata: `Arcade Data Ltd`, URL, so every SBOM carries the supplier field.
- `package` module: an aggregate BOM of what the distribution actually bundles (its runtime classpath), written to
  `target/sbom/arcadedb-<v>.cdx.json`, then merged with the Studio SBOM (step 2) and copied into the assembly under
  `sbom/`. The `-base` assembly gets the SBOM of its own subset.
- The `e2e`, `coverage` and benchmark-only modules are skipped (`<skip>true</skip>`) since they are never published.

### 2. Studio npm SBOM

- `.github/scripts/npm-lock-to-cdx.py` converts `studio/package-lock.json` into a CycloneDX SBOM of what ships
  (dev dependencies omitted), with no new npm dependency.
- `.github/scripts/merge-sbom.py` merges it into the distribution SBOM as a nested component `arcadedb-studio`, sets the
  supplier and timestamp, and lists the components embedded inside the jars (including second-level uber-jars such as
  the shaded Gremlin and `ratis-thirdparty` jars, read from the `pom.properties` inside them), so a scan of the SBOM
  sees what a scan of the image sees.

### 3. Pre-release scan gate (`mvn-release.yml`)

New job `preflight` before `central` (`central: needs: preflight`):

1. Set release version, `./mvnw -Pdeploy -DskipTests -pl package -am package` (produces the SBOMs and the JVM image
   context).
2. `anchore/scan-action` (Grype) on the aggregate SBOM with `--vex vex/arcadedb.openvex.json`,
   `severity-cutoff: critical`, `only-fixed: true`. Full report (all severities) in the job summary and as a workflow
   artifact.
3. Build the JVM image for **each** release platform (`linux/amd64`, `linux/arm64`) single-platform with
   `docker buildx build --platform <p> --output type=oci,dest=image-<arch>.tar` (QEMU, as `docker-and-tag` already
   sets up), and scan each tarball the same way, so an arch-specific base-image Critical is caught too. (Builder-check
   point: a multi-platform build cannot be `--load`ed.)

Recovery: a preflight failure has published nothing. Fix (bump the dependency, or add a VEX `not_affected` statement with
justification) and dispatch again.

Nightly: a new `supply-chain-scan.yml` on `schedule` + `workflow_dispatch` runs steps 1-3 on `main` and opens/updates a
single tracking issue on failure (builder may instead reuse an existing nightly workflow).

### 4. `central` job: provenance for jars and archives

After `deploy` succeeds (i.e. PUBLISHED), a separate job `attest` (`needs: central`) runs. `central` builds and
deploys with Maven plugins and must not hold `id-token`; `attest` only downloads the artifacts `central` uploaded and
attests them, so a failed attestation is recovered with "Re-run failed jobs" without touching Central:

- `actions/attest-build-provenance` with `subject-path` covering `**/target/*.jar` that were published,
  `package/target/arcadedb-*.tar.gz`, `*.zip`, and the SBOM files. The attestation is keyed by digest, so it verifies the
  identical bytes wherever they are later downloaded from (Central or the GitHub release).
- `actions/attest-sbom` binding the distribution archives to their aggregate SBOM.
- `central` uploads the distribution archives + SBOMs as a workflow artifact `release-dist` (retention 90 days) for
  `attest` and for step 7.
- `mvn-release.yml` gets `run-name: release ${{ inputs.releaseversion }}` so step 7 finds the run by version.
- Rehearsal input: `mvn-release.yml` gains a boolean `rehearsal` input, default false. When true, `central`
  runs `package` instead of `deploy` (nothing to Central), `attest` still attests and `release-dist` is uploaded;
  `docker-and-tag` pushes to the namespace in the `DOCKER_NAMESPACE` variable and (since the final fix wave, see
  "Implementation decisions") tags and commits on the fork, so the rest of the chain runs there. A rehearsal runs on forks only: a preflight step refuses it in `ArcadeData/arcadedb`
  (its attestations would carry the real signer identity) and refuses it when `DOCKER_NAMESPACE` is unset or still
  `arcadedata`. The non-rehearsal path and its recovery notes are unchanged.

`attest` holds `id-token: write` and `attestations: write`; `central` does not.

### 5. `docker-and-tag` job: sign the JVM image

`docker-and-tag` (`needs: [central, attest]`) gets `id-token: write` and `attestations: write`; `packages: write` is not
needed (Docker Hub). It runs a Maven build while holding `id-token: write`, an accepted residual because it is
release-only and manually dispatched. After "Build and push Docker
images":

1. Resolve the pushed multi-arch digest: `docker buildx imagetools inspect arcadedata/arcadedb:${RELEASE_VERSION}`
   -> `sha256:...`. Assert `latest` resolves to the same digest.
2. `sigstore/cosign-installer`, then `cosign sign --yes --recursive arcadedata/arcadedb@<digest>` (index and per-arch
   manifests).
3. `anchore/sbom-action` (syft) on the image by digest -> CycloneDX + SPDX JSON; `cosign attest --yes --type cyclonedx`
   with the CycloneDX predicate.
4. `actions/attest-build-provenance` with `subject-name: index.docker.io/arcadedata/arcadedb`,
   `subject-digest: <digest>`, `push-to-registry: true`.
5. Upload the image SBOMs as workflow artifact `release-image-sbom`.

These steps sit before "Tag and commit release version", so a signing failure leaves no tag; a re-run pushes the same
digest again (fabric8 rebuild may differ, see Known gaps) and signs whatever it pushed.

### 6. `native-image.yml`: sign native images, attest native archives

The `build`, `docker` and `manifest` jobs also run on the nightly and run Maven plugins, native-image and the e2e
battery, so none of them holds `id-token`. Signing lives in separate jobs that run only when
`github.event_name == 'release' || inputs.attach_to_release`, and `check-release-signing.py` verifies the mapping from
each push job to its signing job:

- `attest-native-archives` (`needs: build`): `actions/attest-build-provenance` on each archive.
- `sign-native-images` (`needs: docker`, one per architecture): cosign sign the per-arch digest; syft SBOM +
  `cosign attest`; provenance with `push-to-registry: true`; upload the per-arch SBOMs to the release.
- `sign-native-manifest` (`needs: [docker, manifest]`): cosign sign the `<v>-native` manifest list digest
  (non-recursive, per-arch already signed). The per-arch images carry the SBOM attestation, the list only a signature.
- Dispatch-ref guard: on `attach_to_release`, a first step fails if `github.ref != 'refs/heads/main'` (see Decisions).
- When `attach_to_release` completes, it dispatches `release-assets.yml` for that tag (`gh workflow run`, needs
  `actions: write`), because `--clobber` uploads and re-pushed images make the existing `SHA256SUMS` stale.

### 6b. `publish-contract.yml`: provenance for the contract files

A release-only job `attest` (`needs: publish`) holds `id-token: write` and `attestations: write` and runs
`actions/attest-build-provenance` on the OpenAPI JSON and proto files; the `publish` job, which builds them, holds
neither.

Nightly (`schedule`, `publish_nightly`) behavior unchanged: no signing.

### 7. New workflow `release-assets.yml`: SBOMs, VEX, SHA256SUMS on the GitHub release

Trigger: `workflow_run` of `native-image.yml` and of `publish-contract.yml` with `types: [completed]`, filtered to runs
whose triggering event was `release`; plus `workflow_dispatch` with `tag` input (recovery). Runs only when *both*
upstream runs for that tag have concluded successfully (the job checks via `gh run list`; whichever finishes second does
the work, the first exits early). Concurrency group per tag.

Steps:

1. Download the `release-dist` and `release-image-sbom` artifacts from the successful, non-rehearsal `mvn-release.yml`
   run named `release <tag>`, which must be a run on `main`. If the artifacts have expired, the fallback is to download
   the archives from the release and accept them only after `gh attestation verify` passes. Upload `arcadedb-<v>.tar.gz` and `arcadedb-<v>-base.tar.gz` (+ `.sha256`) to the release.
   If an asset with that name already exists with a different digest, fail loudly (someone uploaded by hand); same
   digest is a no-op.
2. Upload SBOMs: `arcadedb-<v>.cdx.json`, `arcadedb-<v>.spdx.json` (SPDX via `syft convert`), image SBOMs
   `arcadedb-image-<v>.{cdx,spdx}.json`, `arcadedb-image-<v>-native.{cdx,spdx}.json`.
3. Upload `vex/arcadedb.openvex.json` as `arcadedb-<v>.openvex.json`.
4. Download every asset, generate `SHA256SUMS` (sorted, excluding itself and its signatures), sign with
   `cosign sign-blob --yes --bundle SHA256SUMS.sigstore.json` and with GPG (`--armor --detach-sign`, key from
   `MAVEN_GPG_PRIVATE_KEY`; the key's fingerprint must equal the repository variable `RELEASE_GPG_FINGERPRINT`, which a
   maintainer sets once before the first release, and the signature is verified against it), upload all three with `--clobber`.
5. Verify step: `gh attestation verify` on every archive with `--repo ArcadeData/arcadedb`, `cosign verify` on every
   image tag with `--certificate-identity-regexp` scoped to the four signing workflows and
   `--certificate-oidc-issuer https://token.actions.githubusercontent.com`. Fails the run if anything is unverifiable. This
   is the end-to-end test of the whole chain on every release.

Permissions: `contents: write`, `id-token: write`, `actions: read`.

### 8. VEX file

`vex/arcadedb.openvex.json`, OpenVEX 0.2.0, author `Arcade Data Ltd`. Starts empty (valid document, no statements).
Statements are added by PR with `justification` and `impact_statement`; `vexctl` validates it in a small CI check that
runs on PRs touching `vex/`.

### 9. Documentation

- Docs repo: new page "Verifying ArcadeDB releases" (American English), sections: Maven Central (GPG + CycloneDX
  classifier), release archives (`sha256sum -c`, `gpg --verify SHA256SUMS.asc`, `cosign verify-blob --bundle`,
  `gh attestation verify`), container images (`cosign verify`, `cosign verify-attestation --type cyclonedx`,
  `gh attestation verify oci://...`), SBOM and VEX locations. Goes through a docs PR, merged after the first release
  that ships these assets.
- `SECURITY.md`: support period statement (latest minor line, critical backports at maintainers' discretion, as today,
  stated explicitly with an end-of-support rule), link to the verification page, mention of SBOM/VEX per release.
- `mvn-release.yml` header comment: extend the job map and recovery notes with `preflight` and the signing steps.

## Verification strategy

Workflows cannot be unit-tested, so:

- `actionlint` (existing config) on every changed workflow.
- Local: `./mvnw -Pdeploy -DskipTests -pl package -am package` produces per-module `*-cyclonedx.json/.xml` and
  `package/target/sbom/*.cdx.json`; validate with `cyclonedx-cli validate` and check NTIA fields with a small script
  (`.github/scripts/check-sbom-fields.py`) that also runs in `preflight`.
- Studio: `npm test` (license allow-list test covers the new devDependency).
- Rehearsal: run the whole chain on a fork (never in `ArcadeData/arcadedb`) with `DOCKER_NAMESPACE` and Docker Hub
  credentials pointing to a scratch namespace, including `release-assets.yml` step 5 as the acceptance test. The first real release then runs
  the same code.
- Regression guard: extend an existing `.github/scripts/check-*.py` (or add one) asserting that every workflow step
  that runs `docker push` / fabric8 `deploy` for a release is followed by a cosign sign step in the same job, so a future
  image-publishing step cannot silently skip signing.

## Known gaps / follow-ups

- **Open finding on `main`**: the pre-release scan fails a clean tree. GHSA-c4c3-7fpv-j4q5 (Critical, fix available) is
  reported against `netty-handler` 4.1.136.Final embedded in the `arcadedb-ha-raft` shaded jar via `ratis-thirdparty`;
  the fixed version is 4.1.137.Final. Bumping it is separate work and out of scope here, but it blocks the next release
  until it is bumped or assessed `not_affected` in the VEX file.
- **Image jars vs Central jars**: `docker-and-tag` rebuilds from source, so the jars inside the JVM image are not
  byte-identical to Central's. Image SBOM and provenance are still correct for the image; byte identity needs
  reproducible builds (`project.build.outputTimestamp`) - follow-up issue.
- OpenSSF Scorecard workflow + badge - follow-up issue.
- Immutable releases (draft -> upload -> publish) - follow-up issue.
- Release runbook (wherever the release manager's checklist lives): "create the release with no files; do not upload
  archives by hand"; native recovery dispatch from `main`.
- CRA vulnerability-reporting process and CISA attestation form - company process, flagged to the user.

## Decisions left to the builder

- Exact plugin/action versions (latest stable, pinned by SHA).
- SBOM merge tool for Studio (must validate against CycloneDX 1.6).
- Whether the nightly scan is a new workflow or a job in an existing nightly.
- Job-summary formatting of scan results.

## Implementation decisions (2026-11-26)

Decisions taken while building this, each already reflected in the sections above.

- Signer identity: pinned to the four workflows `mvn-release.yml`, `native-image.yml`, `release-assets.yml` and
  `publish-contract.yml` on `main` or a release tag, because other workflows on `main` hold `id-token: write`.
- Studio SBOM: generated by `npm-lock-to-cdx.py` instead of `@cyclonedx/cyclonedx-npm`, whose transitive CC-BY-3.0
  dependency is not on the license allow-list.
- Distribution assemblies: only `full` and `-base` are built, so only those two get an SBOM.
- Embedded components: the distribution SBOM also lists components embedded in shaded and second-level uber-jars, so
  consumers scanning the SBOM see the same findings as an image scan.
- Supplier and timestamp: per-module Central SBOMs carry the supplier in `metadata.component.publisher` and may lack a
  timestamp; release-level SBOMs are strict and always carry both.
- Attestation: runs in a separate `attest` job after `central`, so the job that builds and deploys never holds
  `id-token`, and the image jobs need both.
- Native images and contract files: signing and attesting live in release-only jobs separate from the build and push
  jobs, which also run on the nightly and keep their original permissions.
- Rehearsal: forks only, always with `DOCKER_NAMESPACE` set to a scratch namespace; the workflow refuses to rehearse in
  `ArcadeData/arcadedb`.
- GPG: the `SHA256SUMS` signature is verified against the fingerprint in the repository variable
  `RELEASE_GPG_FINGERPRINT`, which a maintainer must set before the first release.
- Release assets: the job reads the archives only from a successful `mvn-release.yml` run on `main`; expired artifacts
  fall back to the release assets plus attestation verification.
- JVM image signing (supersedes the earlier accepted residual): `docker-and-tag` is split into `docker` (Maven build
  and push, no `id-token`), `sign-image` (no checkout; cosign signature, SBOM and provenance attestations, the
  `release-image-sbom` artifact) and `tag` (needs `sign-image`). `check-release-signing.py` verifies the push -> signing
  job mapping, accepts an ungated signing job only in the dispatch-only `mvn-release.yml`, and requires a tagging job to
  need every signing job.
- Release-assets run selection: `release-assets.sh` takes the JVM files from the latest `mvn-release.yml` run on `main`
  titled `release <tag>` whose `attest` and `sign-image` jobs succeeded (jobs stay listed after artifacts expire), whatever the run's
  overall result, so a failed next-version commit does not block the release; `mvn_release_run_id` names the run on a
  dispatch and is checked the same way. JVM jars whose `attest` never ran (central failed after PUBLISHED) ship without
  provenance; the fix is a new patch release, never hand-made attestations.
- Fork rehearsal covers the whole chain: every image reference in `native-image.yml`, `publish-contract.yml` and
  `verify-release.sh` follows `DOCKER_NAMESPACE`, a rehearsal tags and commits on the fork, and `release-assets.sh`
  accepts the `(rehearsal)` title outside `ArcadeData/arcadedb` only.
- Accepted cost: every push to `main` now runs the SBOM merge of the snapshot deploy under `-Pdeploy`.
- A signing job that fails on a release is recovered with "Re-run failed jobs"; a `release-assets.yml` refresh that runs after a failed signing job is fine, because its verify step fails loudly.
- The Critical netty finding in the scan is out of scope for this change and is tracked as a separate dependency bump.
- Support period: `SECURITY.md` states that the latest minor line is supported until 90 days after the next minor
  release (a proposal awaiting maintainer confirmation).
