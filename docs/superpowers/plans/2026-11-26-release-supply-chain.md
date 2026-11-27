# Release Supply Chain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every ArcadeDB release ships SBOMs (CycloneDX + SPDX), signed images, SLSA provenance, a signed `SHA256SUMS`,
a VEX file, and is gated by a vulnerability scan.

**Architecture:** Maven produces per-module and per-assembly CycloneDX SBOMs under the `deploy` profile. The release
workflows sign/attest each artifact in the job that builds it (cosign keyless + `actions/attest-build-provenance`). A new
`release-assets.yml` assembles the GitHub release (archives, SBOMs, VEX, signed SHA256SUMS) and verifies the whole chain.
Python guard scripts, self-tested with fixtures by `test-ci-scripts.sh`, keep the guarantees from regressing.

**Tech Stack:** Maven (`cyclonedx-maven-plugin`), npm (`@cyclonedx/cyclonedx-npm`), GitHub Actions, cosign, syft,
grype, vexctl, Python 3 guard scripts.

**Spec:** `docs/superpowers/specs/2026-11-26-release-supply-chain-design.md`

## Global Constraints

- Work on branch `feat/release-supply-chain` in a worktree. **Never commit or push**: the user commits after review.
  Each task ends with "leave changes staged-for-review" instead of a commit.
- Every action pinned by full SHA with a `# vX.Y.Z` comment (enforced by `check-action-pin-comments.py`).
- Every Maven call in a workflow uses `./mvnw` (enforced by `check-maven-wrapper.py`).
- SBOM formats: CycloneDX schema **1.6** JSON (+ XML on Central); SPDX **2.3** JSON on the GitHub release.
- SBOM required fields (TR-03183-2 / NTIA): supplier `Arcade Data Ltd`, name, version, purl, hash (SHA-512 if
  available, else SHA-256), license, dependencies, author/tool, timestamp.
- Gate: fail only on severity **critical** with a **fix available**, not marked `not_affected` in
  `vex/arcadedb.openvex.json`.
- Signer identity regexp, used verbatim everywhere (the four release workflows only: other workflows on main also
  hold `id-token: write`, and `[^@]+` would accept their certificates):
  `^https://github\.com/ArcadeData/arcadedb/\.github/workflows/(mvn-release|native-image|release-assets|publish-contract)\.yml@refs/(heads/main|tags/[0-9][^/]*)$`,
  issuer `https://token.actions.githubusercontent.com`. CycloneDX `makeBom` needs network (`requiresOnline`), so the
  SBOM commands below run without `-o`.
- Signing/attesting runs only for releases (`mvn-release.yml`, `release` events, `attach_to_release`), never nightly.
- New dependencies: cyclonedx-maven-plugin (Apache-2.0), @cyclonedx/cyclonedx-npm (Apache-2.0) -> add both to
  `ATTRIBUTIONS.md`. CI tools (cosign, syft, grype, vexctl) are not shipped.
- New files carry the repo's Apache-2.0 / SPDX header (copy from `check-release-upload-assets.py`), no Claude author.
- Workflow comments follow the repo style: explain the failure the step prevents.

## Review Focus

1. **A release where a sign step fails after the push** - the image is public unsigned. Expect: job fails before the git
   tag; "Re-run failed jobs" signs the then-current digest. Task 8 asserts sign steps precede "Tag and commit".
2. **`latest` and `<v>` pointing at different digests** after `docker-and-tag` - signing one leaves the other unsigned.
   Expect: the step fails. Task 8 includes the digest-equality assertion.
3. **`release-assets.yml` fired twice** (native and contract both finish) - must not double-upload or race. Expect:
   per-tag concurrency group, first-finisher exits early. Task 11 test case.
4. **A hand-uploaded archive with different bytes** already on the release - expect a loud failure, never `--clobber`.
   Task 11 test case.
5. **A protocol module's SBOM listing `arcadedb-server` as a bundled dependency** (it is `provided`) - misleads every
   scanner. Task 2 asserts it is absent from e.g. the `arcadedb-bolt` SBOM.

---

### Task 1: SBOM field checker

**Files:**
- Create: `.github/scripts/check-sbom-fields.py`
- Create: `.github/scripts/tests/fixtures/sbom/good.cdx.json`, `missing-hash.cdx.json`, `missing-supplier.cdx.json`,
  `missing-license.cdx.json`
- Modify: `.github/scripts/tests/test-ci-scripts.sh` (add cases)

**Interfaces:**
- Produces: `check-sbom-fields.py <sbom.cdx.json> [...]` -> exit 0 if every file passes; exit 1 printing
  `<file>: <component purl>: missing <field>` per violation. Checks: `bomFormat == "CycloneDX"`,
  `specVersion >= "1.5"`, `metadata.timestamp`, `metadata.supplier.name` (or `metadata.manufacturer.name`)
  == `Arcade Data Ltd`, and for every component of `type` `library`/`application`: `name`, `version`, `purl`, at least
  one `hashes[]` with alg `SHA-256` or `SHA-512`, `licenses` non-empty; top-level `dependencies` non-empty.

- [ ] **Step 1:** Write the four fixtures (minimal, 2 components each; each `missing-*` breaks exactly one field).
- [ ] **Step 2:** Add cases to `test-ci-scripts.sh` in its existing must-pass / must-fail style: `good` passes; each
  `missing-*` fails and its output contains `missing hashes`, `missing supplier`, `missing licenses` respectively.
- [ ] **Step 3:** Run `.github/scripts/tests/test-ci-scripts.sh` -> new cases FAIL (script absent).
- [ ] **Step 4:** Implement the script (stdlib only).
- [ ] **Step 5:** Run `.github/scripts/tests/test-ci-scripts.sh` -> all PASS.

### Task 2: Per-module CycloneDX SBOMs on Maven Central

**Files:**
- Modify: `pom.xml` (property `cyclonedx-maven-plugin.version` = latest 2.9.x; plugin in the `deploy` profile)
- Modify: `pom.xml` of `e2e`, `coverage` and any benchmark-only module: `<cyclonedx.skip>true</cyclonedx.skip>`
- Modify: `ATTRIBUTIONS.md`

**Interfaces:**
- Produces: per module `target/<artifactId>-<version>-cyclonedx.json` and `.xml`, attached with classifier
  `cyclonedx`. Config: `makeBom` at `package`, `schemaVersion 1.6`, `outputFormat all`, `includeTestScope false`,
  `includeProvidedScope false`, `includeSystemScope false`, `includeLicenseText false`, `organizationalEntity` name
  `Arcade Data Ltd` / url `https://arcadedb.com`, `outputTimestamp` from `project.build.outputTimestamp` if set.

- [ ] **Step 1:** Run `./mvnw -Pdeploy -DskipTests -pl engine,bolt -am package` and confirm no
  `*-cyclonedx.json` exists (baseline).
- [ ] **Step 2:** Add the plugin config.
- [ ] **Step 3:** Rerun step 1. Expected: `engine/target/arcadedb-engine-*-cyclonedx.json` exists;
  `.github/scripts/check-sbom-fields.py engine/target/*-cyclonedx.json bolt/target/*-cyclonedx.json` exits 0.
- [ ] **Step 4:** Review Focus #5: `jq -e '[.components[].name] | index("arcadedb-server") | not' bolt/target/*-cyclonedx.json`
  exits 0.
- [ ] **Step 5:** `./mvnw -Pdeploy -DskipTests package -pl e2e -am` produces no SBOM for `e2e`.
- [ ] **Step 6:** `./mvnw -Pdeploy -DskipTests -pl engine install` then confirm
  `~/.m2/.../arcadedb-engine-<v>-cyclonedx.json` is installed (proves it is attached, hence GPG-signed and bundled by
  central-publishing on a real deploy). Run `python3 .github/scripts/check-license-allowlist.py` if it runs locally.

### Task 3: Studio SBOM and per-assembly distribution SBOMs

**Files:**
- Modify: `studio/package.json` (+ lock): devDependency `@cyclonedx/cyclonedx-npm`; script
  `"sbom": "cyclonedx-npm --omit dev --spec-version 1.6 --output-format JSON --output-file target/studio.cdx.json"`
- Modify: `studio/pom.xml`: frontend-maven-plugin execution `npm sbom` at `package`, only in a `deploy` profile
- Create: `.github/scripts/merge-sbom.py`
- Modify: `package/pom.xml`: generate one SBOM per assembly, then add `sbom/` to each assembly
- Modify: `package/src/main/assembly/{full,minimal,headless,base}.xml`: fileSet `target/sbom/<assembly>/` -> `sbom/`
- Modify: `ATTRIBUTIONS.md`
- Test: `.github/scripts/tests/test-ci-scripts.sh` + fixtures `fixtures/sbom/merge-{jvm,studio}.cdx.json`

**Interfaces:**
- Consumes: Task 1 checker; Task 2 plugin.
- Produces: `merge-sbom.py --root <jvm.cdx.json> --nested <studio.cdx.json> --name arcadedb-studio --out <file>`:
  appends Studio's components as children of a new `application` component `arcadedb-studio` (version from the root
  BOM), merges `dependencies`, de-duplicates by `bom-ref`, keeps root `metadata`.
- Produces: `package/target/sbom/<assembly>/arcadedb-<v>[-<assembly>].cdx.json` for `full` (no suffix), `minimal`,
  `headless`, `base`; each listing exactly the jars its assembly bundles (scope via the assembly's dependency sets;
  builder decides between per-assembly `makeBom` executions with include/exclude filters or filtering the package
  module's BOM by the assembly's lib/ listing), Studio nested only where the assembly includes Studio.

- [ ] **Step 1:** Merge-script test: merged fixture contains component `arcadedb-studio` with Studio's children,
  `check-sbom-fields.py` passes on the output, no duplicate `bom-ref`. Run `test-ci-scripts.sh` -> FAIL.
- [ ] **Step 2:** Implement `merge-sbom.py`; rerun -> PASS.
- [ ] **Step 3:** Add the npm devDependency; `cd studio && npm test` -> PASS (license allow-list test includes it).
- [ ] **Step 4:** Wire Studio + package poms and assembly descriptors.
- [ ] **Step 5:** `./mvnw -Pdeploy -DskipTests -pl package -am package`; then
  `tar tzf package/target/arcadedb-<v>.tar.gz | grep sbom/` lists the SBOM; `check-sbom-fields.py` passes on all four;
  the `base` SBOM has no `arcadedb-studio` component if `base.xml` excludes Studio; for each assembly, the set of jar
  names under `lib/` equals the set of SBOM components of type `library` with a matching file name (write this
  comparison as a one-off shell check and paste the result in the review notes).

### Task 4: VEX file and its validation

**Files:**
- Create: `vex/arcadedb.openvex.json` (OpenVEX `https://openvex.dev/ns/v0.2.0`, `@id`
  `https://github.com/ArcadeData/arcadedb/vex/arcadedb.openvex.json`, author `Arcade Data Ltd`, `version` 1, empty
  `statements`)
- Create: `vex/README.md` (how to add a statement: `vexctl add`, status `not_affected` requires `justification` and
  `impact_statement`, one PR per CVE)
- Create: `.github/workflows/vex-validate.yml` (on `pull_request` with `paths: vex/**`; installs pinned vexctl; runs
  `vexctl merge vex/arcadedb.openvex.json` to parse, plus a `jq` check that every `not_affected` statement has
  `justification` and `impact_statement`)

- [ ] **Step 1:** Create a temporary invalid copy (a `not_affected` without justification) and run the jq check locally
  -> non-zero.
- [ ] **Step 2:** Run it on the real file -> zero. Delete the temp copy.
- [ ] **Step 3:** `actionlint .github/workflows/vex-validate.yml` -> clean.

### Task 5: Release signing guard

**Files:**
- Create: `.github/scripts/check-release-signing.py`
- Create: fixtures `fixtures/signing/{signed,unsigned-push,sign-after-tag,unguarded-attach}.yml`
- Modify: `.github/scripts/tests/test-ci-scripts.sh`

**Interfaces:**
- Produces: `check-release-signing.py [workflow ...]` (default `mvn-release.yml`, `native-image.yml`). Rules, per job:
  (a) any step whose `run` contains `docker push`, `imagetools create`, or `./mvnw` with `-Pdocker` and `deploy` must be
  followed later in the same job by a step whose `run` contains `cosign sign`, unless the push step's `if:` excludes
  releases (nightly-only pushes are listed in an explicit allow-list inside the script, by job+step name);
  (b) no `git tag` / `git push origin "${RELEASE_VERSION}"` step may precede a `cosign sign` step in the same job;
  (c) in a workflow with an `attach_to_release` input, a job that runs `cosign` or `attest-build-provenance` must contain
  a step that fails when `github.ref != 'refs/heads/main'`. Output: `<workflow>:<job>:<step>: <rule>`.

- [ ] **Step 1:** Fixtures + must-pass/must-fail cases (one per rule). Run `test-ci-scripts.sh` -> FAIL.
- [ ] **Step 2:** Implement (PyYAML is not guaranteed: follow how existing check scripts parse workflows).
- [ ] **Step 3:** Rerun -> PASS. Running it on the live workflows now FAILS (expected until Tasks 8-9); it is wired
  into `mvn-test.yml` in Task 12.

### Task 6: Pre-release scan gate and nightly scan

**Files:**
- Create: `.github/scripts/release-scan.sh`
- Modify: `.github/workflows/mvn-release.yml` (new job `preflight`; `central: needs: preflight`)
- Create: `.github/workflows/supply-chain-scan.yml`

**Interfaces:**
- Produces: `release-scan.sh <version>`: builds `-Pdeploy -DskipTests -pl package -am package`, runs
  `check-sbom-fields.py` on every SBOM under `package/target/sbom/`, builds the JVM image per platform
  (`linux/amd64`, `linux/arm64`) with `docker buildx build --output type=oci,dest=target/image-<arch>.tar`, runs grype
  on each SBOM and each image tarball with `--vex vex/arcadedb.openvex.json --only-fixed --fail-on critical`, and writes
  the full (all-severity) table to `$GITHUB_STEP_SUMMARY` when set. Exit non-zero on any gate failure, after all scans
  ran.
- `preflight` job: checkout, JDK 21, QEMU + buildx, `set-release-version.sh`, pinned grype install,
  `release-scan.sh "$RELEASE_VERSION"`, upload grype JSON reports as artifact `preflight-scan`. Permissions
  `contents: read`.
- `supply-chain-scan.yml`: `schedule` (daily, off-peak minute) + `workflow_dispatch`; runs the script on `main` with the
  current snapshot version; on failure creates or comments on one open issue titled `Nightly supply-chain scan failed`
  (`issues: write`), found by exact title.

- [ ] **Step 1:** Locally, pick a known-vulnerable-and-fixed artifact, add it temporarily as a dependency of `package`,
  run `release-scan.sh` -> exits non-zero naming that CVE. Remove the dependency.
- [ ] **Step 2:** Add a temporary VEX `not_affected` statement for that CVE, re-add the dependency -> exits zero.
  Revert both.
- [ ] **Step 3:** Clean tree -> exits zero (if not, the finding is real: report it to the user, do not suppress).
- [ ] **Step 4:** Update the header comment of `mvn-release.yml` (job map + "preflight failed: nothing published,
  fix and dispatch again"). `actionlint` both workflows -> clean.

### Task 7: `central` job: provenance, run-name, rehearsal

**Files:**
- Modify: `.github/workflows/mvn-release.yml`

**Interfaces:**
- Produces: `run-name: release ${{ inputs.releaseversion }}${{ inputs.rehearsal && ' (rehearsal)' || '' }}`.
- Produces: input `rehearsal` (boolean, default false). In `central`, rehearsal runs `package` instead of `deploy`.
- Produces: artifact `release-dist` containing `package/target/arcadedb-<v>.tar.gz`, `arcadedb-<v>-base.tar.gz`, their
  `.sha256`, and `package/target/sbom/**`; retention 90 days.
- Produces: provenance (`actions/attest-build-provenance`) over the published jars (`*/target/*.jar`, excluding
  `*-tests.jar` and modules with `maven.deploy.skip`), all four distribution archives, and the SBOM files;
  `actions/attest-sbom` binding `arcadedb-<v>.tar.gz` to its CycloneDX SBOM and `-base` to its own.

- [ ] **Step 1:** Implement. `actionlint` -> clean; `check-maven-wrapper.py`, `check-deploy-credential-scope.py`,
  `check-action-pin-comments.py` -> clean.
- [ ] **Step 2:** Extend the header recovery notes: attestation failure after PUBLISHED -> re-run failed jobs (attesting
  is idempotent), never dispatch.

### Task 8: `docker-and-tag` job: sign and attest the JVM image

**Files:**
- Modify: `.github/workflows/mvn-release.yml`

**Interfaces:**
- Consumes: Task 7 `rehearsal` input; repo variable `DOCKER_NAMESPACE` (default `arcadedata`) used as image prefix,
  passed to fabric8 as `-Ddocker.image.prefix` (add the property to `package/pom.xml`'s image `<name>` with default
  `arcadedata`).
- Produces: steps, in order, between "Build and push Docker images" and "Tag and commit release version":
  1. `Resolve image digest`: digest of `${NS}/arcadedb:${RELEASE_VERSION}` via `docker buildx imagetools inspect`;
     fail if `${NS}/arcadedb:latest` differs (Review Focus #2). Output `digest`.
  2. `cosign sign --yes --recursive ${NS}/arcadedb@<digest>`.
  3. syft (`anchore/sbom-action`) on the image by digest -> `arcadedb-image-<v>.cdx.json` and `.spdx.json`;
     `cosign attest --yes --type cyclonedx --predicate ...cdx.json`.
  4. `actions/attest-build-provenance` with `subject-name: index.docker.io/${NS}/arcadedb`, `subject-digest`,
     `push-to-registry: true`.
  5. Upload artifact `release-image-sbom`.
- Job permissions add `id-token: write`, `attestations: write`. Rehearsal skips the tag/commit/next-version steps.

- [ ] **Step 1:** Implement. Run `check-release-signing.py .github/workflows/mvn-release.yml` -> PASS (rules a, b).
- [ ] **Step 2:** `actionlint` -> clean. Header recovery notes: a re-run after a sign failure pushes and signs again
  before any tag exists.

### Task 9: `native-image.yml`: sign, attest, ref guard, re-trigger

**Files:**
- Modify: `.github/workflows/native-image.yml`

**Interfaces:**
- Consumes: `release-assets.yml` (Task 11) accepting `workflow_dispatch` input `tag`.
- Produces, all gated on `github.event_name == 'release' || inputs.attach_to_release`:
  - First step of every job that signs/attests: fail with
    `::error::attach_to_release must be dispatched from main (signer identity); ref still selects what to build`
    when `inputs.attach_to_release && github.ref != 'refs/heads/main'`.
  - Build job: `attest-build-provenance` on `native/target/${{ steps.package.outputs.archive }}`.
  - `docker` job: after push, resolve per-arch digest, `cosign sign`, syft SBOM + `cosign attest`, provenance with
    `push-to-registry: true`; upload the per-arch SBOM to the release as `arcadedb-image-<v>-native-<arch>.cdx.json`.
  - `manifest` job: `cosign sign` the `<v>-native` manifest-list digest (non-recursive).
  - New final job `refresh-release-assets` (`needs: [manifest, <build job>]`, only when `inputs.attach_to_release`):
    `gh workflow run release-assets.yml -f tag=<ref>` (`actions: write`).
- Update the workflow's header comment that recommends a feature branch for recovery.

- [ ] **Step 1:** Implement. `check-release-signing.py` on all defaults -> PASS (rules a, b, c);
  `check-release-upload-assets.py`, `check-native-*.py`, `actionlint` -> clean.

### Task 10: `publish-contract.yml` provenance

**Files:**
- Modify: `.github/workflows/publish-contract.yml`

- [ ] **Step 1:** Add `id-token: write`, `attestations: write`; `attest-build-provenance` over
  `arcadedb-openapi-$TAG.json` and `arcadedb-server-$TAG.proto` before the upload step. `actionlint` -> clean.

### Task 11: `release-assets.yml`

**Files:**
- Create: `.github/workflows/release-assets.yml`
- Create: `.github/scripts/release-assets.sh` (logic outside YAML so it can be tested)
- Create: `.github/scripts/verify-release.sh`
- Test: `.github/scripts/tests/test-ci-scripts.sh` cases using a stub `gh` on `PATH` (fixtures dir `fixtures/release/`)

**Interfaces:**
- Consumes: run-name from Task 7; artifacts `release-dist` (Task 7), `release-image-sbom` (Task 8); VEX (Task 4);
  image SBOM release assets from Task 9.
- Workflow: triggers `workflow_run` (workflows `Native Image`/actual name of native-image.yml and publish-contract's
  name, `types: [completed]`) + `workflow_dispatch` (`tag`). `concurrency: release-assets-<tag>`, not cancelled.
  For `workflow_run`: proceed only if `github.event.workflow_run.event == 'release'` and BOTH upstream runs for that tag
  concluded `success` (otherwise exit 0 with a notice). Permissions `contents: write`, `id-token: write`,
  `actions: read`, `attestations: read`. GPG key from `MAVEN_GPG_PRIVATE_KEY` / `MAVEN_GPG_PASSPHRASE`.
- `release-assets.sh <tag> <workdir>`: (1) find the latest successful non-rehearsal run named `release <tag>`, download
  both artifacts; (2) for each JVM archive + `.sha256`: absent on release -> upload; present with same SHA-256 -> skip;
  present with different SHA-256 -> exit 1 with `::error::<asset> on release <tag> differs from the CI-built file`;
  (3) `syft convert` each CycloneDX SBOM to SPDX JSON, upload `arcadedb-<v>[-base].{cdx,spdx}.json`,
  `arcadedb-image-<v>.{cdx,spdx}.json`; (4) upload VEX as `arcadedb-<v>.openvex.json`; (5) download all assets,
  write `SHA256SUMS` (`sha256sum`, sorted by name, excluding `SHA256SUMS*`), `cosign sign-blob --yes --bundle
  SHA256SUMS.sigstore.json`, `gpg --armor --detach-sign`, upload the three with `--clobber`.
- `verify-release.sh <tag>` (repo in the identity regexp and `--repo` taken from `GITHUB_REPOSITORY`, defaulting to
  `ArcadeData/arcadedb`, so the fork rehearsal verifies too): `sha256sum -c SHA256SUMS`; `cosign verify-blob --bundle` with the Global identity regexp;
  `gpg --verify SHA256SUMS.asc`; `gh attestation verify --repo ArcadeData/arcadedb` on every archive and contract file;
  `cosign verify` + `cosign verify-attestation --type cyclonedx` on `arcadedb:<v>`, `:<v>-native` with the identity
  regexp; exit non-zero listing every failure.

- [ ] **Step 1:** Stub-`gh` test cases: (a) asset absent -> uploaded; (b) same digest -> not re-uploaded;
  (c) different digest -> exit 1 with the error text (Review Focus #4); (d) `workflow_run` with only one upstream
  finished -> exits 0 without uploading (Review Focus #3; implement the "both finished" check as a function in the
  script taking the tag). Run -> FAIL.
- [ ] **Step 2:** Implement the scripts; rerun -> PASS.
- [ ] **Step 3:** Workflow YAML calling them (final step `verify-release.sh`). `actionlint`,
  `check-release-upload-assets.py`, `check-action-pin-comments.py` -> clean.

### Task 12: Wire guards into CI, SECURITY.md

**Files:**
- Modify: `.github/workflows/mvn-test.yml` (steps `Check release signing` -> `check-release-signing.py`, next to
  `Check release upload assets`, with a comment in the existing style)
- Modify: `SECURITY.md`: section "Supported versions" gains an explicit support-period rule (latest minor line receives
  security fixes until the next minor release plus 90 days; older lines only for critical issues when a backport is
  practical - **confirm the 90 days with the user before writing it**); new section "Verifying releases" linking the docs
  page and listing what each release carries (SBOMs, VEX, SHA256SUMS + signatures, provenance, signed images).

- [ ] **Step 1:** Ask the user to confirm the support period wording.
- [ ] **Step 2:** Implement. Run every `.github/scripts/check-*.py` and `tests/test-ci-scripts.sh` locally -> PASS.
- [ ] **Step 3:** `actionlint .github/workflows/*.yml` -> no new findings.

### Task 13: Docs page (docs repository)

**Files:**
- Docs repo: new page "Verifying ArcadeDB releases" (American English, uppercase SQL n/a), in the security / operations
  section, linked from the download page.

- [ ] **Step 1:** Write sections: Maven Central (`gpg --verify`, CycloneDX classifier coordinates), release archives
  (`sha256sum -c`, `gpg --verify SHA256SUMS.asc SHA256SUMS`, `cosign verify-blob` with the Global identity regexp,
  `gh attestation verify <file> --repo ArcadeData/arcadedb`), container images (`cosign verify`,
  `cosign verify-attestation --type cyclonedx`, `gh attestation verify oci://index.docker.io/arcadedata/arcadedb:<v>
  --repo ArcadeData/arcadedb`), SBOM/VEX locations and formats.
- [ ] **Step 2:** Every command copy-pasted from `verify-release.sh` (single source). Open as a branch + PR in the docs
  repo only after the user approves (memory: post-merge docs go to a PR); merge after the first release that ships the
  assets.

### Task 14: Rehearsal on a fork (with the user)

> **WARNING: never put maintainer Docker Hub credentials (any account with push rights on `arcadedata`) on the fork.
> The fork's `DOCKER_USERNAME` / `DOCKER_PASSWORD` must be a scratch account that can push ONLY to the scratch
> namespace.** The workflows refuse a rehearsal into `arcadedata`, but credentials that could push there turn any
> mistake in that guard into an overwritten public image.

- [ ] **Step 1:** Ask the user for a fork with: the repository variable `DOCKER_NAMESPACE` set to a scratch Docker Hub
  namespace; secrets `DOCKER_USERNAME` / `DOCKER_PASSWORD` for a scratch account on that namespace (see the warning);
  a GPG test key as `MAVEN_GPG_PRIVATE_KEY` (+ `MAVEN_GPG_PASSPHRASE`) and its fingerprint as the repository variable
  `RELEASE_GPG_FINGERPRINT`. Actions enabled on the fork.
- [ ] **Step 2:** On the fork, from its `main`: dispatch `mvn-release.yml` with `rehearsal: true`, version
  `0.0.1-rehearsal`, next version e.g. `0.0.2-SNAPSHOT`. Confirm `preflight`, `central` (package only), `attest`,
  `docker` and `sign-image` (images in `DOCKER_NAMESPACE`), and `tag` (tag `0.0.1-rehearsal` and the two version
  commits on the fork's `main`) all pass.
- [ ] **Step 3:** Create the GitHub release `0.0.1-rehearsal` from the fork's tag. Let `native-image.yml` and
  `publish-contract.yml` run (both use `DOCKER_NAMESPACE`); confirm `release-assets.yml` runs once, finds the run titled
  `release 0.0.1-rehearsal (rehearsal)` (accepted only outside `ArcadeData/arcadedb`), uploads, signs, and
  `verify-release.sh` passes with the fork's owner (`GITHUB_REPOSITORY`) and namespace (`DOCKER_NAMESPACE`).
- [ ] **Step 4:** Clean up: delete the fork's tag, release and version commits, and the scratch images.
- [ ] **Step 5:** Report results to the user, then file (with user approval) follow-up issues: reproducible builds,
  OpenSSF Scorecard, immutable releases, release runbook update.
