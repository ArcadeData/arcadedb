# Release Workflow Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `mvn-release.yml` pushes Docker images and the git tag only after Sonatype reports the Central deployment PUBLISHED.

**Architecture:** Split the single `publish` job into `central` (Maven deploy, waits for PUBLISHED, uploads the Docker
build context as a tarball) and `docker-and-tag` (`needs: central`: `docker buildx build --push` from that context,
then tag + next-version commit). Version edits move into one script both jobs call.

**Tech Stack:** GitHub Actions, bash, `central-publishing-maven-plugin` 0.12.0, Docker Buildx.

**Spec:** `docs/superpowers/specs/2026-11-25-release-workflow-split-design.md`

> **Revised after review (2026-11-25):** the maintainer asked for the images to stay built by Maven. Job `central`
> no longer uses `-Pdocker` and no tar/artifact hand-off exists. Job `docker-and-tag` runs
> `./mvnw -Pdocker -DskipPublishing=true -DskipTests -pl package -am deploy`: `-am` builds the release from the tree,
> so nothing is resolved from Central, and `-DskipPublishing=true` stops the default-build
> central-publishing-maven-plugin from uploading a second time. Where Tasks 2 and the constraints below say
> `docker buildx` or "tar", read this instead.

## Global Constraints

- Gate: `-DwaitUntil=published -DwaitMaxTime=3600` on the release `deploy` command line only. Not in `pom.xml` (the
  snapshot deploy in `mvn-deploy.yml` must not wait). Property names verified in plugin 0.12.0's `plugin.xml`
  (`${waitUntil}`, `${waitMaxTime}`, default 1800 s).
- Job `central` keeps `-Pdocker` (its validate-phase copy puts the Dockerfile into the context dir and its
  `docker-maven-plugin.platform` is `linux/amd64,linux/arm64`) but adds `-Ddocker.skip=true` so neither `build` nor
  `push` runs.
- Job `docker-and-tag` does NOT run `docker:build` via Maven: fabric8 0.49.0 `build` has
  `requiresDependencyResolution=test`, which resolves the release's `arcadedb-*` jars from Central and so reintroduces
  propagation lag. It runs `docker buildx build` directly (no new action).
- Image: only `arcadedata/arcadedb` (minimal and headless are commented out in `package/pom.xml`). Tags `latest` and
  `<releaseversion>`. Platforms `linux/amd64,linux/arm64`.
- Order in job 2: Docker push, then commit + tag + push tag, then next-version commit + push `main`.
- Pin every action by SHA, reusing the pins already in the file / repo
  (`actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2`,
  `actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2`).
- Do not commit (the maintainer commits after review).

## Review Focus

1. **File modes across jobs:** `upload-artifact` drops the executable bit, so `bin/*.sh` in the image would not run.
   Ship the context as a `tar` (preserves modes); Task 2 checks `bin/server.sh` is executable after a round trip.
2. **Version drift between jobs:** job 2 commits a tree it rebuilt itself; if its edits differ from job 1's, the tag
   points at sources that were not what went to Central. Task 1's script is the only place edits happen, and Task 1
   checks the exact set of changed files.
3. **Re-run of job 2 after a partial run** (tag pushed, `main` push failed): `git tag` fails on an existing tag.
   Accept: manual recovery, but job 2 must fail loudly, not silently skip. No test; note in PR body.
4. **Slow Portal:** a publish taking longer than `waitMaxTime` fails job 1 while Central may still publish. 3600 s
   gives headroom; documented in the workflow comment so a re-run is not attempted blindly (it would be rejected as
   duplicate version).
5. **Context dir name:** `package/target/arcadedb-<version>.dir` with the version from `versions:set`; a mismatch makes
   the tar step fail. Task 2's local run exercises the real path.

---

### Task 1: Shared version script

**Files:**
- Create: `.github/scripts/set-release-version.sh`

**Interfaces:**
- Produces: `.github/scripts/set-release-version.sh <version>`, run from repo root. Applies exactly today's three
  release edits: `./mvnw versions:set -DnewVersion=<version> --no-transfer-progress` (plus
  `-DgenerateBackupPoms=false`), `studio/package.json` `.version`, `server.json` `.version` and each
  `.packages[].identifier` tag (same `jq` expression as current workflow lines 63-66). `set -euo pipefail`; exits
  non-zero with usage on missing arg. Executable bit set (`git update-index --chmod=+x` is the maintainer's at commit;
  `chmod +x` now).

- [ ] **Step 1: Write the check** - in a scratch worktree (`git worktree add <scratchpad>/relver HEAD`), run the script
  with `99.9.9` and expect: `git diff --name-only` lists only `pom.xml` files, `studio/package.json`, `server.json`;
  `jq -r .version studio/package.json server.json` prints `99.9.9` twice;
  `jq -r '.packages[].identifier' server.json` all end in `:99.9.9`; no `*.versionsBackup` files.
- [ ] **Step 2: Run it before the script exists** - expect "No such file".
- [ ] **Step 3: Write the script.**
- [ ] **Step 4: Run the check** - all expectations above hold; running with no argument exits 1 with usage.
- [ ] **Step 5:** `git worktree remove --force <scratchpad>/relver`.

### Task 2: Split the workflow

**Files:**
- Modify: `.github/workflows/mvn-release.yml` (whole `jobs:` block)

**Interfaces:**
- Consumes: `.github/scripts/set-release-version.sh <version>` (Task 1).
- Produces: artifact `docker-context` containing `docker-context.tar` (the `arcadedb-<version>.dir` directory,
  created with `tar -C package/target -cf docker-context.tar arcadedb-<version>.dir`).

Job `central` (permissions: `contents: read`, `id-token: write`, `attestations: write`):
checkout, setup-java (unchanged block), `set-release-version.sh ${{ inputs.releaseversion }}`, deploy:
`./mvnw -Pdeploy,deploy-central -Pdocker -Ddocker.skip=true -DwaitUntil=published -DwaitMaxTime=3600 ...` (rest of
today's flags and env, minus `DOCKER_*`), keep the existing #10023 / wrapper comment and add one on the gate, tar,
upload `docker-context` (`retention-days: 7`, `if-no-files-found: error`).

Job `docker-and-tag` (`needs: central`, permissions: `contents: write`, `packages: write`):
checkout, setup-java (for `versions:set`), Docker login, QEMU, Buildx (existing pins), download `docker-context`,
`tar -xf docker-context.tar`, then
`docker buildx build --platform linux/amd64,linux/arm64 -t arcadedata/arcadedb:latest -t arcadedata/arcadedb:<v> --push arcadedb-<v>.dir`,
then `set-release-version.sh` and the existing tag / next-version / commit / push steps unchanged (studio next-version
step stays inline: it also touches `package-lock.json`, unlike the release edit). Drop the `if: success()` guards (now
implied by job dependency and step order).

- [ ] **Step 1: Lint before** - `actionlint .github/workflows/mvn-release.yml` on the current file: record baseline
  (expect clean).
- [ ] **Step 2: Edit the workflow** as above.
- [ ] **Step 3: Lint after** - `actionlint .github/workflows/mvn-release.yml`: no findings.
- [ ] **Step 4: Gate wiring check** - `grep -c 'waitUntil' pom.xml` is 0 and
  `grep -n 'DOCKER_\|docker' .github/workflows/mvn-release.yml` shows Docker only inside `docker-and-tag` (plus
  `-Pdocker -Ddocker.skip=true` in `central`).
- [ ] **Step 5: Local job-1 simulation** - `./mvnw -Pdocker -Ddocker.skip=true -pl package -am -DskipTests package`;
  expect `package/target/arcadedb-<snapshot>.dir/Dockerfile` exists, and no image `arcadedata/arcadedb:<snapshot>`
  appears in `docker images` built by this run.
- [ ] **Step 6: Local job-2 simulation** - tar the dir, extract into a scratchpad dir,
  `test -x <dir>/bin/server.sh`, then
  `docker buildx build --platform linux/amd64 -t relsplit-test:local --load <dir>` (no push) and
  `docker run --rm relsplit-test:local ls -l bin/server.sh` shows `x` bits. Remove the image afterwards.

### Task 3: Docs comment

**Files:**
- Modify: `.github/workflows/mvn-release.yml` (header comment)

- [ ] **Step 1:** Add a header comment: why the split (#link to issue if one is filed), recovery recipe: job
  `central` failed -> nothing published, fix and re-dispatch; job `docker-and-tag` failed -> "Re-run failed jobs"
  (never re-dispatch: Central rejects the duplicate version); tag already pushed -> finish remaining steps by hand.
- [ ] **Step 2:** `actionlint` clean again.
