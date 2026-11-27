# Release workflow: Maven Central first, then Docker and tag

## Intent

`mvn-release.yml` pushes the Docker images (`arcadedb`, `arcadedb-minimal`, `arcadedb-headless`, tags `latest` and
`<version>`) from the `deploy` phase of `package/pom.xml`, before `central-publishing-maven-plugin` uploads the bundle at
the end of the reactor. When the Central upload fails, Docker Hub already carries a release whose jars do not exist.
Goal: nothing leaves the building (Docker image, git tag, next-version commit) until Sonatype reports the deployment
PUBLISHED.

## Goals / non-goals

- Goal: Docker push, tag and next-version commit happen only after Central reports PUBLISHED.
- Goal: if a later step fails, it can be retried without touching Central again ("Re-run failed jobs").
- Non-goal: waiting for `repo1.maven.org` propagation. PUBLISHED from Sonatype is the gate (decided).
- Non-goal: changing `native-image.yml` / `publish-contract.yml`; they fire on the GitHub `release` event, which is
  created after the tag already.
- Anti-goal: a Docker image built from bytes different from what went to Central.

## Approaches

| | Approach | Verdict |
|---|---|---|
| A | One workflow, two jobs: `central` then `docker-and-tag` (`needs: central`) | **Picked**: one-click partial retry, Central never re-hit |
| B | One job, reorder steps | Rejected: a re-run repeats the Central deploy, which is rejected (version exists) |
| C | Two workflows chained by dispatch or `workflow_run` | Rejected: same safety as A, more plumbing to pass versions |

## Design

**Job `central`**
- Same setup as today minus Docker login / QEMU / Buildx.
- Version edits (Maven `versions:set`, `studio/package.json`, `server.json`) as today.
- `./mvnw -Pdeploy,deploy-central ... deploy` **without `-Pdocker`**.
- Central gate: `waitUntil=published` on the plugin, passed as a property on the release command line
  (`-Dcentral.waitUntil=published` or equivalent) so the snapshot deploy in `mvn-deploy.yml` is unaffected. A long
  enough `waitMaxTime` so a slow Portal does not fail a deployment that will publish.
- Uploads as a workflow artifact the three Docker build contexts
  `package/target/arcadedb-<version>{,-minimal,-headless}.dir` (built by `package`, with the Dockerfile copied in).
  For that, `-Pdocker` resource copy (validate phase) must still run while the `push` goal does not: split it so the
  copy runs in job 1 and build+push run in job 2 (builder's call how: separate profile, or `-Ddocker.skip.push`/
  `-Ddocker.skip` in job 1).

**Job `docker-and-tag`** (`needs: central`)
- Checkout, JDK, Docker login, QEMU, Buildx.
- Re-apply the same version edits (my call: extract them into one script, e.g. `.github/scripts/set-release-version.sh`,
  used by both jobs, so the two cannot drift).
- Download the build contexts into `package/target`, then `./mvnw -pl package -Pdocker docker:build docker:push`.
- Then the existing steps unchanged: commit + tag `<version>`, push tag, set next development version, commit, push
  `main`.

## Left to the builder

- Exact property names for `waitUntil` / `waitMaxTime` in the pinned plugin version.
- Whether `docker:build` invoked directly with `-pl package` needs dependency resolution from Central (it must not:
  propagation lag is out of scope). If it does, fall back to rebuilding `package` with `-am -DskipTests` in job 2.
- Ordering of Docker push vs tag inside job 2 (my call: Docker first, tag last, so the tag means "everything shipped").

## Verification

- `actionlint` on the workflow.
- Dry run on a fork or with a throwaway `releaseversion` against a non-production Docker repo is not available; at
  minimum run locally: `./mvnw -Pdocker -pl package -am -DskipTests package` with push skipped, confirm the three
  `.dir` contexts are complete, then `docker:build` from them with `-pl package` only.
