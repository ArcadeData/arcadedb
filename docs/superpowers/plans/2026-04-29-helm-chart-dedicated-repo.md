# arcadedb-helm Dedicated Repository - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the ArcadeDB Helm chart from `k8s/helm/` in the main monorepo into a new `ArcadeData/arcadedb-helm` GitHub repository with GitHub Pages serving, a manual-dispatch release workflow, PR lint validation, and Artifact Hub registration.

**Architecture:** A new public GitHub repo hosts the chart under `charts/arcadedb/`. GitHub Pages serves `index.yaml` from the `gh-pages` branch (managed automatically by `helm/chart-releaser-action`). Releases are triggered manually via `workflow_dispatch` with a version input; the workflow bumps `Chart.yaml`, commits, tags, and publishes in one step. The main monorepo's `k8s/helm/` directory is replaced by a redirect `README.md`.

**Tech Stack:** Helm v3, GitHub Actions, `helm/chart-releaser-action@v1.6.0`, `azure/setup-helm@v4`, GitHub Pages (`gh-pages` branch), Artifact Hub.

---

## File Map

**New repo `ArcadeData/arcadedb-helm`:**

| File | Purpose |
|---|---|
| `charts/arcadedb/Chart.yaml` | Chart metadata - version bumped by release workflow |
| `charts/arcadedb/values.yaml` | Default values |
| `charts/arcadedb/README.md` | Chart-level documentation |
| `charts/arcadedb/templates/` | All template files (10 files) |
| `README.md` | Repo-level: `helm repo add` instructions + Artifact Hub badge |
| `LICENSE` | Apache 2.0 |
| `.gitignore` | Ignore `.cr-release-packages/` |
| `.github/workflows/lint.yml` | PR validation: lint + template matrix |
| `.github/workflows/release.yml` | Manual release: validate, bump, commit, tag, package, publish |

**Existing repo `ArcadeData/arcadedb` (cleanup):**

| File | Change |
|---|---|
| `k8s/helm/` | Deleted entirely |
| `k8s/README.md` | New redirect to arcadedb-helm repo |

---

## Task 1: Create and initialize the arcadedb-helm repository

**Context:** All steps in Tasks 1-6 are performed in the NEW `arcadedb-helm` repo, not the arcadedb monorepo. Choose a local parent directory (e.g. `~/projects/arcade/`) to clone into.

**Files:**
- Create: `README.md`
- Create: `LICENSE`
- Create: `.gitignore`

- [ ] **Step 1: Create the GitHub repository**

```bash
gh repo create ArcadeData/arcadedb-helm \
  --public \
  --description "ArcadeDB Helm chart" \
  --clone
cd arcadedb-helm
```

Expected: repository created and cloned, you are now inside `arcadedb-helm/`.

- [ ] **Step 2: Create `.gitignore`**

```bash
cat > .gitignore << 'EOF'
.cr-release-packages/
.helm/
EOF
```

- [ ] **Step 3: Copy Apache 2.0 LICENSE from the main repo**

```bash
curl -s https://raw.githubusercontent.com/ArcadeData/arcadedb/main/LICENSE > LICENSE
```

Expected: `LICENSE` file present, starts with `Apache License`.

- [ ] **Step 4: Create the repo-level `README.md`**

```markdown
# ArcadeDB Helm Chart

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/arcadedb)](https://artifacthub.io/packages/helm/arcadedb/arcadedb)

The official Helm chart for [ArcadeDB](https://arcadedb.com/), a multi-model database supporting SQL, Cypher, Gremlin, MongoDB, and Redis protocols.

## Install

```bash
helm repo add arcadedb https://arcadedata.github.io/arcadedb-helm/
helm repo update
helm install my-arcadedb arcadedb/arcadedb
```

## Configuration

See [charts/arcadedb/README.md](charts/arcadedb/README.md) and [charts/arcadedb/values.yaml](charts/arcadedb/values.yaml) for all available options.

## Release

New chart versions are published via the GitHub Actions Release workflow:

```
GitHub → Actions → Release → Run workflow → enter version → Run
```

## Contributing

PRs welcome. The lint workflow runs on all pull requests.
```

- [ ] **Step 5: Commit and push**

```bash
git add README.md LICENSE .gitignore
git commit -m "chore: initialize arcadedb-helm repository"
git push origin main
```

Expected: push succeeds, repo visible at `https://github.com/ArcadeData/arcadedb-helm`.

---

## Task 2: Copy chart content

**Context:** Still inside the `arcadedb-helm` directory. The source chart files are in the `arcadedb` monorepo at `k8s/helm/`. Adjust the source path to wherever you have the monorepo cloned.

**Files:**
- Create: `charts/arcadedb/` (entire directory tree)

- [ ] **Step 1: Create the charts directory and copy files**

```bash
mkdir -p charts/arcadedb/templates

# Adjust ARCADEDB_REPO to wherever the main repo is cloned
ARCADEDB_REPO=~/projects/arcade/arcadedb

cp $ARCADEDB_REPO/k8s/helm/Chart.yaml     charts/arcadedb/
cp $ARCADEDB_REPO/k8s/helm/values.yaml    charts/arcadedb/
cp $ARCADEDB_REPO/k8s/helm/README.md      charts/arcadedb/
cp $ARCADEDB_REPO/k8s/helm/templates/*    charts/arcadedb/templates/
```

Expected: `charts/arcadedb/` contains `Chart.yaml`, `values.yaml`, `README.md`, and `templates/` with 10 files.

- [ ] **Step 2: Verify the copy is complete**

```bash
ls charts/arcadedb/templates/
```

Expected output (10 files):
```
_helpers.tpl    hpa.yaml         NOTES.txt         secret.yaml       statefulset.yaml
extra-manifests.yaml  ingress.yaml  networkpolicy.yaml  service.yaml  serviceaccount.yaml
```

- [ ] **Step 3: Lint to confirm the chart is valid**

```bash
helm lint charts/arcadedb/
```

Expected: `1 chart(s) linted, 0 chart(s) failed`

- [ ] **Step 4: Commit**

```bash
git add charts/
git commit -m "feat: add arcadedb Helm chart (moved from ArcadeData/arcadedb k8s/helm/)"
git push origin main
```

---

## Task 3: Create the PR validation workflow

**Files:**
- Create: `.github/workflows/lint.yml`

- [ ] **Step 1: Create the workflow directory and file**

```bash
mkdir -p .github/workflows
```

Create `.github/workflows/lint.yml` with this exact content:

```yaml
name: Lint

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Helm
        uses: azure/setup-helm@v4
        with:
          version: v3.14.0

      - name: Lint
        run: helm lint charts/arcadedb/

      - name: Template - default single-node
        run: helm template test charts/arcadedb/ > /dev/null

      - name: Template - HA replicaCount=3
        run: helm template test charts/arcadedb/ --set replicaCount=3 > /dev/null

      - name: Template - HPA valid quorum
        run: |
          helm template test charts/arcadedb/ \
            --set autoscaling.enabled=true \
            --set autoscaling.minReplicas=3 \
            --set autoscaling.maxReplicas=5 > /dev/null

      - name: Template - HPA quorum guard fires
        run: |
          helm template test charts/arcadedb/ \
            --set autoscaling.enabled=true \
            --set autoscaling.minReplicas=1 \
            --set autoscaling.maxReplicas=5 2>&1 | grep -i "quorum"

      - name: Template - ingress enabled
        run: |
          helm template test charts/arcadedb/ \
            --set ingress.enabled=true > /dev/null

      - name: Template - NetworkPolicy enabled
        run: |
          helm template test charts/arcadedb/ \
            --set networkPolicy.enabled=true > /dev/null
```

- [ ] **Step 2: Commit and push**

```bash
git add .github/workflows/lint.yml
git commit -m "ci: add PR lint and template validation workflow"
git push origin main
```

- [ ] **Step 3: Verify the workflow ran green**

```bash
gh run list --workflow=lint.yml --limit=3
```

Expected: latest run shows `completed` with `success` status. If it failed, run `gh run view <run-id> --log-failed` to diagnose.

---

## Task 4: Create the release workflow

**Files:**
- Create: `.github/workflows/release.yml`

- [ ] **Step 1: Create `.github/workflows/release.yml`**

```yaml
name: Release

on:
  workflow_dispatch:
    inputs:
      version:
        description: "Chart version (e.g. 26.5.2)"
        required: true
      app_version:
        description: "ArcadeDB server version (e.g. 26.5.1) — leave blank to keep current appVersion"
        required: false

jobs:
  release:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Validate version format
        run: |
          if ! echo "${{ github.event.inputs.version }}" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
            echo "Error: '${{ github.event.inputs.version }}' is not valid semver (X.Y.Z)"
            exit 1
          fi

      - name: Set up Helm
        uses: azure/setup-helm@v4
        with:
          version: v3.14.0

      - name: Update Chart.yaml
        run: |
          VERSION="${{ github.event.inputs.version }}"
          APP_VERSION="${{ github.event.inputs.app_version }}"
          sed -i "s/^version:.*/version: ${VERSION}/" charts/arcadedb/Chart.yaml
          if [ -n "${APP_VERSION}" ]; then
            sed -i "s/^appVersion:.*/appVersion: \"${APP_VERSION}\"/" charts/arcadedb/Chart.yaml
          fi
          echo "--- Chart.yaml after update ---"
          cat charts/arcadedb/Chart.yaml

      - name: Lint
        run: helm lint charts/arcadedb/

      - name: Commit and tag
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add charts/arcadedb/Chart.yaml
          git commit -m "chore(helm): release v${{ github.event.inputs.version }}"
          git tag "v${{ github.event.inputs.version }}"
          git push origin main --follow-tags

      - name: Run chart-releaser
        uses: helm/chart-releaser-action@v1.6.0
        env:
          CR_TOKEN: "${{ secrets.GITHUB_TOKEN }}"
        with:
          charts_dir: charts
          skip_existing: true
```

- [ ] **Step 2: Commit and push**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add manual-dispatch release workflow"
git push origin main
```

- [ ] **Step 3: Verify the workflow appears in GitHub Actions**

```bash
gh workflow list
```

Expected: both `Lint` and `Release` appear in the output.

---

## Task 5: Run first release and enable GitHub Pages

**Context:** `helm/chart-releaser-action` automatically creates and pushes to a `gh-pages` branch on the first run. After the branch exists, GitHub Pages must be manually enabled in the repo settings (one-time only). The stable URL will be `https://arcadedata.github.io/arcadedb-helm/`.

- [ ] **Step 1: Trigger the release workflow**

```bash
gh workflow run release.yml \
  --field version=26.5.1 \
  --field app_version=26.5.1
```

- [ ] **Step 2: Wait for the workflow to complete**

```bash
gh run watch $(gh run list --workflow=release.yml --limit=1 --json databaseId --jq '.[0].databaseId')
```

Expected: workflow completes with `success`. If it fails, check logs:
```bash
gh run view --log-failed
```

- [ ] **Step 3: Verify GitHub Release was created**

```bash
gh release list
```

Expected: `v26.5.1` release appears with a `arcadedb-26.5.1.tgz` asset.

- [ ] **Step 4: Enable GitHub Pages on the `gh-pages` branch**

Navigate to: `https://github.com/ArcadeData/arcadedb-helm/settings/pages`

Set:
- Source: `Deploy from a branch`
- Branch: `gh-pages`
- Folder: `/ (root)`

Click **Save**. GitHub Pages deployment takes 1-2 minutes.

- [ ] **Step 5: Verify GitHub Pages is serving index.yaml**

```bash
curl -s https://arcadedata.github.io/arcadedb-helm/index.yaml | head -5
```

Expected output:
```yaml
apiVersion: v1
entries:
  arcadedb:
  - apiVersion: v2
    appVersion: 26.5.1
```

- [ ] **Step 6: Verify helm repo add and install work end-to-end**

```bash
helm repo add arcadedb-test https://arcadedata.github.io/arcadedb-helm/
helm repo update
helm search repo arcadedb-test
helm install dry-run-test arcadedb-test/arcadedb --dry-run --generate-name 2>&1 | grep "NAME:"
helm repo remove arcadedb-test
```

Expected: `helm search repo` shows `arcadedb-test/arcadedb` at version `26.5.1`. `helm install --dry-run` outputs a line starting with `NAME:`.

---

## Task 6: Register on Artifact Hub and add ownership annotation

**Context:** Artifact Hub registration requires the `index.yaml` to be publicly accessible (done in Task 5). The `repositoryID` UUID is assigned by Artifact Hub and proves ownership. It must be added to `Chart.yaml` so Artifact Hub can verify it when indexing.

- [ ] **Step 1: Register the repository on Artifact Hub**

1. Go to [https://artifacthub.io](https://artifacthub.io) and sign in (or create an account)
2. Click your avatar → **Control Panel** → **Add repository**
3. Fill in:
   - Kind: `Helm charts`
   - Name: `arcadedb`
   - Display name: `ArcadeDB`
   - URL: `https://arcadedata.github.io/arcadedb-helm/`
4. Click **Add**
5. Copy the `repositoryID` UUID shown (format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

- [ ] **Step 2: Add the ownership annotation to `charts/arcadedb/Chart.yaml`**

Add this block after `appVersion` in `charts/arcadedb/Chart.yaml` (replace the UUID with the one copied in Step 1):

```yaml
annotations:
  artifacthub.io/repositoryID: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

The full `Chart.yaml` should now look like:

```yaml
apiVersion: v2
name: arcadedb
description: |
  A Helm chart for ArcadeDB

type: application

version: 26.5.1

appVersion: "26.5.1"

annotations:
  artifacthub.io/repositoryID: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

- [ ] **Step 3: Commit the annotation (no release needed - Artifact Hub picks it up on next release)**

```bash
git add charts/arcadedb/Chart.yaml
git commit -m "chore: add Artifact Hub repository ownership annotation"
git push origin main
```

- [ ] **Step 4: Verify Artifact Hub indexes the chart**

Wait 5-10 minutes, then:

```bash
helm search hub arcadedb 2>/dev/null | grep arcadedb-helm || \
  echo "Not yet indexed - check https://artifacthub.io/packages/search?q=arcadedb"
```

The chart page will be live at: `https://artifacthub.io/packages/helm/arcadedb/arcadedb`

---

## Task 7: Clean up the main arcadedb repo

**Context:** Switch back to the `arcadedb` monorepo for this task. The goal is to remove `k8s/helm/` and leave a clear redirect. Also close issue #4034 now that the chart has a proper home.

**Files (in `ArcadeData/arcadedb` monorepo):**
- Delete: `k8s/helm/` (entire directory)
- Create: `k8s/README.md`

- [ ] **Step 1: Delete the helm directory**

```bash
# From inside the arcadedb monorepo root
git rm -r k8s/helm/
```

Expected: all `k8s/helm/` files staged for deletion.

- [ ] **Step 2: Create `k8s/README.md`**

```markdown
# Kubernetes

The ArcadeDB Helm chart has moved to its own repository:

**https://github.com/ArcadeData/arcadedb-helm**

## Install

```bash
helm repo add arcadedb https://arcadedata.github.io/arcadedb-helm/
helm repo update
helm install my-arcadedb arcadedb/arcadedb
```
```

- [ ] **Step 3: Commit and push**

```bash
git add k8s/README.md
git commit -m "chore: remove k8s/helm/ - chart moved to ArcadeData/arcadedb-helm"
git push origin main
```

- [ ] **Step 4: Close issue #4034 with a pointer to the new repo**

```bash
gh issue comment 4034 \
  --repo ArcadeData/arcadedb \
  --body "The Helm chart refactored in this issue has been moved to its permanent home at https://github.com/ArcadeData/arcadedb-helm. Install via:

\`\`\`bash
helm repo add arcadedb https://arcadedata.github.io/arcadedb-helm/
helm repo update
helm install my-arcadedb arcadedb/arcadedb
\`\`\`"

gh issue close 4034 --repo ArcadeData/arcadedb --reason completed
```

Expected: issue #4034 is closed with the comment visible.
