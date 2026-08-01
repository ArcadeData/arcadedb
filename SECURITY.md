# Security Policy

The ArcadeDB team takes the security of the database and its ecosystem seriously. We appreciate responsible disclosure of vulnerabilities and aim to acknowledge, investigate, and remediate valid reports as quickly as possible.

## Reporting a vulnerability

**Please do not open public GitHub issues for security problems.**

You have two private channels:

- **GitHub Security Advisories** (preferred): https://github.com/ArcadeData/arcadedb/security/advisories/new
- **Email**: support@arcadedb.com

Either channel reaches the maintainers privately and lets us coordinate a fix and a CVE before the issue becomes public.

### What to include in a report

To help us triage quickly, please include:

- Affected version(s) of ArcadeDB and the affected component (engine, server, HA, wire protocol, Studio).
- A clear description of the issue and the security impact.
- Step-by-step reproduction instructions, or a minimal proof-of-concept.
- The configuration and environment where you observed the issue (OS, Java version, deployment mode).
- Any relevant logs, request/response captures, or patches you already have.

## Supported versions

Security fixes are delivered on the latest stable minor release line. Older minor lines receive fixes only when the issue is severe and a backport is practical.

| Version                 | Status                           |
| ----------------------- | -------------------------------- |
| Latest stable (main)    | Supported                        |
| Previous minor release  | Critical fixes only              |
| Older versions          | Not supported - please upgrade   |

Check [GitHub Releases](https://github.com/ArcadeData/arcadedb/releases) for the current supported version.

## Our response

When you report an issue we will:

1. Acknowledge receipt within **3 business days**.
2. Provide an initial assessment (confirmed / needs more info / not a vulnerability) within **10 business days**.
3. Keep you updated at least every **14 days** while we work on a fix.
4. Coordinate a release and, if applicable, a CVE through the GitHub Security Advisory process.
5. Publish the advisory only after a fixed release is available, unless the vulnerability is already public.

## Disclosure and credit

We follow a **coordinated disclosure** model. By default we credit reporters in the published advisory and the release notes. If you prefer to remain anonymous or to use a specific handle, let us know in your report.

We ask that reporters:

- Give us reasonable time to fix the issue before public disclosure.
- Do not access, modify, or exfiltrate data that is not yours.
- Do not run denial-of-service tests against production systems.

## Scope

In scope:

- ArcadeDB engine, server, HA, Studio, wire protocol modules (HTTP, Postgres, MongoDB, Redis, Bolt, Gremlin, gRPC, GraphQL), and the official Docker images and client libraries published by the project.

Out of scope:

- Third-party forks and derivative products not maintained by the ArcadeDB team.
- Vulnerabilities in upstream dependencies that are already tracked by their own advisories. (Please still let us know so we can upgrade.)
- Issues that require physical access to an already-compromised host.

## How we triage dependency alerts

Dependabot alerts and security updates are enabled on this repository. We triage
them by **where the dependency runs**, not by severity alone.

**Runtime dependencies** - anything reachable from a published artifact (the
engine, server, HA, wire protocol modules, Studio, the Docker images, the client
libraries) - are always reviewed and patched. No automatic dismissal applies to
them at any severity.

**Development-scope dependencies** - build plugins, test frameworks, and the
`e2e-*` harnesses - never ship in a distribution and execute only in CI, against
inputs this repository controls. Two auto-triage rules dismiss the classes of
alert that are not actionable there:

| Rule | Covers |
| ---- | ------ |
| GitHub-curated preset, *Dismiss low-impact alerts for development-scoped dependencies* | Resource-exhaustion advisories: CWE-400, CWE-674, CWE-754, CWE-770, CWE-835 |
| Custom rule, *Dismiss dev-scope algorithmic complexity* (`cwe:407 scope:development`) | CWE-407 (inefficient algorithmic complexity), development scope only |

The custom rule exists because the curated preset dismisses an advisory only when
*every* CWE on it is in the preset's set, so a ReDoS tagged CWE-400 **and**
CWE-407 stays open while the same class of issue tagged CWE-400 alone is
dismissed. Both rules are configured in the repository's security settings and
therefore leave no trace in the source tree, which is why they are recorded here.

**Development scope does not mean ignored.** These rules are deliberately narrow.
At development scope we still review, and never auto-dismiss, path traversal
(CWE-22), information disclosure (CWE-200), code injection, and malicious or
compromised packages - a build tool that can read or exfiltrate files is a real
supply-chain risk regardless of whether it ships. As a backstop,
`.github/workflows/e2e-dependency-audit.yml` fails on any runtime-scope finding
in the E2E harnesses and reports development-scope findings without blocking.

Some advisories have no fixed release on the major line a pinned upstream
dependency requires. Where we knowingly carry one, the reason is recorded in the
relevant `pom.xml` or in `.github/dependabot.yml` rather than being silently
dismissed.

**The Meterian scan reports, it does not gate.** `.github/workflows/meterian.yml`
runs `continue-on-error` on purpose. Meterian fails a build on a composite
security *score* computed over the whole transitive graph of every language in
the repository, which no individual pull request can move - so gating on it
produced a check that was red on every commit and taught reviewers to ignore red,
which is worse than not gating at all. Nothing is hidden: the scan still runs on
pull requests, on `main` and nightly, and still uploads its SARIF, so every
finding lands as a GitHub code-scanning alert. **Those alerts are the source of
truth for Meterian findings and they do need triaging** - a green Meterian check
means the scan completed, not that it found nothing. The gates that do block are
Dependabot, `dependency-review`, and `.github/workflows/e2e-dependency-audit.yml`
described above.

None of this changes what you should report: if you can show that a
development-scope dependency is reachable from a shipped artifact, or exploitable
in a way we have not considered, please tell us through the private channels
above.

Thank you for helping keep ArcadeDB and its users safe.
