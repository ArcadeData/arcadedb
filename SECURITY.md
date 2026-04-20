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

Thank you for helping keep ArcadeDB and its users safe.
