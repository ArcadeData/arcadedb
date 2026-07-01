# Issue #4838 - HA_K8S_DNS_SUFFIX applied only to self host, not to peer addresses

## Problem

`ArcadeDBServer.assignHostAddress()` builds the self-advertised host as
`HOSTNAME + HA_K8S_DNS_SUFFIX` when running inside Kubernetes (`HA_K8S=true`).
However `RaftPeerAddressResolver.parsePeerList()` derives every peer's Raft/HTTP/HTTPS
address straight from the raw `HA_SERVER_LIST` host strings and never applies the
suffix. Result: when an operator writes short pod names (e.g. `arcadedb-0`) in
`HA_SERVER_LIST` and configures `k8sSuffix`, the peers keep their short names, fail
to resolve (NXDOMAIN) and the cluster silently never forms. The suffix is effectively
a dead knob for peer resolution.

The configuration description states the suffix is used "to reach the other servers",
so applying it only to self is a defect.

## Root cause

`RaftPeerAddressResolver.parsePeerList(serverList, defaultPort)` has no knowledge of the
K8s DNS suffix; it builds addresses verbatim from the parsed host component.

## Fix

- Add an overload `parsePeerList(serverList, defaultPort, k8sDnsSuffix)` that appends the
  DNS suffix to each peer host (Raft, HTTP and HTTPS addresses), consistent with how the
  self host is built in `assignHostAddress`.
- The application is idempotent (skips a host already ending with the suffix) and skips
  raw IP literals and `localhost`, which do not need DNS resolution.
- The legacy 2-arg `parsePeerList` delegates with an empty suffix, so non-K8s behavior is
  unchanged.
- `RaftHAServer` reads `HA_K8S` / `HA_K8S_DNS_SUFFIX` and passes the suffix only when
  running inside Kubernetes (default suffix is empty, so non-K8s deployments are unaffected).

## Tests

`RaftHAServerAddressParsingTest` (new methods):
- suffix appended to short peer hosts (Raft + HTTP) and reflected in the peer ID
- suffix appended in object form and to HTTPS address
- idempotent when the host is already fully qualified
- skipped for IPv4 literal and for localhost
- empty/absent suffix leaves hosts unchanged (legacy 2-arg path)

## Impact

K8s-only behavior gated behind `HA_K8S` + non-empty `k8sSuffix`. No change for any
existing deployment that already uses FQDNs or runs outside Kubernetes.
