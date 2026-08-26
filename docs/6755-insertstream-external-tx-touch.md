# #6755: gRPC insertStream on an external transaction reaped mid-stream

## Summary

`insertStream`'s external-transaction branch resolved and touched the transaction (refreshing
`lastAccessMs`) only on the first inbound chunk, via `resolveAuthorizedTransaction` ->
`lookupActiveTransaction` -> `touch()`. Every subsequent chunk dispatched straight onto the transaction's
executor without touching it. A stream whose total active duration exceeded `txMaxIdleMs` (default 5
minutes) - even with every individual gap between chunks well under that threshold - was reclaimed by the
idle-transaction reaper while the client was still sending, discarding all rows streamed so far (the
external transaction is never committed until the caller's own `commitTransaction` call, so a mid-stream
reap loses everything, not just the tail).

## Fix

`ArcadeDbGrpcService.insertStream`'s subsequent-chunk branch (`onNext`, the `else` arm once `ctx != null`)
now touches the external transaction (`extTxCtxRef.get().touch()`) on every chunk, matching every other
transaction-scoped RPC (each of which calls `resolveAuthorizedTransaction`/`touch()` per call).

## Tests

- `Issue6755InsertStreamExternalTxTouchIT` (grpcw, `@Tag("slow")`): configures a short
  `arcadedb.grpc.tx.maxIdleMs` (600ms) / `arcadedb.grpc.tx.reaperPeriodMs` (100ms) via system properties,
  begins an external transaction, and streams 4 chunks 250ms apart (each gap under the idle threshold, but
  the stream's total ~750ms duration exceeds it). Asserts all 4 rows are inserted with no errors, the
  transaction is still alive to commit, and the commit persists all 4 rows.

Verified to fail without the fix: reverted the `touch()` call and reran - the stream failed mid-way with
`FAILED_PRECONDITION: Unknown or expired transaction id ... rolled back by the idle reaper`.
