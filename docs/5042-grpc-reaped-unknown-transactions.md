# Issue #5042 - gRPC reaped/unknown transactions cause silent data loss and partial commits

## Symptom
When a client-supplied transaction id is unknown to the server (most commonly because the idle reaper
already rolled the transaction back after 5 minutes), the gRPC layer silently does the wrong thing:
- Client `commit()`/`rollback()` treat a `committed=false`/`rolledBack=false` server response as success.
- Server write RPCs (`executeCommand`, `createRecord`, `updateRecord`, `deleteRecord`) fall through to a
  non-transactional auto-commit path for a non-blank-but-unknown txId, producing partial commits.
- `streamQuery` ignores `transaction_id`, so streamed reads inside a client transaction cannot see the
  transaction's uncommitted writes.

## Root cause
The protocol distinguishes "the RPC succeeded" (`success`) from "the transaction was actually committed"
(`committed`), but the client collapses them. The server write handlers treat a missing/blank txId as
"no external transaction -> auto-transaction", which is correct for a genuinely absent txId but wrong for a
txId the client did supply that has since been reaped. `streamQuery` never resolves the supplied txId.

## Fix (TX-2, TX-5, TX-8, CON-5)
1. Client `commit()` throws `TransactionException` when `response.getCommitted() == false`; `rollback()`
   throws when `response.getRolledBack() == false`.
2. Server write RPCs reject a non-blank-but-unknown txId with `FAILED_PRECONDITION` instead of
   auto-committing. A genuinely absent/blank txId keeps the auto-transaction path.
3. `streamQuery` resolves the supplied txId and runs the stream on the transaction's dedicated executor
   thread (streaming analogue of the #4260 executeQuery fix); it no longer begins/commits a throwaway read
   tx when a tx is supplied. The client now attaches the active transaction to stream requests.
4. CON-5: the idle reaper re-reads `lastAccessMs` immediately before `activeTransactions.remove(key,value)`
   so a just-touched (active) transaction is not reaped in the TOCTOU window.

## Tests
- `grpcw` `Issue5042ReapedTransactionIT` - write RPCs reject unknown non-blank txId; blank still auto-commits;
  streamQuery honors txId and sees uncommitted writes.
- `grpc-client` `Issue5042CommitRollbackThrowIT` - reaped-tx commit()/rollback() throw; streamed query inside
  a transaction sees the transaction's uncommitted write.

## Impact
High-severity silent data loss / partial commits closed; unknown-tx writes now fail loudly.
