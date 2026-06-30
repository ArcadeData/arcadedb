# Issue #4804 - gRPC `executeCommand` failures counted as successes (status OK)

## Summary

`ArcadeDbGrpcService.executeCommand` reports execution failures *in-band* as
`ExecuteCommandResponse{success=false}` and then closes the RPC with `onCompleted()`, so the
gRPC status is `OK`. `GrpcMetricsInterceptor` keyed its error counter off `!status.isOk()`, so
every command failure was counted as a success and error observability for the most-used RPC
was broken.

## Root cause

Two layers report command failures in-band:
- `ArcadeDbGrpcService.executeCommandInternal` catches every exception and returns a
  `success=false` response (rolls back if needed).
- `ArcadeDbGrpcService.executeCommand`'s outer catch likewise replies with `success=false` via
  `onNext`/`onCompleted`.

Both terminate the RPC with status `OK`, and `GrpcMetricsInterceptor.close` only incremented the
error counter when `!status.isOk()`.

## Why not the issue's suggested `onError` change

The issue suggests surfacing failures via `resp.onError(Status.INTERNAL…)`. That would change the
RPC contract from in-band `success=false` to a gRPC error status. Two existing tests pin the
current in-band contract and would break:
- `GrpcServerIT.executeCommandInvalidSqlReturnsError` - asserts invalid SQL returns
  `success=false` (status OK).
- `Issue4794GrpcPerDbAuthorizationIT.crossDatabaseCommandIsDenied` - asserts a denied
  cross-database command returns `success=false` with an access-denied message.

Repo policy is to never modify or delete existing tests. The fix therefore targets the locus of
the reported impact (the metrics counter) and leaves the in-band response contract intact, so all
existing tests stay green while the observability defect is corrected.

## Fix

`GrpcMetricsInterceptor`:
- Override `sendMessage` on the wrapped `ServerCall` to detect an in-band command failure
  (`ExecuteCommandResponse.success == false`).
- In `close`, increment the error counter when `!status.isOk()` **or** an in-band failure was
  observed.
- Added package-private `getErrorCount()` / `getRequestCount()` test seams so error accounting can
  be verified without reflection.

## Tests

`Issue4804GrpcCommandErrorMetricsTest` (new unit test):
- `inBandCommandFailureWithStatusOkIsCountedAsError` - a `success=false` response followed by a
  `Status.OK` close increments the error counter (fails before the fix, passes after).
- `successfulCommandWithStatusOkIsNotCountedAsError` - a `success=true` response with `Status.OK`
  is not counted as an error.
- `nonOkStatusIsStillCountedAsError` - pre-existing non-OK accounting is preserved.

## Affected components

- `grpcw` module: `com.arcadedb.server.grpc.GrpcMetricsInterceptor`

## Impact

Restores error observability for the most-used gRPC RPC: command failures are now counted as
errors by the metrics interceptor even though the response is still reported in-band as
`success=false` (preserving the existing client-facing contract).
