# Issue #5044 - gRPC value encoder/decoder divergences

## Symptom
Three near-identical Java <-> protobuf value encoders diverge in branch coverage,
producing silent data corruption. Highest priority (COR-8): the gRPC **client**
encoder (`ProtoUtils.toGrpcValue`) has no array branch, so writing a vector
(`float[]`) property sends `String.valueOf(float[])` (e.g. `"[F@6d03e736"`),
destroying the vector.

## Root cause
- `ProtoUtils.toGrpcValue` (client) handles `Collection` but not Java arrays.
  Primitive/object arrays (`float[]`, `double[]`, `int[]`, `long[]`, `Object[]`)
  are not `Collection`s, so they fall through to the `String.valueOf(...)` fallback.
- `GrpcTypeConverter.toGrpcValue` (the dead-in-production static converter that the
  unit test validates) has the same array gap.
- `ProtoUtils.fromGrpcValue` lacks the `if (v == null)` guard the other two decoders
  have, so a null `GrpcValue` NPEs instead of decoding to `null` (COR-16, part).

## Fix
- `ProtoUtils.toGrpcValue`: add a generic array branch (via `java.lang.reflect.Array`)
  that encodes any Java array - including `float[]`/`double[]` vectors - as a
  `GrpcList`, mirroring the `Collection` branch. `byte[]` keeps its existing
  dedicated `BYTES` branch (checked earlier).
- `ProtoUtils.fromGrpcValue`: add the `if (v == null) return null;` guard.
- `GrpcTypeConverter.toGrpcValue`: add the same generic array branch so the
  matrix cell converges and the unit test validates array handling.

## Tests
- `grpc-client` `ProtoUtilsArrayEncoderTest` - `float[]`/`double[]`/`int[]`/`long[]`/
  `Object[]` encode to `LIST` and round-trip to a numeric list; null `GrpcValue`
  decodes to `null` without NPE.
- `grpcw` `GrpcTypeConverterArrayEncoderTest` - `float[]`/`double[]` encode to `LIST`.

## Impact
Client-side vector writes and any array-valued property now round-trip as lists
instead of corrupting to a string. No behavior change for existing scalar/collection
paths. Fixes are additive branches; no existing test modified.

## Deferred (out of scope for this PR - documented for the maintainer)
- **COR-9 (LINK decode -> RID on the client).** Making `ProtoUtils.fromGrpcValue`
  return a `RID` instead of the rid `String` would break the existing
  `ProtoUtilsTest.fromGrpcValueLink` assertion, which codifies the current String
  behavior. Changing an existing test is out of bounds for this workflow; left to
  the maintainer.
- **COR-7 (embedded-document type loss) and COR-13 (server param `logical_type=="json"`
  BYTES).** These live on the server path (`ArcadeDbGrpcService`, ~4590 lines) and the
  insert paths (`applyGrpcRecord`/`toPropertyArray`); verifying them properly needs an
  end-to-end server integration harness rather than an encoder unit test.
- **Full single-encoder consolidation.** `grpc-client` cannot depend on `grpcw`
  (server) in compile scope, so a literally-shared encoder is architecturally
  constrained; convergence here is by matching branches, not by deletion.
