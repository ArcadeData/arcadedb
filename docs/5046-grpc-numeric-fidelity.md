# Issue #5046 - gRPC numeric fidelity (BYTE/SHORT CCE + BigDecimal string degradation)

## Symptom
- **COR-4 (High):** Writing an `int32`/`float`/`double` wire value into a schema `BYTE` or `SHORT`
  property crashed the RPC with `INTERNAL` /
  `java.lang.ClassCastException: class java.lang.Integer cannot be cast to class java.lang.Long`.
- **COR-10 (Medium):** A `BigDecimal` whose unscaled value exceeds 63 bits silently round-tripped
  back as a `java.lang.String` on the schemaless path (type + precision loss).

## Root cause
- **COR-4:** `convertWithSchemaType` narrowed BYTE/SHORT with `(byte) (long) fromGrpcValue(v)`.
  `fromGrpcValue` returns `Integer` for `INT32_VALUE` (and `Double`/`Float` for the floating kinds),
  so the `(long)` unboxing cast required the runtime type to be `Long` and threw a CCE.
- **COR-10:** Encoders fell back to `string_value` + `logical_type="decimal"` when the unscaled value
  did not fit in a signed 64-bit `sint64`, but no decoder consulted `logical_type` on a `STRING_VALUE`.

## Fix
- **COR-4:** Narrow via `((Number) fromGrpcValue(v)).byteValue()` / `.shortValue()` in
  `ArcadeDbGrpcService.convertWithSchemaType`.
- **COR-10:** Added a `bytes unscaled_bytes = 3` field to `GrpcDecimal` in `arcadedb-server.proto`
  (big-endian two's-complement, precedence over `unscaled` when non-empty). All BigDecimal encoders
  now emit `decimal_value` losslessly (unscaled_bytes for >63-bit magnitudes) and all decoders
  reconstruct through shared helpers `GrpcTypeConverter.toGrpcDecimal`/`toBigDecimal` (grpcw) and
  `ProtoUtils.toGrpcDecimal`/`toBigDecimal` (grpc-client). The string fallback is removed.

## Files changed
- `grpc/src/main/proto/arcadedb-server.proto` - new `unscaled_bytes` field.
- `grpcw/.../ArcadeDbGrpcService.java` - COR-4 narrowing; BigDecimal encode/decode + JSON path via helpers.
- `grpcw/.../GrpcTypeConverter.java` - `toGrpcDecimal`/`toBigDecimal` helpers; encode/decode via helpers.
- `grpc-client/.../utils/ProtoUtils.java` - `toGrpcDecimal`/`toBigDecimal` helpers; encode/decode via helpers.

## Tests
- `grpcw/.../GrpcTypeConverterTest` - large/negative >63-bit BigDecimal encode + round-trip (value + scale + unscaled).
- `grpc-client/.../ProtoUtilsTest` - updated `toGrpcValueBigDecimalLarge` (was asserting the buggy
  string fallback) to assert lossless `decimal_value`; added `roundTripBigDecimalLargeUnscaled`.
- `grpc-client/.../Issue5046GrpcByteShortWriteIT` - end-to-end BYTE/SHORT writes from int32/double/float
  wire kinds succeed and store the correct narrowed value (no CCE).

## Impact
- Small decimals still encode via `unscaled` sint64 (backward compatible on the wire).
- No behavior change for values that already fit in 63 bits; large decimals now lossless.

## Upgrade / version-skew note
- The `unscaled_bytes` field (tag 3) is only ever populated for `BigDecimal` values whose unscaled
  magnitude exceeds 63 bits. Peers built before this change ignore the unknown field and would decode
  such a value as `unscaled=0` (its default), reconstructing `BigDecimal(0, scale)`. Before this change
  those same values arrived as a `String`, so mixed-version deployments should upgrade gRPC client and
  server together when high-precision (>63-bit unscaled) decimals are in use. Decimals that fit in 63
  bits are unaffected and fully backward compatible.
