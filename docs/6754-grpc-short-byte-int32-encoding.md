# #6754: gRPC returns SHORT/BYTE column values as string_value instead of int32

## Summary

All three gRPC value encoders (`ArcadeDbGrpcService.toGrpcValue`, `GrpcTypeConverter.toGrpcValue`,
`ProtoUtils.toGrpcValue` in `grpc-client`) had `instanceof` branches for `Boolean`/`Integer`/`Long`/
`Float`/`Double`/... but none for `Short`/`Byte`. A `SHORT`/`BYTE` column value therefore fell through to
the `String.valueOf(o)` fallback and was sent as `string_value` instead of `int32_value`, diverging from
the HTTP/SQL paths and from a type-sensitive client's expectations.

## Fix

Added `Short`/`Byte` branches immediately after the `Integer` branch in all three encoders, each mapping to
`int32_value` (Java allows the implicit unboxing + widening conversion `Short`/`Byte` -> `int` at the
`setInt32Value(int)` call site).

## Tests

- `GrpcTypeConverterTest.toGrpcValueShort` / `toGrpcValueByte` (grpcw, unit)
- `ProtoUtilsTest.toGrpcValueShort` / `toGrpcValueByte` (grpc-client, unit)
- `ArcadeDbGrpcServiceExtendedTest.executeQueryReturnsShortAndByteColumnsAsInt32NotString` (grpcw,
  end-to-end over a real gRPC connection): creates a SHORT/BYTE-typed vertex property, inserts values,
  queries them back, and asserts the wire `GrpcValue.KindCase` is `INT32_VALUE`, not `STRING_VALUE` -
  the existing `Issue5046GrpcByteShortWriteIT` could not have caught this regression because it asserts via
  `getInteger()`, which parses the (wrongly) returned string back into a number.

All new/modified tests verified to fail without the fix (reverted the encoder changes and reran).
