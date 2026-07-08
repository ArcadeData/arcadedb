# Bolt: make writeTo() the sole header authority in PackStreamStructure

Issue: [#5082](https://github.com/ArcadeData/arcadedb/issues/5082)
Epic: [#4882](https://github.com/ArcadeData/arcadedb/issues/4882) - Bolt Driver Compatibility Certification
Tracking: [#4890](https://github.com/ArcadeData/arcadedb/issues/4890) - protocol/type-fidelity gaps
Follow-up to: [#5001](https://github.com/ArcadeData/arcadedb/issues/5001) (PR #5059, Bolt 5.x version-gated wire encoding)

## Motivation

After #5001 added Bolt 5.x version-gated wire encoding, the `PackStreamStructure`
accessors `getSignature()` and `getFieldCount()` became inconsistent with
`writeTo()` on the version-gated structures:

- `BoltNode` / `BoltRelationship` / `BoltUnboundRelationship`: `getFieldCount()`
  returns the Bolt 4.x base count (3 / 5 / 3) while `writeTo()` emits 4 / 8 / 4
  when the negotiated major is >= 5 (adding `element_id` fields).
- `BoltDateTimeStructure`: `getSignature()` returns the legacy `'F'` / `'f'`
  signature while `writeTo()` emits the UTC `'I'` / `'i'` signature on Bolt 5.0+.

This is **inert today** and was verified during review of this design:
`PackStreamWriter` serializes structures exclusively through `writeTo()` (the
`writeValue` dispatch at `PackStreamWriter.java` calls `((PackStreamStructure)
value).writeTo(this)` and never consults the accessors). The only main-source
consumer of the `writeStructureHeader(getSignature(), getFieldCount())` idiom is
`BoltPointStructure.writeTo` - a version-invariant struct.

It remains a latent footgun: any future serialization path that calls
`writeStructureHeader(getSignature(), getFieldCount())` for a version-gated struct
would emit a header/body mismatch on a 5.x connection and corrupt the stream. The
accessors cannot be made version-correct because they are no-arg interface methods
with no access to the negotiated version (only `writeTo()` has it, via the writer).
Raised repeatedly by Claude and Gemini reviews on PR #5059; deferred out of that PR
because it is a serialization-layer refactor, not a protocol-negotiation change.

## Scope verification (done up front against the code)

The interface `com.arcadedb.bolt.packstream.PackStreamStructure` declares three
methods: `getSignature()`, `getFieldCount()`, `writeTo()`. Seven classes implement
it:

| Class | version-gated? | `writeTo()` header today | accessors consumed by `writeTo()`? |
|---|---|---|---|
| `BoltNode` | yes (>=5 adds `element_id`) | inline literals `3`/`4` | no |
| `BoltRelationship` | yes (>=5 adds element ids) | inline literals `5`/`8` | no |
| `BoltUnboundRelationship` | yes (>=5 adds `element_id`) | inline literals `3`/`4` | no |
| `BoltDateTimeStructure` | yes (>=5 UTC signature) | inline `SIG_*`, count `3` | no |
| `BoltPath` | no | inline `SIGNATURE`, `3` | no |
| `BoltTemporalStructure` | no | inline `signature`, `fields.length` | no |
| `BoltPointStructure` | no | **`writeStructureHeader(getSignature(), getFieldCount())`** | **yes** |

**Callers that look like consumers but are not** (different types, each keeps its
own methods - untouched by this change):

- `BoltStructureMapper.fromInboundStructure` calls `structure.getSignature()` where
  `structure` is a `PackStreamReader.StructureValue` (inbound decode holder).
- `BoltMessage.parse` calls `structure.getSignature()` on a `StructureValue`, and
  `BoltMessage` has its own `getSignature()`.
- `PackStreamReader.StructureValue` defines its own `getSignature()` /
  `getFieldCount()`.

**Correction to the issue text:** #5082 names only `BoltStructureTest` for test
updates. The *interface* accessors are actually asserted in **three** test files:
`BoltStructureTest` (Node/Rel/UnboundRel/Path), `BoltTypeRoundTripTest`
(`BoltTemporalStructure` + `BoltPointStructure`), and `Bolt4998PathMappingTest`
(`BoltPointStructure`). Assertions that *look* affected but are not, because their
receiver is a `PackStreamReader.StructureValue` / `BoltMessage` (different types
that keep their own methods), and therefore stay unchanged:

- `BoltDateTimeStructureTest` - all 8 `getSignature()` calls are on the
  `StructureValue` returned by its `roundTrip()` / `mapperRoundTrip()` helpers,
  which already serialize through `writeTo()`. **No change needed.**
- `PackStreamTest` (`struct` is a `StructureValue`) and `BoltMessageTest`
  (`msg` is a `BoltMessage`). **No change needed.**
- In `BoltTypeRoundTripTest` the `wire.getSignature()` assertion (TYPE-011) is on a
  `StructureValue` and stays; only the pre-wire `s.*` / `p.*` accessor assertions go.

## Design decision

**Full removal** (the issue's literal design): drop `getSignature()` and
`getFieldCount()` from the interface *and* from all seven implementers, inlining
the signature + field count directly into every `writeTo()`. `writeTo()` / the
serialized wire bytes become the single source of truth for structure-header shape.
The version-invariant structs (Point, Path, Temporal) lose their accessors too,
even though only the version-gated ones are footguns - chosen for a uniform
single-method contract that matches the acceptance criteria exactly, over a split
API that keeps plain getters on some structs.

## Proposed design

### 1. Interface `PackStreamStructure`

Remove the `getSignature()` and `getFieldCount()` declarations and their Javadoc.
The interface becomes a single-method contract:

```java
public interface PackStreamStructure {
  /** Write this structure to a PackStream writer. */
  void writeTo(PackStreamWriter writer) throws IOException;
}
```

### 2. Implementers

- **`BoltPointStructure`** (only class using the idiom): inline the header write in
  `writeTo()`. The signature (`z == null ? SIGNATURE_2D : SIGNATURE_3D`) and field
  count (`z == null ? 3 : 4`) are computed locally at the `writeStructureHeader`
  call. Remove the two `@Override` accessors. `SIGNATURE_2D` / `SIGNATURE_3D`
  constants and the value getters (`getSrid`/`getX`/`getY`/`getZ`) stay.
- **`BoltNode`, `BoltRelationship`, `BoltUnboundRelationship`,
  `BoltDateTimeStructure`, `BoltPath`, `BoltTemporalStructure`**: delete the two
  now-unused override methods. Their `writeTo()` bodies already write the header
  with inline literals and are unchanged. Remove the stale "writeTo is
  authoritative" comment fragments on the deleted accessors. `SIGNATURE` constants
  and value getters stay.

No `writeTo()` body logic changes for any class - this is dead-accessor removal,
not a re-encode.

### 3. Tests

Convert accessor assertions to serialize-then-assert-on-wire-bytes, which is
strictly stronger (it pins the actual emitted header, the thing that matters):

- **`BoltStructureTest`** - `boltNodeCreation` / `boltRelationshipCreation` /
  `boltUnboundRelationshipCreation` / `boltPathCreation` assert
  `getSignature()`/`getFieldCount()` as a construction smoke-check. Replace each
  with a `writeTo` + assert on the struct-marker byte (`0xB0 | fieldCount`) and the
  following signature byte. The existing `...WriteTo` tests already serialize, so
  this extends an established pattern.
- **`BoltTypeRoundTripTest`** - TYPE-011 already round-trips `BoltTemporalStructure`
  through the wire and asserts `wire.getSignature()` on a `StructureValue` (safe -
  stays). Drop the redundant pre-wire `s.getSignature()` / `s.getFieldCount()`.
  TYPE-012 Point: replace `p.getSignature()` with a wire-byte assertion (or the
  already-present `getZ()` null / non-null check that distinguishes 2D from 3D).
- **`Bolt4998PathMappingTest`** - one Point `getSignature()` assertion → wire-byte
  or `getZ()` check.

## Acceptance criteria

- [ ] `PackStreamStructure` no longer exposes `getSignature()` / `getFieldCount()`;
      `writeTo()` is the only header contract.
- [ ] No `@Override getSignature()` / `getFieldCount()` remains on any of the seven
      implementers.
- [ ] No behavioral change on the wire for any negotiated Bolt version (3.0-5.4),
      guarded by the existing `bolt` unit tests + real-driver ITs.
- [ ] All three affected test files (`BoltStructureTest`, `BoltTypeRoundTripTest`,
      `Bolt4998PathMappingTest`) compile and pass with wire-byte / value-getter
      assertions replacing the removed accessor assertions.
- [ ] `mvn -pl bolt verify` green.

## Testing / verification

`mvn -pl bolt verify` runs the `bolt` unit tests plus the real-driver integration
tests (`RemoteBoltDatabaseIT`, `BoltProtocolIT`, and the Bolt 5.x negotiation ITs)
that exercise the actual `neo4j-java-driver` against a live server across
negotiated versions 3.0 through 5.4. Green there is the guard that proves no wire
drift. Because no `writeTo()` body changes, the refactor is behavior-preserving by
construction; compile-time safety catches any missed interface caller.

## Risk

Essentially nil. The removed methods have exactly one main-source consumer
(`BoltPointStructure.writeTo`), which is rewritten in the same change. Any missed
caller is a compile error, not a runtime surprise. The ITs catch any accidental
wire divergence.

## Non-goals

- No change to `writeTo()` encoding for any version.
- No change to inbound decode (`PackStreamReader.StructureValue`,
  `BoltStructureMapper.fromInboundStructure`, `BoltMessage.parse`).
- No change to `BoltMessage`'s own `getSignature()`.
- No change to the advertised server identity or negotiated version set.

## Effort

**S** (per the issue).
