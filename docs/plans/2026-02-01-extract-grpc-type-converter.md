# Extract GrpcTypeConverter Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract type conversion logic from `ArcadeDbGrpcService` into a dedicated `GrpcTypeConverter` class to improve maintainability.

**Architecture:** Create a new utility class with static methods for converting between gRPC protobuf types and Java/ArcadeDB types. The converter will be stateless and independently testable. Move ~600 lines of conversion logic out of the 3,120-line service class.

**Tech Stack:** Java 21, gRPC, Protobuf, ArcadeDB types, JUnit 5, AssertJ, Mockito

---

## Methods to Extract

| Method | Lines | Purpose |
|--------|-------|---------|
| `fromGrpcValue()` | 1931-1983 | GrpcValue → Java Object |
| `toGrpcValue()` (2 overloads) | 1991-2271 | Java Object → GrpcValue |
| `convertParameters()` | 2450-2460 | Map<String, GrpcValue> → Map<String, Object> |
| `convertResultToGrpcRecord()` | 2472-2536 | Result → GrpcRecord |
| `convertToGrpcRecord()` | 2538-2588 | Record → GrpcRecord |
| `convertPropToGrpcValue()` (2 overloads) | 2590-2610 | Property → GrpcValue |
| `toJavaForProperty()` | 2612-2630 | GrpcValue → typed Java for schema |
| `convertWithSchemaType()` | 2632-2939 | Schema-aware conversion |
| `gsonToGrpc()` | 3017-3085 | JsonElement → GrpcValue |
| `tsToMillis()` | 1927-1929 | Timestamp → millis |
| `msToTimestamp()` | 1985-1989 | millis → Timestamp |
| `bytesOf()` | 2273-2275 | String → byte length |
| `summarizeJava()` | 2946-2962 | Debug helper |
| `summarizeGrpc()` | 2964-3000 | Debug helper |
| `dbgEnc()` / `dbgDec()` | 3002-3014 | Debug wrappers |
| `ProjectionConfig` (inner class) | 3087-3119 | Projection settings |

---

### Task 1: Create GrpcTypeConverter with Timestamp Helpers

**Files:**
- Create: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java`
- Create: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java`

**Step 1: Write the failing test**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.grpc;

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcTypeConverterTest {

  @Test
  void tsToMillisConvertsCorrectly() {
    Timestamp ts = Timestamp.newBuilder()
        .setSeconds(1000)
        .setNanos(500_000_000)
        .build();

    long millis = GrpcTypeConverter.tsToMillis(ts);

    assertThat(millis).isEqualTo(1000_500L);
  }

  @Test
  void msToTimestampConvertsCorrectly() {
    long millis = 1000_500L;

    Timestamp ts = GrpcTypeConverter.msToTimestamp(millis);

    assertThat(ts.getSeconds()).isEqualTo(1000L);
    assertThat(ts.getNanos()).isEqualTo(500_000_000);
  }

  @Test
  void timestampRoundTrip() {
    long original = System.currentTimeMillis();

    Timestamp ts = GrpcTypeConverter.msToTimestamp(original);
    long result = GrpcTypeConverter.tsToMillis(ts);

    assertThat(result).isEqualTo(original);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: FAIL with "cannot find symbol: class GrpcTypeConverter"

**Step 3: Write minimal implementation**

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.grpc;

import com.google.protobuf.Timestamp;

/**
 * Utility class for converting between gRPC protobuf types and Java/ArcadeDB types.
 * This class is stateless and all methods are static.
 */
class GrpcTypeConverter {

  private GrpcTypeConverter() {
    // Utility class - prevent instantiation
  }

  /**
   * Convert a protobuf Timestamp to milliseconds since epoch.
   */
  static long tsToMillis(Timestamp ts) {
    return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
  }

  /**
   * Convert milliseconds since epoch to a protobuf Timestamp.
   */
  static Timestamp msToTimestamp(long ms) {
    long seconds = Math.floorDiv(ms, 1000L);
    int nanos = (int) Math.floorMod(ms, 1000L) * 1_000_000;
    return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
  }
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: Tests run: 3, Failures: 0, Errors: 0

**Step 5: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java \
        grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java
git commit -m "refactor(grpcw): create GrpcTypeConverter with timestamp helpers"
```

---

### Task 2: Add fromGrpcValue - Primitive Types

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java`
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java`

**Step 1: Add tests for primitive conversions**

Add to `GrpcTypeConverterTest.java`:

```java
  @Test
  void fromGrpcValueNull() {
    assertThat(GrpcTypeConverter.fromGrpcValue(null)).isNull();
  }

  @Test
  void fromGrpcValueBoolean() {
    GrpcValue v = GrpcValue.newBuilder().setBoolValue(true).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(true);
  }

  @Test
  void fromGrpcValueInt32() {
    GrpcValue v = GrpcValue.newBuilder().setInt32Value(42).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(42);
  }

  @Test
  void fromGrpcValueInt64() {
    GrpcValue v = GrpcValue.newBuilder().setInt64Value(123456789012L).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(123456789012L);
  }

  @Test
  void fromGrpcValueFloat() {
    GrpcValue v = GrpcValue.newBuilder().setFloatValue(3.14f).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(3.14f);
  }

  @Test
  void fromGrpcValueDouble() {
    GrpcValue v = GrpcValue.newBuilder().setDoubleValue(3.14159).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(3.14159);
  }

  @Test
  void fromGrpcValueString() {
    GrpcValue v = GrpcValue.newBuilder().setStringValue("hello").build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo("hello");
  }

  @Test
  void fromGrpcValueBytes() {
    byte[] data = {1, 2, 3, 4};
    GrpcValue v = GrpcValue.newBuilder().setBytesValue(ByteString.copyFrom(data)).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isEqualTo(data);
  }

  @Test
  void fromGrpcValueTimestamp() {
    Timestamp ts = Timestamp.newBuilder().setSeconds(1000).setNanos(0).build();
    GrpcValue v = GrpcValue.newBuilder().setTimestampValue(ts).build();
    Object result = GrpcTypeConverter.fromGrpcValue(v);
    assertThat(result).isInstanceOf(Date.class);
    assertThat(((Date) result).getTime()).isEqualTo(1000_000L);
  }

  @Test
  void fromGrpcValueKindNotSet() {
    GrpcValue v = GrpcValue.newBuilder().build();
    assertThat(GrpcTypeConverter.fromGrpcValue(v)).isNull();
  }
```

Add import at top:
```java
import com.google.protobuf.ByteString;
import java.util.Date;
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: FAIL with "cannot find symbol: method fromGrpcValue"

**Step 3: Implement fromGrpcValue for primitives**

Add to `GrpcTypeConverter.java`:

```java
import com.arcadedb.database.RID;
import com.google.protobuf.ByteString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
```

And add method:

```java
  /**
   * Convert a GrpcValue to a Java Object.
   */
  static Object fromGrpcValue(GrpcValue v) {
    if (v == null)
      return null;

    switch (v.getKindCase()) {
    case BOOL_VALUE:
      return v.getBoolValue();
    case INT32_VALUE:
      return v.getInt32Value();
    case INT64_VALUE:
      return v.getInt64Value();
    case FLOAT_VALUE:
      return v.getFloatValue();
    case DOUBLE_VALUE:
      return v.getDoubleValue();
    case STRING_VALUE:
      return v.getStringValue();
    case BYTES_VALUE:
      return v.getBytesValue().toByteArray();
    case TIMESTAMP_VALUE:
      return new Date(tsToMillis(v.getTimestampValue()));
    case LINK_VALUE:
      return new RID(v.getLinkValue().getRid());
    case DECIMAL_VALUE: {
      var d = v.getDecimalValue();
      return new BigDecimal(BigInteger.valueOf(d.getUnscaled()), d.getScale());
    }
    case LIST_VALUE: {
      var out = new ArrayList<>();
      for (GrpcValue e : v.getListValue().getValuesList())
        out.add(fromGrpcValue(e));
      return out;
    }
    case MAP_VALUE: {
      var out = new LinkedHashMap<String, Object>();
      v.getMapValue().getEntriesMap().forEach((k, vv) -> out.put(k, fromGrpcValue(vv)));
      return out;
    }
    case EMBEDDED_VALUE: {
      var out = new LinkedHashMap<String, Object>();
      v.getEmbeddedValue().getFieldsMap().forEach((k, vv) -> out.put(k, fromGrpcValue(vv)));
      return out;
    }
    case KIND_NOT_SET:
      return null;
    }
    return null;
  }
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: Tests run: 13, Failures: 0, Errors: 0

**Step 5: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java \
        grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java
git commit -m "refactor(grpcw): add fromGrpcValue to GrpcTypeConverter"
```

---

### Task 3: Add fromGrpcValue - Complex Types (List, Map, Embedded, Decimal, Link)

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java`
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java`

**Step 1: Add tests for complex types**

Add to `GrpcTypeConverterTest.java`:

```java
import com.arcadedb.database.RID;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

  @Test
  void fromGrpcValueDecimal() {
    GrpcDecimal decimal = GrpcDecimal.newBuilder()
        .setUnscaled(12345)
        .setScale(2)
        .build();
    GrpcValue v = GrpcValue.newBuilder().setDecimalValue(decimal).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(BigDecimal.class);
    assertThat(((BigDecimal) result).toString()).isEqualTo("123.45");
  }

  @Test
  void fromGrpcValueLink() {
    GrpcLink link = GrpcLink.newBuilder().setRid("#1:0").build();
    GrpcValue v = GrpcValue.newBuilder().setLinkValue(link).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(RID.class);
    assertThat(result.toString()).isEqualTo("#1:0");
  }

  @Test
  void fromGrpcValueList() {
    GrpcList list = GrpcList.newBuilder()
        .addValues(GrpcValue.newBuilder().setInt32Value(1).build())
        .addValues(GrpcValue.newBuilder().setInt32Value(2).build())
        .addValues(GrpcValue.newBuilder().setInt32Value(3).build())
        .build();
    GrpcValue v = GrpcValue.newBuilder().setListValue(list).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(List.class);
    assertThat((List<?>) result).containsExactly(1, 2, 3);
  }

  @Test
  void fromGrpcValueMap() {
    GrpcMap map = GrpcMap.newBuilder()
        .putEntries("name", GrpcValue.newBuilder().setStringValue("John").build())
        .putEntries("age", GrpcValue.newBuilder().setInt32Value(30).build())
        .build();
    GrpcValue v = GrpcValue.newBuilder().setMapValue(map).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> mapResult = (Map<String, Object>) result;
    assertThat(mapResult).containsEntry("name", "John");
    assertThat(mapResult).containsEntry("age", 30);
  }

  @Test
  void fromGrpcValueEmbedded() {
    GrpcEmbedded embedded = GrpcEmbedded.newBuilder()
        .setType("Address")
        .putFields("city", GrpcValue.newBuilder().setStringValue("Rome").build())
        .build();
    GrpcValue v = GrpcValue.newBuilder().setEmbeddedValue(embedded).build();

    Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> mapResult = (Map<String, Object>) result;
    assertThat(mapResult).containsEntry("city", "Rome");
  }
```

**Step 2: Run tests to verify they pass (already implemented)**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: Tests run: 18, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java
git commit -m "test(grpcw): add tests for complex type conversions in GrpcTypeConverter"
```

---

### Task 4: Add toGrpcValue - Primitive Types

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java`
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java`

**Step 1: Add tests for toGrpcValue primitives**

Add to `GrpcTypeConverterTest.java`:

```java
  @Test
  void toGrpcValueNull() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(null);
    assertThat(result.getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
  }

  @Test
  void toGrpcValueBoolean() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(true);
    assertThat(result.getBoolValue()).isTrue();
  }

  @Test
  void toGrpcValueInteger() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(42);
    assertThat(result.getInt32Value()).isEqualTo(42);
  }

  @Test
  void toGrpcValueLong() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(123456789012L);
    assertThat(result.getInt64Value()).isEqualTo(123456789012L);
  }

  @Test
  void toGrpcValueFloat() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(3.14f);
    assertThat(result.getFloatValue()).isEqualTo(3.14f);
  }

  @Test
  void toGrpcValueDouble() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue(3.14159);
    assertThat(result.getDoubleValue()).isEqualTo(3.14159);
  }

  @Test
  void toGrpcValueString() {
    GrpcValue result = GrpcTypeConverter.toGrpcValue("hello");
    assertThat(result.getStringValue()).isEqualTo("hello");
  }

  @Test
  void toGrpcValueBytes() {
    byte[] data = {1, 2, 3, 4};
    GrpcValue result = GrpcTypeConverter.toGrpcValue(data);
    assertThat(result.getBytesValue().toByteArray()).isEqualTo(data);
  }

  @Test
  void toGrpcValueDate() {
    Date date = new Date(1000_500L);
    GrpcValue result = GrpcTypeConverter.toGrpcValue(date);
    assertThat(result.hasTimestampValue()).isTrue();
    assertThat(tsToMillis(result.getTimestampValue())).isEqualTo(1000_500L);
  }

  private static long tsToMillis(Timestamp ts) {
    return GrpcTypeConverter.tsToMillis(ts);
  }
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: FAIL with "cannot find symbol: method toGrpcValue"

**Step 3: Implement toGrpcValue for primitives**

Add to `GrpcTypeConverter.java`:

```java
  /**
   * Convert a Java Object to a GrpcValue.
   */
  static GrpcValue toGrpcValue(Object o) {
    GrpcValue.Builder b = GrpcValue.newBuilder();

    if (o == null)
      return b.build();

    if (o instanceof Boolean v)
      return b.setBoolValue(v).build();
    if (o instanceof Integer v)
      return b.setInt32Value(v).build();
    if (o instanceof Long v)
      return b.setInt64Value(v).build();
    if (o instanceof Float v)
      return b.setFloatValue(v).build();
    if (o instanceof Double v)
      return b.setDoubleValue(v).build();
    if (o instanceof CharSequence v)
      return b.setStringValue(v.toString()).build();
    if (o instanceof byte[] v)
      return b.setBytesValue(ByteString.copyFrom(v)).build();

    if (o instanceof Date v)
      return b.setTimestampValue(msToTimestamp(v.getTime())).setLogicalType("datetime").build();

    if (o instanceof RID rid)
      return b.setLinkValue(GrpcLink.newBuilder().setRid(rid.toString()).build()).setLogicalType("rid").build();

    if (o instanceof BigDecimal v) {
      var unscaled = v.unscaledValue();
      if (unscaled.bitLength() <= 63) {
        return b.setDecimalValue(
            GrpcDecimal.newBuilder().setUnscaled(unscaled.longValue()).setScale(v.scale()))
            .setLogicalType("decimal").build();
      } else {
        return b.setStringValue(v.toPlainString()).setLogicalType("decimal").build();
      }
    }

    // Collections
    if (o instanceof List<?> list) {
      GrpcList.Builder lb = GrpcList.newBuilder();
      for (Object item : list)
        lb.addValues(toGrpcValue(item));
      return b.setListValue(lb.build()).build();
    }

    if (o instanceof Map<?, ?> map) {
      GrpcMap.Builder mb = GrpcMap.newBuilder();
      map.forEach((k, v) -> mb.putEntries(String.valueOf(k), toGrpcValue(v)));
      return b.setMapValue(mb.build()).build();
    }

    // Fallback: convert to string
    return b.setStringValue(String.valueOf(o)).build();
  }
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: Tests run: 27, Failures: 0, Errors: 0

**Step 5: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java \
        grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java
git commit -m "refactor(grpcw): add toGrpcValue to GrpcTypeConverter"
```

---

### Task 5: Add toGrpcValue - Complex Types (RID, BigDecimal, List, Map)

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java`

**Step 1: Add tests for complex toGrpcValue types**

Add to `GrpcTypeConverterTest.java`:

```java
  @Test
  void toGrpcValueRID() {
    RID rid = new RID("#1:0");
    GrpcValue result = GrpcTypeConverter.toGrpcValue(rid);
    assertThat(result.hasLinkValue()).isTrue();
    assertThat(result.getLinkValue().getRid()).isEqualTo("#1:0");
  }

  @Test
  void toGrpcValueBigDecimal() {
    BigDecimal decimal = new BigDecimal("123.45");
    GrpcValue result = GrpcTypeConverter.toGrpcValue(decimal);
    assertThat(result.hasDecimalValue()).isTrue();
    assertThat(result.getDecimalValue().getUnscaled()).isEqualTo(12345);
    assertThat(result.getDecimalValue().getScale()).isEqualTo(2);
  }

  @Test
  void toGrpcValueList() {
    List<Integer> list = List.of(1, 2, 3);
    GrpcValue result = GrpcTypeConverter.toGrpcValue(list);
    assertThat(result.hasListValue()).isTrue();
    assertThat(result.getListValue().getValuesCount()).isEqualTo(3);
  }

  @Test
  void toGrpcValueMap() {
    Map<String, Object> map = Map.of("name", "John", "age", 30);
    GrpcValue result = GrpcTypeConverter.toGrpcValue(map);
    assertThat(result.hasMapValue()).isTrue();
    assertThat(result.getMapValue().getEntriesCount()).isEqualTo(2);
  }

  @Test
  void roundTripPrimitives() {
    // Test that fromGrpcValue(toGrpcValue(x)) == x for various types
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(true))).isEqualTo(true);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(42))).isEqualTo(42);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(3.14))).isEqualTo(3.14);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue("hello"))).isEqualTo("hello");
  }
```

**Step 2: Run tests to verify they pass (already implemented)**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: Tests run: 32, Failures: 0, Errors: 0

**Step 3: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java
git commit -m "test(grpcw): add round-trip and complex type tests for GrpcTypeConverter"
```

---

### Task 6: Add convertParameters Helper

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java`
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java`

**Step 1: Add test for convertParameters**

Add to `GrpcTypeConverterTest.java`:

```java
import java.util.HashMap;

  @Test
  void convertParametersEmpty() {
    Map<String, GrpcValue> params = Map.of();
    Map<String, Object> result = GrpcTypeConverter.convertParameters(params);
    assertThat(result).isEmpty();
  }

  @Test
  void convertParametersWithValues() {
    Map<String, GrpcValue> params = new HashMap<>();
    params.put("name", GrpcValue.newBuilder().setStringValue("John").build());
    params.put("age", GrpcValue.newBuilder().setInt32Value(30).build());
    params.put("active", GrpcValue.newBuilder().setBoolValue(true).build());

    Map<String, Object> result = GrpcTypeConverter.convertParameters(params);

    assertThat(result).hasSize(3);
    assertThat(result).containsEntry("name", "John");
    assertThat(result).containsEntry("age", 30);
    assertThat(result).containsEntry("active", true);
  }
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: FAIL with "cannot find symbol: method convertParameters"

**Step 3: Implement convertParameters**

Add to `GrpcTypeConverter.java`:

```java
import java.util.HashMap;
import java.util.Map;

  /**
   * Convert a map of GrpcValue parameters to Java objects.
   */
  static Map<String, Object> convertParameters(Map<String, GrpcValue> protoParams) {
    Map<String, Object> params = new HashMap<>();
    for (Map.Entry<String, GrpcValue> entry : protoParams.entrySet()) {
      params.put(entry.getKey(), fromGrpcValue(entry.getValue()));
    }
    return params;
  }
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: Tests run: 34, Failures: 0, Errors: 0

**Step 5: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java \
        grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java
git commit -m "refactor(grpcw): add convertParameters to GrpcTypeConverter"
```

---

### Task 7: Add bytesOf Helper

**Files:**
- Modify: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java`
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java`

**Step 1: Add tests for bytesOf**

Add to `GrpcTypeConverterTest.java`:

```java
  @Test
  void bytesOfNull() {
    assertThat(GrpcTypeConverter.bytesOf(null)).isEqualTo(0);
  }

  @Test
  void bytesOfEmpty() {
    assertThat(GrpcTypeConverter.bytesOf("")).isEqualTo(0);
  }

  @Test
  void bytesOfAscii() {
    assertThat(GrpcTypeConverter.bytesOf("hello")).isEqualTo(5);
  }

  @Test
  void bytesOfUtf8() {
    // UTF-8 multibyte characters
    assertThat(GrpcTypeConverter.bytesOf("日本")).isEqualTo(6); // 3 bytes each
  }
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: FAIL with "cannot find symbol: method bytesOf"

**Step 3: Implement bytesOf**

Add to `GrpcTypeConverter.java`:

```java
import java.nio.charset.StandardCharsets;

  /**
   * Calculate the UTF-8 byte length of a string.
   */
  static int bytesOf(String s) {
    return s == null ? 0 : s.getBytes(StandardCharsets.UTF_8).length;
  }
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn test -pl grpcw -Dtest=GrpcTypeConverterTest -q`
Expected: Tests run: 38, Failures: 0, Errors: 0

**Step 5: Commit**

```bash
git add grpcw/src/test/java/com/arcadedb/server/grpc/GrpcTypeConverterTest.java \
        grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java
git commit -m "refactor(grpcw): add bytesOf to GrpcTypeConverter"
```

---

### Task 8: Wire GrpcTypeConverter into ArcadeDbGrpcService

**Files:**
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`

**Step 1: Replace calls to local methods with GrpcTypeConverter**

In `ArcadeDbGrpcService.java`, change usages:

```java
// Before:
Object javaVal = fromGrpcValue(grpcVal);
Map<String, Object> params = convertParameters(req.getParametersMap());

// After:
Object javaVal = GrpcTypeConverter.fromGrpcValue(grpcVal);
Map<String, Object> params = GrpcTypeConverter.convertParameters(req.getParametersMap());
```

Key locations to update (search and replace):
- Line ~250: `convertParameters(req.getParametersMap())`
- Line ~440: `toJavaForProperty` calls that use `fromGrpcValue`
- Line ~1921: `fromGrpcValue` calls

**Step 2: Run existing integration tests to verify no regression**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn verify -pl grpcw -DskipITs=false 2>&1 | tail -30`
Expected: BUILD SUCCESS with all tests passing

**Step 3: Commit**

```bash
git add grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java
git commit -m "refactor(grpcw): wire GrpcTypeConverter into ArcadeDbGrpcService"
```

---

### Task 9: Remove Duplicate Methods from ArcadeDbGrpcService

**Files:**
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`

**Step 1: Delete the now-unused private methods**

Remove these methods from `ArcadeDbGrpcService.java`:
- `private Object fromGrpcValue(GrpcValue v)` (lines 1931-1983)
- `private static Timestamp msToTimestamp(long ms)` (lines 1985-1989)
- `private static long tsToMillis(Timestamp ts)` (lines 1927-1929)
- `private Map<String, Object> convertParameters(...)` (lines 2450-2460)
- `private static int bytesOf(String s)` (lines 2273-2275)

**Step 2: Verify compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn compile -pl grpcw -q`
Expected: BUILD SUCCESS

**Step 3: Run all tests to verify no regression**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn verify -pl grpcw -DskipITs=false 2>&1 | tail -30`
Expected: BUILD SUCCESS with all tests passing

**Step 4: Commit**

```bash
git add grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java
git commit -m "refactor(grpcw): remove duplicate conversion methods from ArcadeDbGrpcService"
```

---

### Task 10: Run Full Test Suite and Verify Line Count Reduction

**Step 1: Run all grpcw tests**

Run: `cd /Users/frank/projects/arcade/worktrees/add-tests && mvn verify -pl grpcw -DskipITs=false`
Expected: BUILD SUCCESS

**Step 2: Verify line count reduction**

Run: `wc -l grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java grpcw/src/main/java/com/arcadedb/server/grpc/GrpcTypeConverter.java`

Expected:
- `ArcadeDbGrpcService.java`: ~2,900 lines (was 3,120)
- `GrpcTypeConverter.java`: ~200 lines (new)
- Net reduction: ~20 lines (moved to cleaner, testable location)

**Step 3: Verify test counts**

Run: `mvn verify -pl grpcw -DskipITs=false 2>&1 | grep -E "Tests run:"`

Expected:
- Unit tests: 30 + ~38 new = ~68
- Integration tests: 31
- Total: ~99 tests

---

## Summary

After completing all tasks:

| Metric | Before | After |
|--------|--------|-------|
| ArcadeDbGrpcService.java lines | 3,120 | ~2,900 |
| GrpcTypeConverter.java lines | 0 | ~200 |
| GrpcTypeConverterTest.java tests | 0 | ~38 |
| Total grpcw tests | 61 | ~99 |

## Success Criteria

- All existing tests pass
- New GrpcTypeConverter has comprehensive unit tests
- No functional changes to gRPC API behavior
- Code is cleaner and more maintainable
- Conversion logic is independently testable
