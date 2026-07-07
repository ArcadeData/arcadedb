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
package com.arcadedb.bolt;

import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltNode;
import com.arcadedb.bolt.structure.BoltPath;
import com.arcadedb.bolt.structure.BoltRelationship;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import com.arcadedb.bolt.structure.BoltTemporalStructure;
import com.arcadedb.bolt.structure.BoltUnboundRelationship;
import com.arcadedb.database.RID;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for BOLT structure classes.
 */
class BoltStructureTest {

  // ============ BoltNode tests ============

  @Test
  void boltNodeCreation() {
    final BoltNode node = new BoltNode(123L, List.of("Person", "Employee"), Map.of("name", "Alice"), "#1:0");

    assertThat(node.getSignature()).isEqualTo(BoltNode.SIGNATURE);
    assertThat(node.getFieldCount()).isEqualTo(3);
    assertThat(node.getId()).isEqualTo(123L);
    assertThat(node.getLabels()).containsExactly("Person", "Employee");
    assertThat(node.getProperties()).containsEntry("name", "Alice");
    assertThat(node.getElementId()).isEqualTo("#1:0");
  }

  @Test
  void boltNodeNullDefaults() {
    final BoltNode node = new BoltNode(1L, null, null, null);

    assertThat(node.getLabels()).isEmpty();
    assertThat(node.getProperties()).isEmpty();
    assertThat(node.getElementId()).isEqualTo("1");
  }

  @Test
  void boltNodeWriteTo() throws Exception {
    final BoltNode node = new BoltNode(1L, List.of("Label"), Map.of("key", "value"), "#1:0");
    final PackStreamWriter writer = new PackStreamWriter();
    node.writeTo(writer);

    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void boltNodeToString() {
    final BoltNode node = new BoltNode(1L, List.of("Person"), Map.of("name", "Test"), "#1:0");
    final String str = node.toString();

    assertThat(str).contains("Node");
    assertThat(str).contains("id=1");
    assertThat(str).contains("Person");
    assertThat(str).contains("name");
  }

  // ============ BoltRelationship tests ============

  @Test
  void boltRelationshipCreation() {
    final BoltRelationship rel = new BoltRelationship(
        100L, 1L, 2L, "KNOWS", Map.of("since", 2020), "#10:0", "#1:0", "#2:0"
    );

    assertThat(rel.getSignature()).isEqualTo(BoltRelationship.SIGNATURE);
    assertThat(rel.getFieldCount()).isEqualTo(5);
    assertThat(rel.getId()).isEqualTo(100L);
    assertThat(rel.getStartNodeId()).isEqualTo(1L);
    assertThat(rel.getEndNodeId()).isEqualTo(2L);
    assertThat(rel.getType()).isEqualTo("KNOWS");
    assertThat(rel.getProperties()).containsEntry("since", 2020);
    assertThat(rel.getElementId()).isEqualTo("#10:0");
    assertThat(rel.getStartNodeElementId()).isEqualTo("#1:0");
    assertThat(rel.getEndNodeElementId()).isEqualTo("#2:0");
  }

  @Test
  void boltRelationshipNullDefaults() {
    final BoltRelationship rel = new BoltRelationship(1L, 2L, 3L, "TYPE", null, null, null, null);

    assertThat(rel.getProperties()).isEmpty();
    assertThat(rel.getElementId()).isEqualTo("1");
    assertThat(rel.getStartNodeElementId()).isEqualTo("2");
    assertThat(rel.getEndNodeElementId()).isEqualTo("3");
  }

  @Test
  void boltRelationshipWriteTo() throws Exception {
    final BoltRelationship rel = new BoltRelationship(1L, 2L, 3L, "REL", Map.of(), "#1:0", "#2:0", "#3:0");
    final PackStreamWriter writer = new PackStreamWriter();
    rel.writeTo(writer);

    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void boltRelationshipToString() {
    final BoltRelationship rel = new BoltRelationship(1L, 2L, 3L, "KNOWS", Map.of(), "#1:0", "#2:0", "#3:0");
    final String str = rel.toString();

    assertThat(str).contains("Relationship");
    assertThat(str).contains("KNOWS");
    assertThat(str).contains("startNodeId=2");
    assertThat(str).contains("endNodeId=3");
  }

  // ============ BoltUnboundRelationship tests ============

  @Test
  void boltUnboundRelationshipCreation() {
    final BoltUnboundRelationship rel = new BoltUnboundRelationship(1L, "FRIEND", Map.of("weight", 0.5), "#1:0");

    assertThat(rel.getSignature()).isEqualTo(BoltUnboundRelationship.SIGNATURE);
    assertThat(rel.getFieldCount()).isEqualTo(3);
    assertThat(rel.getId()).isEqualTo(1L);
    assertThat(rel.getType()).isEqualTo("FRIEND");
    assertThat(rel.getProperties()).containsEntry("weight", 0.5);
    assertThat(rel.getElementId()).isEqualTo("#1:0");
  }

  @Test
  void boltUnboundRelationshipNullDefaults() {
    final BoltUnboundRelationship rel = new BoltUnboundRelationship(42L, "TYPE", null, null);

    assertThat(rel.getProperties()).isEmpty();
    assertThat(rel.getElementId()).isEqualTo("42");
  }

  @Test
  void boltUnboundRelationshipWriteTo() throws Exception {
    final BoltUnboundRelationship rel = new BoltUnboundRelationship(1L, "REL", Map.of(), "#1:0");
    final PackStreamWriter writer = new PackStreamWriter();
    rel.writeTo(writer);

    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void boltUnboundRelationshipToString() {
    final BoltUnboundRelationship rel = new BoltUnboundRelationship(1L, "LIKES", Map.of(), "#1:0");
    final String str = rel.toString();

    assertThat(str).contains("UnboundRelationship");
    assertThat(str).contains("LIKES");
    assertThat(str).contains("id=1");
  }

  // ============ BoltPath tests ============

  @Test
  void boltPathCreation() {
    final List<BoltNode> nodes = List.of(
        new BoltNode(1L, List.of("A"), Map.of(), "#1:0"),
        new BoltNode(2L, List.of("B"), Map.of(), "#2:0")
    );
    final List<BoltUnboundRelationship> rels = List.of(
        new BoltUnboundRelationship(10L, "CONNECTS", Map.of(), "#10:0")
    );
    final List<Long> indices = List.of(1L, 1L);

    final BoltPath path = new BoltPath(nodes, rels, indices);

    assertThat(path.getSignature()).isEqualTo(BoltPath.SIGNATURE);
    assertThat(path.getFieldCount()).isEqualTo(3);
    assertThat(path.getNodes()).hasSize(2);
    assertThat(path.getRelationships()).hasSize(1);
    assertThat(path.getIndices()).containsExactly(1L, 1L);
  }

  @Test
  void boltPathNullDefaults() {
    final BoltPath path = new BoltPath(null, null, null);

    assertThat(path.getNodes()).isEmpty();
    assertThat(path.getRelationships()).isEmpty();
    assertThat(path.getIndices()).isEmpty();
  }

  @Test
  void boltPathWriteTo() throws Exception {
    final List<BoltNode> nodes = List.of(new BoltNode(1L, List.of("A"), Map.of(), "#1:0"));
    final List<BoltUnboundRelationship> rels = List.of();
    final List<Long> indices = List.of();

    final BoltPath path = new BoltPath(nodes, rels, indices);
    final PackStreamWriter writer = new PackStreamWriter();
    path.writeTo(writer);

    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void boltPathWriteToWithRelationships() throws Exception {
    final List<BoltNode> nodes = List.of(
        new BoltNode(1L, List.of("A"), Map.of(), "#1:0"),
        new BoltNode(2L, List.of("B"), Map.of(), "#2:0")
    );
    final List<BoltUnboundRelationship> rels = List.of(
        new BoltUnboundRelationship(10L, "CONNECTS", Map.of("w", 1.0), "#10:0")
    );
    final List<Long> indices = List.of(1L, 1L);

    final BoltPath path = new BoltPath(nodes, rels, indices);
    final PackStreamWriter writer = new PackStreamWriter();
    path.writeTo(writer);

    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void boltPathToString() {
    final List<BoltNode> nodes = List.of(
        new BoltNode(1L, List.of("A"), Map.of(), "#1:0"),
        new BoltNode(2L, List.of("B"), Map.of(), "#2:0")
    );
    final List<BoltUnboundRelationship> rels = List.of(
        new BoltUnboundRelationship(10L, "REL", Map.of(), "#10:0")
    );
    final BoltPath path = new BoltPath(nodes, rels, List.of(1L, 1L));
    final String str = path.toString();

    assertThat(str).contains("Path");
    assertThat(str).contains("nodes=2");
    assertThat(str).contains("relationships=1");
  }

  @Test
  void nodeWritesElementIdOnBolt5() throws Exception {
    final BoltNode node = new BoltNode(1L, List.of("Person"), Map.of("name", "Alice"), "#1:0");

    final PackStreamWriter v4 = new PackStreamWriter();
    node.writeTo(v4);
    // Tiny-struct marker: 0xB0 | fieldCount. v4 => 3 fields, signature 0x4E.
    assertThat(v4.toByteArray()[0]).isEqualTo((byte) (0xB0 | 3));
    assertThat(v4.toByteArray()[1]).isEqualTo(BoltNode.SIGNATURE);

    final PackStreamWriter v5 = new PackStreamWriter().boltMajorVersion(5);
    node.writeTo(v5);
    assertThat(v5.toByteArray()[0]).isEqualTo((byte) (0xB0 | 4));
    assertThat(v5.toByteArray()[1]).isEqualTo(BoltNode.SIGNATURE);
    // Last field is the element_id string "#1:0" -> tiny-string 0x84 then ASCII bytes.
    final byte[] b = v5.toByteArray();
    assertThat(b[b.length - 5]).isEqualTo((byte) (0x80 | 4));
    assertThat(new String(b, b.length - 4, 4, java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("#1:0");
  }

  @Test
  void relationshipWritesElementIdsOnBolt5() throws Exception {
    final BoltRelationship rel = new BoltRelationship(10L, 1L, 2L, "KNOWS", Map.of(), "#10:0", "#1:0", "#2:0");

    final PackStreamWriter v4 = new PackStreamWriter();
    rel.writeTo(v4);
    assertThat(v4.toByteArray()[0]).isEqualTo((byte) (0xB0 | 5));

    final PackStreamWriter v5 = new PackStreamWriter().boltMajorVersion(5);
    rel.writeTo(v5);
    assertThat(v5.toByteArray()[0]).isEqualTo((byte) (0xB0 | 8));
    assertThat(v5.toByteArray()[1]).isEqualTo(BoltRelationship.SIGNATURE);
  }

  @Test
  void unboundRelationshipWritesElementIdOnBolt5() throws Exception {
    final BoltUnboundRelationship rel = new BoltUnboundRelationship(10L, "KNOWS", Map.of(), "#10:0");

    final PackStreamWriter v4 = new PackStreamWriter();
    rel.writeTo(v4);
    assertThat(v4.toByteArray()[0]).isEqualTo((byte) (0xB0 | 3));

    final PackStreamWriter v5 = new PackStreamWriter().boltMajorVersion(5);
    rel.writeTo(v5);
    assertThat(v5.toByteArray()[0]).isEqualTo((byte) (0xB0 | 4));
    assertThat(v5.toByteArray()[1]).isEqualTo(BoltUnboundRelationship.SIGNATURE);
  }

  // ============ BoltStructureMapper tests ============

  @Test
  void mapperNullValue() {
    assertThat(BoltStructureMapper.toPackStreamValue(null)).isNull();
  }

  @Test
  void mapperPrimitiveTypes() {
    assertThat(BoltStructureMapper.toPackStreamValue(true)).isEqualTo(true);
    assertThat(BoltStructureMapper.toPackStreamValue(false)).isEqualTo(false);
    assertThat(BoltStructureMapper.toPackStreamValue("hello")).isEqualTo("hello");
  }

  @Test
  void mapperIntegerTypes() {
    assertThat(BoltStructureMapper.toPackStreamValue((byte) 42)).isEqualTo(42L);
    assertThat(BoltStructureMapper.toPackStreamValue((short) 1000)).isEqualTo(1000L);
    assertThat(BoltStructureMapper.toPackStreamValue(100000)).isEqualTo(100000L);
    assertThat(BoltStructureMapper.toPackStreamValue(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void mapperFloatTypes() {
    assertThat((Double) BoltStructureMapper.toPackStreamValue(3.14f)).isCloseTo(3.14, within(0.01));
    assertThat(BoltStructureMapper.toPackStreamValue(3.14159)).isEqualTo(3.14159);
  }

  @Test
  void mapperBigInteger() {
    // BigInteger that fits in long
    final BigInteger smallBigInt = BigInteger.valueOf(1000L);
    assertThat(BoltStructureMapper.toPackStreamValue(smallBigInt)).isEqualTo(1000L);

    // BigInteger that exceeds long (converted to double with precision loss)
    final BigInteger largeBigInt = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN);
    final Object result = BoltStructureMapper.toPackStreamValue(largeBigInt);
    assertThat(result).isInstanceOf(Double.class);
  }

  @Test
  void mapperBigDecimal() {
    final BigDecimal bd = new BigDecimal("123.456");
    final Object result = BoltStructureMapper.toPackStreamValue(bd);
    assertThat((Double) result).isCloseTo(123.456, within(0.001));
  }

  @Test
  void mapperByteArray() {
    final byte[] bytes = { 1, 2, 3, 4, 5 };
    assertThat(BoltStructureMapper.toPackStreamValue(bytes)).isEqualTo(bytes);
  }

  @Test
  void mapperList() {
    final List<Object> list = List.of(1, "two", true);
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) BoltStructureMapper.toPackStreamValue(list);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo(1L);
    assertThat(result.get(1)).isEqualTo("two");
    assertThat(result.get(2)).isEqualTo(true);
  }

  @Test
  void mapperSet() {
    final Set<Object> set = new LinkedHashSet<>(List.of(1, 2, 3));
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) BoltStructureMapper.toPackStreamValue(set);
    assertThat(result).hasSize(3);
  }

  @Test
  void mapperObjectArray() {
    final Object[] array = { "a", "b", "c" };
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) BoltStructureMapper.toPackStreamValue(array);
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void mapperMap() {
    final Map<String, Object> map = Map.of("key", "value", "num", 42);
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) BoltStructureMapper.toPackStreamValue(map);
    assertThat(result).containsEntry("key", "value");
    assertThat(result.get("num")).isEqualTo(42L);
  }

  @Test
  void mapperMapWithNullKey() {
    final Map<Object, Object> map = new HashMap<>();
    map.put(null, "nullValue");
    map.put("normalKey", "normalValue");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) BoltStructureMapper.toPackStreamValue(map);
    assertThat(result).containsEntry("null", "nullValue");
    assertThat(result).containsEntry("normalKey", "normalValue");
  }

  // Temporal values are now emitted as native Bolt PackStream structures (issue #4907), not ISO strings.
  // Each is verified by a full wire round-trip: toPackStreamValue -> write -> read -> decode.

  @Test
  void mapperLocalDate() throws Exception {
    final LocalDate date = LocalDate.of(2024, 1, 15);
    assertThat(BoltStructureMapper.toPackStreamValue(date)).isInstanceOf(BoltTemporalStructure.class);
    assertThat(wireRoundTrip(date)).isEqualTo(date);
  }

  @Test
  void mapperLocalTime() throws Exception {
    final LocalTime time = LocalTime.of(10, 30, 45);
    assertThat(wireRoundTrip(time)).isEqualTo(time);
  }

  @Test
  void mapperLocalDateTime() throws Exception {
    final LocalDateTime dateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
    assertThat(wireRoundTrip(dateTime)).isEqualTo(dateTime);
  }

  @Test
  void mapperOffsetDateTime() throws Exception {
    final OffsetDateTime dateTime = OffsetDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneOffset.ofHours(2));
    assertThat(wireRoundTrip(dateTime)).isEqualTo(dateTime);
  }

  @Test
  void mapperZonedDateTime() throws Exception {
    final ZonedDateTime dateTime = ZonedDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneId.of("Europe/Rome"));
    assertThat(wireRoundTrip(dateTime)).isEqualTo(dateTime);
  }

  @Test
  void mapperOffsetTime() throws Exception {
    final OffsetTime time = OffsetTime.of(10, 30, 45, 0, ZoneOffset.ofHours(2));
    assertThat(wireRoundTrip(time)).isEqualTo(time);
  }

  @Test
  void mapperInstant() throws Exception {
    final Instant instant = Instant.parse("2024-01-15T10:30:45Z");
    // Instant maps to a DateTime struct; assert the instant survives the round-trip.
    assertThat(((OffsetDateTime) wireRoundTrip(instant)).toInstant()).isEqualTo(instant);
  }

  @Test
  void mapperJavaDate() throws Exception {
    final Date date = Date.from(Instant.parse("2024-01-15T10:30:45Z"));
    assertThat(((OffsetDateTime) wireRoundTrip(date)).toInstant()).isEqualTo(date.toInstant());
  }

  @Test
  void mapperSqlDate() throws Exception {
    // java.sql.Date extends java.util.Date but has no time component (toInstant() throws): must map to a Date struct.
    final java.sql.Date sqlDate = java.sql.Date.valueOf(LocalDate.of(2024, 1, 15));
    assertThat(wireRoundTrip(sqlDate)).isEqualTo(sqlDate.toLocalDate());
  }

  @Test
  void mapperSqlTime() throws Exception {
    final java.sql.Time sqlTime = java.sql.Time.valueOf(LocalTime.of(10, 30, 45));
    assertThat(wireRoundTrip(sqlTime)).isEqualTo(sqlTime.toLocalTime());
  }

  @Test
  void mapperCalendar() throws Exception {
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.setTimeInMillis(Instant.parse("2024-01-15T10:30:45Z").toEpochMilli());
    // The calendar's named zone is preserved, so it round-trips as a zoned datetime; assert the instant.
    assertThat(((ZonedDateTime) wireRoundTrip(calendar)).toInstant()).isEqualTo(calendar.toInstant());
  }

  /**
   * Map a value to its outbound PackStream form, serialize it, read it back, and decode - the full
   * outbound+inbound wire path. Returns the decoded java.time value.
   */
  private static Object wireRoundTrip(final Object value) throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeValue(BoltStructureMapper.toPackStreamValue(value));
    return BoltStructureMapper.fromPackStreamValue(new PackStreamReader(writer.toByteArray()).readValue());
  }

  @Test
  void mapperUUID() {
    final UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    assertThat(BoltStructureMapper.toPackStreamValue(uuid)).isEqualTo("123e4567-e89b-12d3-a456-426614174000");
  }

  @Test
  void mapperRID() {
    final RID rid = new RID(1, 100);
    assertThat(BoltStructureMapper.toPackStreamValue(rid)).isEqualTo("#1:100");
  }

  @Test
  void mapperUnknownTypeToString() {
    // Custom object should be converted to string
    final Object custom = new Object() {
      @Override
      public String toString() {
        return "CustomObject";
      }
    };
    assertThat(BoltStructureMapper.toPackStreamValue(custom)).isEqualTo("CustomObject");
  }

  @Test
  void ridToIdBasic() {
    final RID rid = new RID(1, 100);
    final long id = BoltStructureMapper.ridToId(rid);
    // Bucket 1 in high 16 bits, position 100 in low 48 bits
    assertThat(id).isEqualTo((1L << 48) | 100L);
  }

  @Test
  void ridToIdNull() {
    assertThat(BoltStructureMapper.ridToId(null)).isEqualTo(-1L);
  }

  @Test
  void ridToIdMaxValues() {
    // Maximum valid bucket ID (16 bits)
    final RID ridMaxBucket = new RID(0xFFFF, 0);
    assertThat(BoltStructureMapper.ridToId(ridMaxBucket)).isEqualTo(0xFFFF_0000_0000_0000L);

    // Maximum valid position (48 bits)
    final RID ridMaxPosition = new RID(0, 0xFFFF_FFFF_FFFFL);
    assertThat(BoltStructureMapper.ridToId(ridMaxPosition)).isEqualTo(0xFFFF_FFFF_FFFFL);
  }

  @Test
  void ridToIdInvalidBucketNegative() {
    final RID rid = new RID(-1, 100);
    assertThatThrownBy(() -> BoltStructureMapper.ridToId(rid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bucket ID out of range");
  }

  @Test
  void ridToIdInvalidBucketTooLarge() {
    final RID rid = new RID(0x10000, 100); // 65536, exceeds 16-bit limit
    assertThatThrownBy(() -> BoltStructureMapper.ridToId(rid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bucket ID out of range");
  }

  @Test
  void ridToIdInvalidPositionNegative() {
    final RID rid = new RID(1, -1);
    assertThatThrownBy(() -> BoltStructureMapper.ridToId(rid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Position out of range");
  }

  // ============ Nested collection tests ============

  @Test
  void mapperNestedList() {
    final List<Object> nested = List.of(
        List.of(1, 2),
        List.of("a", "b")
    );
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) BoltStructureMapper.toPackStreamValue(nested);
    assertThat(result).hasSize(2);

    @SuppressWarnings("unchecked")
    final List<Object> inner1 = (List<Object>) result.get(0);
    assertThat(inner1).containsExactly(1L, 2L);
  }

  @Test
  void mapperNestedMap() {
    final Map<String, Object> nested = Map.of(
        "inner", Map.of("key", "value"),
        "list", List.of(1, 2, 3)
    );
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) BoltStructureMapper.toPackStreamValue(nested);
    assertThat(result).containsKey("inner");
    assertThat(result).containsKey("list");
  }
}
