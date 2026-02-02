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

import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.bolt.structure.BoltNode;
import com.arcadedb.bolt.structure.BoltPath;
import com.arcadedb.bolt.structure.BoltRelationship;
import com.arcadedb.bolt.structure.BoltStructureMapper;
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

  @Test
  void mapperLocalDate() {
    final LocalDate date = LocalDate.of(2024, 1, 15);
    assertThat(BoltStructureMapper.toPackStreamValue(date)).isEqualTo("2024-01-15");
  }

  @Test
  void mapperLocalTime() {
    final LocalTime time = LocalTime.of(10, 30, 45);
    assertThat(BoltStructureMapper.toPackStreamValue(time)).isEqualTo("10:30:45");
  }

  @Test
  void mapperLocalDateTime() {
    final LocalDateTime dateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
    assertThat(BoltStructureMapper.toPackStreamValue(dateTime)).isEqualTo("2024-01-15T10:30:45");
  }

  @Test
  void mapperOffsetDateTime() {
    final OffsetDateTime dateTime = OffsetDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneOffset.UTC);
    assertThat(BoltStructureMapper.toPackStreamValue(dateTime)).isEqualTo("2024-01-15T10:30:45Z");
  }

  @Test
  void mapperZonedDateTime() {
    final ZonedDateTime dateTime = ZonedDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneId.of("UTC"));
    assertThat((String) BoltStructureMapper.toPackStreamValue(dateTime)).contains("2024-01-15T10:30:45");
  }

  @Test
  void mapperOffsetTime() {
    final OffsetTime time = OffsetTime.of(10, 30, 45, 0, ZoneOffset.UTC);
    assertThat(BoltStructureMapper.toPackStreamValue(time)).isEqualTo("10:30:45Z");
  }

  @Test
  void mapperInstant() {
    final Instant instant = Instant.parse("2024-01-15T10:30:45Z");
    assertThat(BoltStructureMapper.toPackStreamValue(instant)).isEqualTo("2024-01-15T10:30:45Z");
  }

  @Test
  void mapperJavaDate() {
    final Date date = Date.from(Instant.parse("2024-01-15T10:30:45Z"));
    assertThat(BoltStructureMapper.toPackStreamValue(date)).isEqualTo("2024-01-15T10:30:45Z");
  }

  @Test
  void mapperCalendar() {
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.setTimeInMillis(Instant.parse("2024-01-15T10:30:45Z").toEpochMilli());
    assertThat(BoltStructureMapper.toPackStreamValue(calendar)).isEqualTo("2024-01-15T10:30:45Z");
  }

  @Test
  void mapperUUID() {
    final UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    assertThat(BoltStructureMapper.toPackStreamValue(uuid)).isEqualTo("123e4567-e89b-12d3-a456-426614174000");
  }

  @Test
  void mapperRID() {
    final RID rid = new RID(null, 1, 100);
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
    final RID rid = new RID(null, 1, 100);
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
    final RID ridMaxBucket = new RID(null, 0xFFFF, 0);
    assertThat(BoltStructureMapper.ridToId(ridMaxBucket)).isEqualTo(0xFFFF_0000_0000_0000L);

    // Maximum valid position (48 bits)
    final RID ridMaxPosition = new RID(null, 0, 0xFFFF_FFFF_FFFFL);
    assertThat(BoltStructureMapper.ridToId(ridMaxPosition)).isEqualTo(0xFFFF_FFFF_FFFFL);
  }

  @Test
  void ridToIdInvalidBucketNegative() {
    final RID rid = new RID(null, -1, 100);
    assertThatThrownBy(() -> BoltStructureMapper.ridToId(rid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bucket ID out of range");
  }

  @Test
  void ridToIdInvalidBucketTooLarge() {
    final RID rid = new RID(null, 0x10000, 100); // 65536, exceeds 16-bit limit
    assertThatThrownBy(() -> BoltStructureMapper.ridToId(rid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bucket ID out of range");
  }

  @Test
  void ridToIdInvalidPositionNegative() {
    final RID rid = new RID(null, 1, -1);
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
