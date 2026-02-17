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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for missing Cypher functions reported in GitHub issue #3420.
 * Covers: coll.*, toXxxList, elementId, exists, lower/upper/btrim, normalize, vector functions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMissingFunctionsTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-cypher-missing-functions").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  // ========== coll.distinct ==========
  @Test
  void testCollDistinct() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.distinct([1, 2, 2, 3, 3, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
  }

  @Test
  void testCollDistinctNull() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.distinct(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  // ========== coll.flatten ==========
  @Test
  void testCollFlattenDefaultOneLevelDeep() {
    // #3442: default flatten should only flatten one level deep
    final ResultSet rs = database.query("opencypher", "RETURN coll.flatten(['a', ['b', ['c']]]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    // One level flatten: ['a', 'b', ['c']]
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo("a");
    assertThat(result.get(1)).isEqualTo("b");
    assertThat(result.get(2)).isInstanceOf(List.class);
  }

  @Test
  void testCollFlattenDepthZero() {
    // #3443: coll.flatten(list, 0) should return the list unchanged
    final ResultSet rs = database.query("opencypher", "RETURN coll.flatten(['a', ['b']], 0) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(2);
    assertThat(result.get(0)).isEqualTo("a");
    assertThat(result.get(1)).isInstanceOf(List.class);
  }

  @Test
  void testCollFlattenNullDepthReturnsNull() {
    // #3444: coll.flatten(list, null) should return null
    final ResultSet rs = database.query("opencypher", "RETURN coll.flatten(['a'], null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  void testCollFlattenMultipleLevels() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.flatten([[1, 2], [3, [4, 5]]]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    // Default depth=1: flattens one level, so [4, 5] stays nested
    assertThat(result).hasSize(4);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
    assertThat(result.get(3)).isInstanceOf(List.class);
  }

  // ========== coll.indexOf ==========
  @Test
  void testCollIndexOf() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.indexOf([10, 20, 30], 20) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(1L);
  }

  @Test
  void testCollIndexOfNotFound() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.indexOf([10, 20, 30], 99) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(-1L);
  }

  // ========== coll.insert ==========
  @Test
  void testCollInsert() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.insert([1, 3, 4], 1, 2) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(4);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
    assertThat(((Number) result.get(3)).longValue()).isEqualTo(4L);
  }

  // ========== coll.max ==========
  @Test
  void testCollMax() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.max([3, 1, 4, 1, 5, 9]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(9L);
  }

  @Test
  void testCollMaxStrings() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.max(['banana', 'apple', 'cherry']) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("cherry");
  }

  // ========== coll.min ==========
  @Test
  void testCollMin() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.min([3, 1, 4, 1, 5, 9]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(1L);
  }

  // ========== coll.remove ==========
  @Test
  void testCollRemove() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.remove([1, 2, 3, 4], 1) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(3L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(4L);
  }

  @Test
  void testCollRemoveMultiple() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.remove([1, 2, 3, 4, 5], 1, 2) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(4L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(5L);
  }

  // ========== coll.sort ==========
  @Test
  void testCollSort() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.sort([3, 1, 4, 1, 5]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(5);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
    assertThat(((Number) result.get(3)).longValue()).isEqualTo(4L);
    assertThat(((Number) result.get(4)).longValue()).isEqualTo(5L);
  }

  // ========== elementId ==========
  @Test
  void testElementId() {
    database.getSchema().createVertexType("TestNode");
    database.transaction(() -> database.command("opencypher", "CREATE (:TestNode {name: 'test'})"));
    final ResultSet rs = database.query("opencypher", "MATCH (n:TestNode) RETURN elementId(n) AS eid");
    assertThat(rs.hasNext()).isTrue();
    final String eid = rs.next().getProperty("eid");
    assertThat(eid).isNotNull();
    assertThat(eid).contains(":");
  }

  // ========== exists ==========
  @Test
  void testExistsWithValue() {
    final ResultSet rs = database.query("opencypher", "RETURN exists('hello') AS result");
    assertThat(rs.hasNext()).isTrue();
    final Boolean result = rs.next().getProperty("result");
    assertThat(result).isTrue();
  }

  @Test
  void testExistsWithNull() {
    final ResultSet rs = database.query("opencypher", "RETURN exists(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    final Boolean result = rs.next().getProperty("result");
    assertThat(result).isFalse();
  }

  // ========== toBooleanList ==========
  @Test
  void testToBooleanList() {
    final ResultSet rs = database.query("opencypher", "RETURN toBooleanList(['true', 'false', '1']) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo(true);
    assertThat(result.get(1)).isEqualTo(false);
  }

  // ========== toFloatList ==========
  @Test
  void testToFloatList() {
    final ResultSet rs = database.query("opencypher", "RETURN toFloatList(['1.5', '2.5', '3.5']) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(((Number) result.get(0)).doubleValue()).isEqualTo(1.5);
    assertThat(((Number) result.get(1)).doubleValue()).isEqualTo(2.5);
  }

  // ========== toIntegerList ==========
  @Test
  void testToIntegerList() {
    final ResultSet rs = database.query("opencypher", "RETURN toIntegerList(['1', '2', '3']) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
  }

  // ========== toStringList ==========
  @Test
  void testToStringList() {
    final ResultSet rs = database.query("opencypher", "RETURN toStringList([1, 2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo("1");
    assertThat(result.get(1)).isEqualTo("2");
    assertThat(result.get(2)).isEqualTo("3");
  }

  // ========== lower (alias for toLower) ==========
  @Test
  void testLower() {
    final ResultSet rs = database.query("opencypher", "RETURN lower('HELLO') AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
  }

  // ========== upper (alias for toUpper) ==========
  @Test
  void testUpper() {
    final ResultSet rs = database.query("opencypher", "RETURN upper('hello') AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("HELLO");
  }

  // ========== btrim (alias for trim) ==========
  @Test
  void testBtrim() {
    final ResultSet rs = database.query("opencypher", "RETURN btrim('  hello  ') AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
  }

  // ========== normalize ==========
  @Test
  void testNormalize() {
    final ResultSet rs = database.query("opencypher", "RETURN normalize('caf\\u0065\\u0301') AS result");
    assertThat(rs.hasNext()).isTrue();
    final String result = rs.next().getProperty("result");
    assertThat(result).isNotNull();
    // NFC normalization combines the e + combining acute into a single character
    assertThat(result).isEqualTo("caf\u00e9");
  }

  @Test
  void testNormalizeWithForm() {
    final ResultSet rs = database.query("opencypher", "RETURN normalize('caf\\u00e9', 'NFD') AS result");
    assertThat(rs.hasNext()).isTrue();
    final String result = rs.next().getProperty("result");
    assertThat(result).isNotNull();
    // NFD decomposes the é into e + combining acute accent
    assertThat(result).isEqualTo("caf\u0065\u0301");
  }

  // ========== vector (alias for vector_create) ==========
  @Test
  void testVector() {
    final ResultSet rs = database.query("opencypher", "RETURN vector([1.0, 2.0, 3.0]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final Object result = rs.next().getProperty("result");
    assertThat(result).isInstanceOf(float[].class);
    assertThat(((float[]) result)).hasSize(3);
  }

  // ========== vector_dimension_count ==========
  @Test
  void testVectorDimensionCount() {
    final ResultSet rs = database.query("opencypher", "RETURN vector_dimension_count(vector([1.0, 2.0, 3.0])) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(3L);
  }

  // ========== vector_distance ==========
  @Test
  void testVectorDistanceEuclidean() {
    final ResultSet rs = database.query("opencypher",
        "RETURN vector_distance(vector([0.0, 0.0]), vector([3.0, 4.0])) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isCloseTo(5.0, Offset.offset(0.001));
  }

  @Test
  void testVectorDistanceManhattan() {
    final ResultSet rs = database.query("opencypher",
        "RETURN vector_distance(vector([0.0, 0.0]), vector([3.0, 4.0]), 'MANHATTAN') AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isCloseTo(7.0, Offset.offset(0.001));
  }

  // ========== vector.distance.euclidean ==========
  @Test
  void testVectorDistanceEuclideanDot() {
    final ResultSet rs = database.query("opencypher",
        "RETURN vector.distance.euclidean(vector([1.0, 0.0]), vector([0.0, 1.0])) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isCloseTo(Math.sqrt(2.0), Offset.offset(0.001));
  }

  // ========== vector.norm ==========
  @Test
  void testVectorNorm() {
    final ResultSet rs = database.query("opencypher", "RETURN vector.norm(vector([3.0, 4.0])) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isCloseTo(5.0, Offset.offset(0.001));
  }

  // ========== APOC-compatible access via apoc.coll.* ==========
  @Test
  void testApocCollDistinct() {
    final ResultSet rs = database.query("opencypher", "RETURN apoc.coll.distinct([1, 1, 2]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(2);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
  }

  // ========== Null handling ==========
  @Test
  void testToStringListNull() {
    final ResultSet rs = database.query("opencypher", "RETURN toStringList(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  void testNormalizeNull() {
    final ResultSet rs = database.query("opencypher", "RETURN normalize(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  void testVectorDimensionCountNull() {
    final ResultSet rs = database.query("opencypher", "RETURN vector_dimension_count(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }
}
