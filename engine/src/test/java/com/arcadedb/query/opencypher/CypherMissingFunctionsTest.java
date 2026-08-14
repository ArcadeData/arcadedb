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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  // ========== coll.avg ==========
  @Test
  void collAvg() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.avg([1, 2, 3, 4]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isEqualTo(2.5);
  }

  @Test
  void collAvgEmpty() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.avg([]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  // ========== coll.distinct ==========
  @Test
  void collDistinct() {
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
  void collDistinctNull() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.distinct(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  // ========== coll.flatten ==========
  @Test
  void collFlattenDefaultOneLevelDeep() {
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
  void collFlattenDepthZero() {
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
  void collFlattenNullDepthReturnsNull() {
    // #3444: coll.flatten(list, null) should return null
    final ResultSet rs = database.query("opencypher", "RETURN coll.flatten(['a'], null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  void collFlattenMultipleLevels() {
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
  void collIndexOf() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.indexOf([10, 20, 30], 20) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(1L);
  }

  @Test
  void collIndexOfNotFound() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.indexOf([10, 20, 30], 99) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(-1L);
  }

  // ========== coll.insert ==========
  @Test
  void collInsert() {
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
  void collMax() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.max([3, 1, 4, 1, 5, 9]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(9L);
  }

  @Test
  void collMaxStrings() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.max(['banana', 'apple', 'cherry']) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("cherry");
  }

  // ========== coll.min ==========
  @Test
  void collMin() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.min([3, 1, 4, 1, 5, 9]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(1L);
  }

  // ========== coll.remove ==========
  @Test
  void collRemove() {
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
  void collRemoveMultiple() {
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
  void collSort() {
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

  // ========== coll.sum ==========
  @Test
  void collSum() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.sum([1, 2, 3, 4]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isEqualTo(10.0);
  }

  @Test
  void collSumEmpty() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.sum([]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isEqualTo(0.0);
  }

  @Test
  void collSumWrongArity() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.sum() AS result").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void collSumNonNumericElement() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.sum([1, 2, 'x']) AS result").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void collSumNullElementIsSkipped() {
    // requireNumberArgument propagates null for a null element rather than treating it as a type error,
    // so a null in the list is skipped, not rejected - documented explicitly, not just an inferred side effect.
    final ResultSet rs = database.query("opencypher", "RETURN coll.sum([1, 2, null]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isEqualTo(3.0);
  }

  @Test
  void collSumApocPrefix() {
    final ResultSet rs = database.query("opencypher", "RETURN apoc.coll.sum([1, 2, 3, 4]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isEqualTo(10.0);
  }

  // ========== coll.avg ==========
  @Test
  void collAvgWrongArity() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.avg() AS result").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void collAvgNonNumericElement() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.avg([1, 2, 'x']) AS result").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void collAvgNullElementIsSkipped() {
    // Same null-propagation as coll.sum: a null element is skipped rather than counted or rejected, so
    // coll.avg([1, 2, null]) averages over the 2 non-null elements, not 3.
    final ResultSet rs = database.query("opencypher", "RETURN coll.avg([1, 2, null]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isEqualTo(1.5);
  }

  @Test
  void collAvgApocPrefix() {
    final ResultSet rs = database.query("opencypher", "RETURN apoc.coll.avg([1, 2, 3, 4]) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isEqualTo(2.5);
  }

  // ========== coll.union ==========
  @Test
  void collUnion() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.union([1, 2, 3], [2, 3, 4]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(4);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
    assertThat(((Number) result.get(3)).longValue()).isEqualTo(4L);
  }

  @Test
  void collUnionWrongArity() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.union([1, 2]) AS result").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void collUnionDedupsByTypeAndValue() {
    // Dedup is by object equality, so an integer and a float of the same numeric value are NOT collapsed -
    // e.g. coll.union([1], [1.0]) keeps both. Documented here since it's an easy surprise coming from Neo4j.
    final ResultSet rs = database.query("opencypher", "RETURN coll.union([1], [1.0]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(2);
  }

  @Test
  void collUnionApocPrefix() {
    final ResultSet rs = database.query("opencypher", "RETURN apoc.coll.union([1, 2], [2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
  }

  // ========== coll.unionAll ==========
  @Test
  void collUnionAll() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.unionAll([1, 2, 3], [2, 3, 4]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    // Unlike coll.union, duplicates across the two lists are preserved
    assertThat(result).hasSize(6);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
    assertThat(((Number) result.get(3)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(4)).longValue()).isEqualTo(3L);
    assertThat(((Number) result.get(5)).longValue()).isEqualTo(4L);
  }

  @Test
  void collUnionAllWrongArity() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.unionAll([1, 2]) AS result").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void collUnionAllApocPrefix() {
    // A 3-segment name post-strip (apoc.coll.unionAll -> coll.unionAll) exercises more of the dotted-name
    // grammar than the 2-segment coll.* spellings, so this is worth its own round-trip check.
    final ResultSet rs = database.query("opencypher", "RETURN apoc.coll.unionAll([1, 2], [2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(4);
  }

  // ========== coll.toSet ==========
  @Test
  void collToSet() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.toSet([1, 2, 2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
  }

  @Test
  void collToSetEmpty() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.toSet([]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).isEmpty();
  }

  @Test
  void collToSetNull() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.toSet(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  void collToSetKeepsNullElement() {
    // A null element is a value like any other, so it survives dedup exactly once.
    final ResultSet rs = database.query("opencypher", "RETURN coll.toSet([1, null, 1, null]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(2);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(result.get(1)).isNull();
  }

  @Test
  void collToSetDedupsByTypeAndValue() {
    // Same caveat as coll.union/coll.distinct: dedup is by object equality, so an integer and a float of the
    // same numeric value are NOT collapsed. Pinned here so the whole coll.* namespace stays consistent.
    final ResultSet rs = database.query("opencypher", "RETURN coll.toSet([1, 1.0]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(2);
  }

  @Test
  void collToSetWrongArity() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.toSet([1, 2], [3]) AS result").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void collToSetNonListArgument() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.toSet('not a list') AS result").hasNext())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("coll.toSet() requires a list argument");
  }

  @Test
  void collToSetApocPrefix() {
    final ResultSet rs = database.query("opencypher", "RETURN apoc.coll.toSet([1, 2, 2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(3);
    assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) result.get(1)).longValue()).isEqualTo(2L);
    assertThat(((Number) result.get(2)).longValue()).isEqualTo(3L);
  }

  // ========== coll.pairsMin ==========
  @Test
  @SuppressWarnings("unchecked")
  void collPairsMin() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.pairsMin([1, 2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(2);

    final List<Object> first = (List<Object>) result.get(0);
    assertThat(first).hasSize(2);
    assertThat(((Number) first.get(0)).longValue()).isEqualTo(1L);
    assertThat(((Number) first.get(1)).longValue()).isEqualTo(2L);

    final List<Object> second = (List<Object>) result.get(1);
    assertThat(second).hasSize(2);
    assertThat(((Number) second.get(0)).longValue()).isEqualTo(2L);
    assertThat(((Number) second.get(1)).longValue()).isEqualTo(3L);
  }

  @Test
  @SuppressWarnings("unchecked")
  void collPairsMinDropsTheTrailingIncompletePair() {
    // This is the whole difference from APOC's coll.pairs: no [3, null] tail is appended. Assert the last pair
    // specifically, so this stays a check on the tail rather than a restatement of collPairsMin()'s size check.
    final ResultSet rs = database.query("opencypher", "RETURN coll.pairsMin([1, 2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> result = rs.next().getProperty("result");

    final List<Object> last = (List<Object>) result.get(result.size() - 1);
    assertThat(last).doesNotContainNull();
    assertThat(((Number) last.get(0)).longValue()).isEqualTo(2L);
    assertThat(((Number) last.get(1)).longValue()).isEqualTo(3L);
  }

  @Test
  void collPairsMinSingleElement() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.pairsMin([1]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).isEmpty();
  }

  @Test
  void collPairsMinEmpty() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.pairsMin([]) AS result");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).isEmpty();
  }

  @Test
  void collPairsMinNull() {
    final ResultSet rs = database.query("opencypher", "RETURN coll.pairsMin(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  void collPairsMinOfNestedLists() {
    // The elements themselves may be lists: pairing is positional and does not look inside them.
    final ResultSet rs = database.query("opencypher", "RETURN coll.pairsMin([[1, 2], [3, 4]]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(1);

    final List<Object> pair = (List<Object>) result.get(0);
    assertThat(pair).hasSize(2);
    assertThat((List<Object>) pair.get(0)).hasSize(2);
    assertThat((List<Object>) pair.get(1)).hasSize(2);
  }

  @Test
  void collPairsMinWrongArity() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.pairsMin([1, 2], [3]) AS result").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void collPairsMinNonListArgument() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN coll.pairsMin('not a list') AS result").hasNext())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("coll.pairsMin() requires a list argument");
  }

  @Test
  @SuppressWarnings("unchecked")
  void collPairsMinApocPrefix() {
    final ResultSet rs = database.query("opencypher", "RETURN apoc.coll.pairsMin([1, 2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> result = rs.next().getProperty("result");
    assertThat(result).hasSize(2);
    assertThat((List<Object>) result.get(0)).hasSize(2);
    assertThat((List<Object>) result.get(1)).hasSize(2);
  }

  // ========== elementId ==========
  @Test
  void elementId() {
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
  void existsWithValue() {
    final ResultSet rs = database.query("opencypher", "RETURN exists('hello') AS result");
    assertThat(rs.hasNext()).isTrue();
    final Boolean result = rs.next().getProperty("result");
    assertThat(result).isTrue();
  }

  @Test
  void existsWithNull() {
    final ResultSet rs = database.query("opencypher", "RETURN exists(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    final Boolean result = rs.next().getProperty("result");
    assertThat(result).isFalse();
  }

  // ========== toBooleanList ==========
  @Test
  void toBooleanList() {
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
  void toFloatList() {
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
  void toIntegerList() {
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
  void toStringList() {
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
  void lower() {
    final ResultSet rs = database.query("opencypher", "RETURN lower('HELLO') AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
  }

  // ========== upper (alias for toUpper) ==========
  @Test
  void upper() {
    final ResultSet rs = database.query("opencypher", "RETURN upper('hello') AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("HELLO");
  }

  // ========== btrim (alias for trim) ==========
  @Test
  void btrim() {
    final ResultSet rs = database.query("opencypher", "RETURN btrim('  hello  ') AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
  }

  // ========== normalize ==========
  @Test
  void normalize() {
    final ResultSet rs = database.query("opencypher", "RETURN normalize('caf\\u0065\\u0301') AS result");
    assertThat(rs.hasNext()).isTrue();
    final String result = rs.next().getProperty("result");
    assertThat(result).isNotNull();
    // NFC normalization combines the e + combining acute into a single character
    assertThat(result).isEqualTo("caf\u00e9");
  }

  @Test
  void normalizeWithForm() {
    final ResultSet rs = database.query("opencypher", "RETURN normalize('caf\\u00e9', 'NFD') AS result");
    assertThat(rs.hasNext()).isTrue();
    final String result = rs.next().getProperty("result");
    assertThat(result).isNotNull();
    // NFD decomposes the é into e + combining acute accent
    assertThat(result).isEqualTo("caf\u0065\u0301");
  }

  // ========== vector (alias for vector_create) ==========
  @Test
  void vector() {
    final ResultSet rs = database.query("opencypher", "RETURN vector([1.0, 2.0, 3.0]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final Object result = rs.next().getProperty("result");
    assertThat(result).isInstanceOf(float[].class);
    assertThat((float[]) result).hasSize(3);
  }

  // ========== vector_dimension_count ==========
  @Test
  void vectorDimensionCount() {
    final ResultSet rs = database.query("opencypher", "RETURN vector.dimension.count(vector([1.0, 2.0, 3.0])) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").longValue()).isEqualTo(3L);
  }

  // ========== vector_distance ==========
  @Test
  void vectorDistanceEuclidean() {
    final ResultSet rs = database.query("opencypher",
        "RETURN vector.distance(vector([0.0, 0.0]), vector([3.0, 4.0])) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isCloseTo(5.0, Offset.offset(0.001));
  }

  @Test
  void vectorDistanceManhattan() {
    final ResultSet rs = database.query("opencypher",
        "RETURN vector.distance(vector([0.0, 0.0]), vector([3.0, 4.0]), 'MANHATTAN') AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isCloseTo(7.0, Offset.offset(0.001));
  }

  // ========== vector.distance.euclidean ==========
  @Test
  void vectorDistanceEuclideanDot() {
    final ResultSet rs = database.query("opencypher",
        "RETURN vector.distance.euclidean(vector([1.0, 0.0]), vector([0.0, 1.0])) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isCloseTo(Math.sqrt(2.0), Offset.offset(0.001));
  }

  // ========== vector.norm ==========
  @Test
  void vectorNorm() {
    final ResultSet rs = database.query("opencypher", "RETURN vector.norm(vector([3.0, 4.0])) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isCloseTo(5.0, Offset.offset(0.001));
  }

  // ========== APOC-compatible access via apoc.coll.* ==========
  @Test
  void apocCollDistinct() {
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
  void toStringListNull() {
    final ResultSet rs = database.query("opencypher", "RETURN toStringList(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  void normalizeNull() {
    final ResultSet rs = database.query("opencypher", "RETURN normalize(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }

  @Test
  void vectorDimensionCountNull() {
    final ResultSet rs = database.query("opencypher", "RETURN vector.dimension.count(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("result")).isNull();
  }
}
