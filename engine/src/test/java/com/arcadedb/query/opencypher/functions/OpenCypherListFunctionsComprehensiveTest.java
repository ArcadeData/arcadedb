/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for OpenCypher List functions based on Neo4j Cypher documentation.
 * Tests cover all 21 list functions including coll.* functions, keys(), labels(), nodes(), range(), reduce(), relationships(), reverse(), tail(), and to*List() functions.
 */
class OpenCypherListFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-list-functions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // Create test graph matching Neo4j documentation
    database.getSchema().createVertexType("Developer");
    database.getSchema().createVertexType("Administrator");
    database.getSchema().createVertexType("Designer");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("MARRIED");

    database.command("opencypher",
        "CREATE " +
            "(alice:Developer {name:'Alice', age: 38, eyes: 'Brown'}), " +
            "(bob:Administrator {name: 'Bob', age: 25, eyes: 'Blue'}), " +
            "(charlie:Administrator {name: 'Charlie', age: 53, eyes: 'Green'}), " +
            "(daniel:Administrator {name: 'Daniel', age: 54, eyes: 'Brown'}), " +
            "(eskil:Designer {name: 'Eskil', age: 41, eyes: 'blue', likedColors: ['Pink', 'Yellow', 'Black']}), " +
            "(alice)-[:KNOWS]->(bob), " +
            "(alice)-[:KNOWS]->(charlie), " +
            "(bob)-[:KNOWS]->(daniel), " +
            "(charlie)-[:KNOWS]->(daniel), " +
            "(bob)-[:MARRIED]->(eskil)");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== coll.distinct() Tests ====================

  @Test
  void collDistinctBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.distinct([1, 3, 2, 4, 2, 3, 1]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> distinct = (List<Object>) result.next().getProperty("result");
    assertThat(distinct).containsExactly(1L, 3L, 2L, 4L);
  }

  @Test
  void collDistinctMixedTypes() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.distinct([1, true, true, null, 'a', false, true, 1, null]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> distinct = (List<Object>) result.next().getProperty("result");
    assertThat(distinct).hasSize(5);
    assertThat(distinct.get(0)).isEqualTo(1L);
    assertThat(distinct.get(1)).isEqualTo(true);
    assertThat(distinct.get(2)).isNull();
    assertThat(distinct.get(3)).isEqualTo("a");
    assertThat(distinct.get(4)).isEqualTo(false);
  }

  @Test
  void collDistinctEmptyList() {
    final ResultSet result = database.command("opencypher", "RETURN coll.distinct([]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> distinct = (List<Object>) result.next().getProperty("result");
    assertThat(distinct).isEmpty();
  }

  @Test
  void collDistinctNull() {
    final ResultSet result = database.command("opencypher", "RETURN coll.distinct(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coll.flatten() Tests ====================

  @Test
  void collFlattenDefaultDepth() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.flatten(['a', ['b', ['c']]]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> flattened = (List<Object>) result.next().getProperty("result");
    assertThat(flattened).hasSize(3);
    assertThat(flattened.get(0)).isEqualTo("a");
    assertThat(flattened.get(1)).isEqualTo("b");
    // Third element should still be a list since default depth is 1
    assertThat(flattened.get(2)).isInstanceOf(List.class);
  }

  @Test
  void collFlattenWithDepth() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.flatten(['a', ['b', ['c']]], 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> flattened = (List<Object>) result.next().getProperty("result");
    assertThat(flattened).containsExactly("a", "b", "c");
  }

  @Test
  void collFlattenDepthZero() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.flatten(['a', ['b']], 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> flattened = (List<Object>) result.next().getProperty("result");
    assertThat(flattened).hasSize(2);
    assertThat(flattened.get(0)).isEqualTo("a");
    assertThat(flattened.get(1)).isInstanceOf(List.class);
  }

  @Test
  void collFlattenNull() {
    ResultSet result = database.command("opencypher", "RETURN coll.flatten(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN coll.flatten(['a'], null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coll.indexOf() Tests ====================

  @Test
  void collIndexOfBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.indexOf(['a', 'b', 'c', 'c'], 'c') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).longValue()).isEqualTo(2L);
  }

  @Test
  void collIndexOfNotFound() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.indexOf([1, 'b', false], 4.3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).longValue()).isEqualTo(-1L);
  }

  @Test
  void collIndexOfFirstMatch() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.indexOf([1, 2, 3, 2], 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).longValue()).isEqualTo(1L);
  }

  @Test
  void collIndexOfNull() {
    ResultSet result = database.command("opencypher", "RETURN coll.indexOf(null, 'a') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN coll.indexOf(['a'], null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coll.insert() Tests ====================

  @Test
  void collInsertBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.insert([true, 'a', 1, 5.4], 1, false) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> inserted = (List<Object>) result.next().getProperty("result");
    assertThat(inserted).hasSize(5);
    assertThat(inserted.get(0)).isEqualTo(true);
    assertThat(inserted.get(1)).isEqualTo(false);
    assertThat(inserted.get(2)).isEqualTo("a");
  }

  @Test
  void collInsertAtStart() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.insert([1, 2, 3], 0, 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> inserted = (List<Object>) result.next().getProperty("result");
    assertThat(inserted).containsExactly(0L, 1L, 2L, 3L);
  }

  @Test
  void collInsertAtEnd() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.insert([1, 2, 3], 3, 4) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> inserted = (List<Object>) result.next().getProperty("result");
    assertThat(inserted).containsExactly(1L, 2L, 3L, 4L);
  }

  @Test
  void collInsertNegativeIndexRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN coll.insert([1, 2], -1, 0) AS result"))
        .hasMessageContaining("negative");
  }

  @Test
  void collInsertIndexTooLargeRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN coll.insert([1, 2], 10, 0) AS result"))
        .hasMessageContaining("index");
  }

  @Test
  void collInsertNull() {
    ResultSet result = database.command("opencypher", "RETURN coll.insert(null, 0, 'a') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN coll.insert([1], null, 'a') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coll.max() Tests ====================

  @Test
  void collMaxBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.max([true, 'a', 1, 5.4]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isEqualTo(5.4);
  }

  @Test
  void collMaxNumbers() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.max([1, 5, 3, 9, 2]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(9);
  }

  @Test
  void collMaxEmptyList() {
    final ResultSet result = database.command("opencypher", "RETURN coll.max([]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void collMaxNull() {
    final ResultSet result = database.command("opencypher", "RETURN coll.max(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coll.min() Tests ====================

  @Test
  void collMinBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.min([true, 'a', 1, 5.4]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void collMinNumbers() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.min([5, 1, 9, 2, 3]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(1);
  }

  @Test
  void collMinEmptyList() {
    final ResultSet result = database.command("opencypher", "RETURN coll.min([]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void collMinNull() {
    final ResultSet result = database.command("opencypher", "RETURN coll.min(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coll.remove() Tests ====================

  @Test
  void collRemoveBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.remove([true, 'a', 1, 5.4], 1) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> removed = (List<Object>) result.next().getProperty("result");
    assertThat(removed).containsExactly(true, 1L, 5.4);
  }

  @Test
  void collRemoveFirst() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.remove([1, 2, 3], 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> removed = (List<Object>) result.next().getProperty("result");
    assertThat(removed).containsExactly(2L, 3L);
  }

  @Test
  void collRemoveLast() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.remove([1, 2, 3], 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> removed = (List<Object>) result.next().getProperty("result");
    assertThat(removed).containsExactly(1L, 2L);
  }

  @Test
  void collRemoveNegativeIndexRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN coll.remove([1, 2], -1) AS result"))
        .hasMessageContaining("negative");
  }

  @Test
  void collRemoveIndexTooLargeRaisesError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN coll.remove([1, 2], 10) AS result"))
        .hasMessageContaining("index");
  }

  @Test
  void collRemoveNull() {
    ResultSet result = database.command("opencypher", "RETURN coll.remove(null, 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN coll.remove([1], null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coll.sort() Tests ====================

  @Test
  void collSortBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.sort([true, 'a', 1, 2]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> sorted = (List<Object>) result.next().getProperty("result");
    assertThat(sorted).hasSize(4);
    // Cypher ordering: strings < booleans < numbers
    assertThat(sorted.get(0)).isEqualTo("a");
    assertThat(sorted.get(1)).isEqualTo(true);
  }

  @Test
  void collSortNumbers() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.sort([5, 1, 9, 2, 3]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> sorted = (List<Object>) result.next().getProperty("result");
    assertThat(sorted).containsExactly(1L, 2L, 3L, 5L, 9L);
  }

  @Test
  void collSortStrings() {
    final ResultSet result = database.command("opencypher",
        "RETURN coll.sort(['zebra', 'apple', 'banana']) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> sorted = (List<Object>) result.next().getProperty("result");
    assertThat(sorted).containsExactly("apple", "banana", "zebra");
  }

  @Test
  void collSortNull() {
    final ResultSet result = database.command("opencypher", "RETURN coll.sort(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== keys() Tests ====================

  @Test
  void keysFromNode() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) WHERE a.name = 'Alice' RETURN keys(a) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> keys = (List<String>) result.next().getProperty("result");
    assertThat(keys).containsExactlyInAnyOrder("name", "age", "eyes");
  }

  @Test
  void keysFromMap() {
    final ResultSet result = database.command("opencypher",
        "RETURN keys({name: 'Alice', age: 38}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> keys = (List<String>) result.next().getProperty("result");
    assertThat(keys).containsExactlyInAnyOrder("name", "age");
  }

  @Test
  void keysNull() {
    final ResultSet result = database.command("opencypher", "RETURN keys(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== labels() Tests ====================

  @Test
  void labelsBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) WHERE a.name = 'Alice' RETURN labels(a) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> labels = (List<String>) result.next().getProperty("result");
    assertThat(labels).contains("Developer");
  }

  @Test
  void labelsNull() {
    final ResultSet result = database.command("opencypher", "RETURN labels(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== nodes() Tests ====================

  @Test
  void nodesFromPath() {
    final ResultSet result = database.command("opencypher",
        "MATCH p = (a)-->(b)-->(c) WHERE a.name = 'Alice' AND c.name = 'Eskil' " +
            "RETURN size(nodes(p)) AS nodeCount");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("nodeCount")).intValue()).isEqualTo(3);
  }

  @Test
  void nodesNull() {
    final ResultSet result = database.command("opencypher", "RETURN nodes(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== range() Tests ====================

  @Test
  void rangeBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN range(0, 10) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> range = (List<Object>) result.next().getProperty("result");
    assertThat(range).hasSize(11);
    assertThat(((Number) range.get(0)).intValue()).isEqualTo(0);
    assertThat(((Number) range.get(10)).intValue()).isEqualTo(10);
  }

  @Test
  void rangeWithStep() {
    final ResultSet result = database.command("opencypher",
        "RETURN range(2, 18, 3) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> range = (List<Object>) result.next().getProperty("result");
    assertThat(range).hasSize(6);
    assertThat(((Number) range.get(0)).intValue()).isEqualTo(2);
    assertThat(((Number) range.get(1)).intValue()).isEqualTo(5);
    assertThat(((Number) range.get(5)).intValue()).isEqualTo(17);
  }

  @Test
  void rangeDecreasing() {
    final ResultSet result = database.command("opencypher",
        "RETURN range(10, 0, -2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> range = (List<Object>) result.next().getProperty("result");
    assertThat(range).hasSize(6);
    assertThat(((Number) range.get(0)).intValue()).isEqualTo(10);
    assertThat(((Number) range.get(5)).intValue()).isEqualTo(0);
  }

  @Test
  void rangeEmpty() {
    // Positive start, positive end, negative step = empty range
    final ResultSet result = database.command("opencypher",
        "RETURN range(0, 5, -1) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> range = (List<Object>) result.next().getProperty("result");
    assertThat(range).isEmpty();
  }

  // ==================== reduce() Tests ====================

  @Test
  void reduceBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH p = (a)-->(b)-->(c) " +
            "WHERE a.name = 'Alice' AND b.name = 'Bob' AND c.name = 'Daniel' " +
            "RETURN reduce(totalAge = 0, n IN nodes(p) | totalAge + n.age) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(117); // 38 + 25 + 54
  }

  @Test
  void reduceSimpleList() {
    final ResultSet result = database.command("opencypher",
        "RETURN reduce(sum = 0, x IN [1, 2, 3, 4, 5] | sum + x) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(15);
  }

  @Test
  void reduceMultiply() {
    final ResultSet result = database.command("opencypher",
        "RETURN reduce(product = 1, x IN [2, 3, 4] | product * x) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(24);
  }

  // ==================== relationships() Tests ====================

  @Test
  void relationshipsFromPath() {
    final ResultSet result = database.command("opencypher",
        "MATCH p = (a)-->(b)-->(c) WHERE a.name = 'Alice' AND c.name = 'Eskil' " +
            "RETURN size(relationships(p)) AS relCount");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("relCount")).intValue()).isEqualTo(2);
  }

  @Test
  void relationshipsNull() {
    final ResultSet result = database.command("opencypher", "RETURN relationships(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== reverse() Tests ====================

  @Test
  void reverseList() {
    final ResultSet result = database.command("opencypher",
        "RETURN reverse([4923, 'abc', 521, null, 487]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> reversed = (List<Object>) result.next().getProperty("result");
    assertThat(reversed).hasSize(5);
    assertThat(((Number) reversed.get(0)).intValue()).isEqualTo(487);
    assertThat(reversed.get(1)).isNull();
    assertThat(((Number) reversed.get(2)).intValue()).isEqualTo(521);
    assertThat(reversed.get(3)).isEqualTo("abc");
    assertThat(((Number) reversed.get(4)).intValue()).isEqualTo(4923);
  }

  @Test
  void reverseEmpty() {
    final ResultSet result = database.command("opencypher", "RETURN reverse([]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> reversed = (List<Object>) result.next().getProperty("result");
    assertThat(reversed).isEmpty();
  }

  @Test
  void reverseNull() {
    final ResultSet result = database.command("opencypher", "RETURN reverse(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== tail() Tests ====================

  @Test
  void tailBasic() {
    final ResultSet result = database.command("opencypher",
        "MATCH (a) WHERE a.name = 'Eskil' RETURN tail(a.likedColors) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<String> tail = (List<String>) result.next().getProperty("result");
    assertThat(tail).containsExactly("Yellow", "Black");
  }

  @Test
  void tailSimpleList() {
    final ResultSet result = database.command("opencypher",
        "RETURN tail([1, 2, 3, 4, 5]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> tail = (List<Object>) result.next().getProperty("result");
    assertThat(tail).hasSize(4);
    assertThat(((Number) tail.get(0)).intValue()).isEqualTo(2);
    assertThat(((Number) tail.get(3)).intValue()).isEqualTo(5);
  }

  @Test
  void tailSingleElement() {
    final ResultSet result = database.command("opencypher", "RETURN tail([1]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> tail = (List<Object>) result.next().getProperty("result");
    assertThat(tail).isEmpty();
  }

  @Test
  void tailEmpty() {
    final ResultSet result = database.command("opencypher", "RETURN tail([]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> tail = (List<Object>) result.next().getProperty("result");
    assertThat(tail).isEmpty();
  }

  // ==================== toBooleanList() Tests ====================

  @Test
  void toBooleanListBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBooleanList(['a string', true, 'false', null, ['A','B']]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> boolList = (List<Object>) result.next().getProperty("result");
    assertThat(boolList).hasSize(5);
    assertThat(boolList.get(0)).isNull(); // 'a string' not convertible
    assertThat(boolList.get(1)).isEqualTo(true);
    assertThat(boolList.get(2)).isEqualTo(false); // 'false' string converts to false
    assertThat(boolList.get(3)).isNull();
    assertThat(boolList.get(4)).isNull(); // list not convertible
  }

  @Test
  void toBooleanListNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBooleanList(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  @Test
  void toBooleanListNullsInList() {
    final ResultSet result = database.command("opencypher",
        "RETURN toBooleanList([null, null]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> boolList = (List<Object>) result.next().getProperty("result");
    assertThat(boolList).containsExactly(null, null);
  }

  // ==================== toFloatList() Tests ====================

  @Test
  void toFloatListBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN toFloatList(['a string', 2.5, '3.14159', null, ['A','B']]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> floatList = (List<Object>) result.next().getProperty("result");
    assertThat(floatList).hasSize(5);
    assertThat(floatList.get(0)).isNull();
    assertThat(((Number) floatList.get(1)).doubleValue()).isEqualTo(2.5);
    assertThat(((Number) floatList.get(2)).doubleValue()).isEqualTo(3.14159);
    assertThat(floatList.get(3)).isNull();
    assertThat(floatList.get(4)).isNull();
  }

  @Test
  void toFloatListNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN toFloatList(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toIntegerList() Tests ====================

  @Test
  void toIntegerListBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN toIntegerList(['a string', 2, '5', null, ['A','B']]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> intList = (List<Object>) result.next().getProperty("result");
    assertThat(intList).hasSize(5);
    assertThat(intList.get(0)).isNull();
    assertThat(((Number) intList.get(1)).intValue()).isEqualTo(2);
    assertThat(((Number) intList.get(2)).intValue()).isEqualTo(5);
    assertThat(intList.get(3)).isNull();
    assertThat(intList.get(4)).isNull();
  }

  @Test
  void toIntegerListNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN toIntegerList(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== toStringList() Tests ====================

  @Test
  void toStringListBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN toStringList(['already a string', 2, null, ['A','B']]) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> strList = (List<Object>) result.next().getProperty("result");
    assertThat(strList).hasSize(4);
    assertThat(strList.get(0)).isEqualTo("already a string");
    assertThat(strList.get(1)).isEqualTo("2");
    assertThat(strList.get(2)).isNull();
    assertThat(strList.get(3)).isNull(); // list not convertible
  }

  @Test
  void toStringListNull() {
    final ResultSet result = database.command("opencypher",
        "RETURN toStringList(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void listFunctionsCombined() {
    final ResultSet result = database.command("opencypher",
        "RETURN tail(coll.sort([5, 1, 3, 9, 2])) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> combined = (List<Object>) result.next().getProperty("result");
    assertThat(combined).containsExactly(2L, 3L, 5L, 9L); // sorted, then tail
  }

  @Test
  void listFunctionsWithReduce() {
    final ResultSet result = database.command("opencypher",
        "RETURN reduce(s = '', x IN ['hello', 'world'] | s + x + ' ') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("hello world ");
  }

  @Test
  void rangeWithReduce() {
    final ResultSet result = database.command("opencypher",
        "RETURN reduce(sum = 0, x IN range(1, 10) | sum + x) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(55);
  }
}
