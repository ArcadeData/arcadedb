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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for Cypher function support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherFunctionTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-function").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_AT");
    database.getSchema().createEdgeType("KNOWS");

    // Create test data
    database.transaction(() -> {
      final Vertex alice = database.newVertex("Person")
          .set("name", "Alice")
          .set("age", 30)
          .save();

      final Vertex bob = database.newVertex("Person")
          .set("name", "Bob")
          .set("age", 25)
          .save();

      final Vertex charlie = database.newVertex("Person")
          .set("name", "Charlie")
          .set("age", 35)
          .save();

      final Vertex arcadedb = database.newVertex("Company")
          .set("name", "ArcadeDB")
          .set("founded", 2021)
          .save();

      // Create relationships
      alice.newEdge("KNOWS", bob, "since", 2020).save();
      alice.newEdge("WORKS_AT", arcadedb, "since", 2021).save();
      bob.newEdge("WORKS_AT", arcadedb, "since", 2022).save();
      charlie.newEdge("KNOWS", alice, "since", 2019).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void idFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN id(n) AS personId");

    int count = 0;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      // id(n) is the Neo4j-compatible Long-encoded RID since issue #4183.
      assertThat(result.<Object>getProperty("personId")).isInstanceOf(Number.class);
      assertThat(result.<Number>getProperty("personId").longValue()).isGreaterThanOrEqualTo(0L);
      count++;
    }

    assertThat(count).as("Expected 3 persons").isEqualTo(3);
  }

  @Test
  void labelsFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person {name: 'Alice'}) RETURN labels(n) AS personLabels");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object labels = result.getProperty("personLabels");

    assertThat(labels).isNotNull();
    assertThat(labels).isInstanceOf(List.class);
    final List<?> labelsList = (List<?>) labels;
    assertThat(labelsList.contains("Person")).isTrue();
  }

  @Test
  void typeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r]->(b) RETURN type(r) AS relType");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final String relType = result.getProperty("relType");

    assertThat(relType).isNotNull();
    assertThat(relType.equals("KNOWS") || relType.equals("WORKS_AT")).isTrue();
  }

  @Test
  void keysFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person {name: 'Alice'}) RETURN keys(n) AS personKeys");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object keys = result.getProperty("personKeys");

    assertThat(keys).isNotNull();
    assertThat(keys).isInstanceOf(List.class);
    final List<?> keysList = (List<?>) keys;
    assertThat(keysList.contains("name")).isTrue();
    assertThat(keysList.contains("age")).isTrue();
  }

  @Test
  void countFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN count(n) AS totalPersons");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object count = result.getProperty("totalPersons");

    assertThat(count).isNotNull();
    assertThat(((Number) count).longValue()).isEqualTo(3L);
  }

  @Test
  void countStar() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN count(*) AS total");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object count = result.getProperty("total");

    assertThat(count).isNotNull();
    assertThat(((Number) count).longValue()).isEqualTo(3L);
  }

  @Test
  void sumFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN sum(n.age) AS totalAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object sum = result.getProperty("totalAge");

    assertThat(sum).isNotNull();
    assertThat(((Number) sum).intValue()).isEqualTo(90); // 30 + 25 + 35
  }

  @Test
  void avgFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN avg(n.age) AS averageAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object avg = result.getProperty("averageAge");

    assertThat(avg).isNotNull();
    assertThat(((Number) avg).doubleValue()).isCloseTo(30.0, within(0.01)); // (30 + 25 + 35) / 3
  }

  @Test
  void minFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN min(n.age) AS minAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object min = result.getProperty("minAge");

    assertThat(min).isNotNull();
    assertThat(((Number) min).intValue()).isEqualTo(25);
  }

  @Test
  void maxFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (n:Person) RETURN max(n.age) AS maxAge");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object max = result.getProperty("maxAge");

    assertThat(max).isNotNull();
    assertThat(((Number) max).intValue()).isEqualTo(35);
  }

  @Test
  void absFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN abs(-42) AS absValue");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object abs = result.getProperty("absValue");

    assertThat(abs).isNotNull();
    assertThat(((Number) abs).intValue()).isEqualTo(42);
  }

  @Test
  void sqrtFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN sqrt(16) AS sqrtValue");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object sqrt = result.getProperty("sqrtValue");

    assertThat(sqrt).isNotNull();
    assertThat(((Number) sqrt).doubleValue()).isCloseTo(4.0, within(0.01));
  }

  @Test
  void startNodeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b) RETURN startNode(r) AS source");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Single-variable RETURN with Document result is unwrapped to element
    assertThat(result.isElement()).isTrue();
    final Object source = result.toElement();

    assertThat(source).isNotNull();
    assertThat(source).isInstanceOf(Vertex.class);
  }

  @Test
  void endNodeFunction() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person)-[r:KNOWS]->(b) RETURN endNode(r) AS target");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Single-variable RETURN with Document result is unwrapped to element
    assertThat(result.isElement()).isTrue();
    final Object target = result.toElement();

    assertThat(target).isNotNull();
    assertThat(target).isInstanceOf(Vertex.class);
  }

  /**
   * Opens an isolated database for tests that need a clean schema or different seed data than the shared setup
   * provides. The returned database must be drop()'d by the caller.
   */
  private Database newIsolatedDatabase(final String tag) {
    return new DatabaseFactory("./target/databases/test-function-" + tag + "-" + UUID.randomUUID()).create();
  }

  // Issue #3920: tail(null) must return null per Cypher null-propagation
  @Test
  void tailWithNullReturnsNull() {
    final Database db = newIsolatedDatabase("tail-null");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN tail(null) AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("result")).isNull();
      assertThat(rs.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3920: tail([1,2,3]) returns the rest of the list
  @SuppressWarnings("unchecked")
  @Test
  void tailWithListReturnsRest() {
    final Database db = newIsolatedDatabase("tail-list");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN tail([1, 2, 3]) AS result");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("result");
      assertThat(list).containsExactly(2L, 3L);
    } finally {
      db.drop();
    }
  }

  // Issue #3920: tail([1]) returns empty list
  @SuppressWarnings("unchecked")
  @Test
  void tailWithSingleElementReturnsEmptyList() {
    final Database db = newIsolatedDatabase("tail-single");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN tail([1]) AS result");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("result");
      assertThat(list).isEmpty();
    } finally {
      db.drop();
    }
  }

  // Issue #3920: tail([]) returns empty list
  @SuppressWarnings("unchecked")
  @Test
  void tailWithEmptyListReturnsEmptyList() {
    final Database db = newIsolatedDatabase("tail-empty");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN tail([]) AS result");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("result");
      assertThat(list).isEmpty();
    } finally {
      db.drop();
    }
  }

  // Issue #3922: CASE null WHEN null does not match (Cypher 3-valued logic)
  @Test
  void caseNullWhenNullDoesNotMatch() {
    final Database db = newIsolatedDatabase("case-null-null");
    try {
      final ResultSet rs = db.query("opencypher",
          "RETURN CASE null WHEN null THEN 'matched' ELSE 'not_matched' END AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("result")).isEqualTo("not_matched");
      assertThat(rs.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3922: CASE value WHEN null does not match
  @Test
  void caseValueWhenNullDoesNotMatch() {
    final Database db = newIsolatedDatabase("case-val-null");
    try {
      final ResultSet rs = db.query("opencypher",
          "RETURN CASE 1 WHEN null THEN 'matched' ELSE 'not_matched' END AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("result")).isEqualTo("not_matched");
    } finally {
      db.drop();
    }
  }

  // Issue #3922: CASE null WHEN value does not match
  @Test
  void caseNullWhenValueDoesNotMatch() {
    final Database db = newIsolatedDatabase("case-null-val");
    try {
      final ResultSet rs = db.query("opencypher",
          "RETURN CASE null WHEN 1 THEN 'matched' ELSE 'not_matched' END AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("result")).isEqualTo("not_matched");
    } finally {
      db.drop();
    }
  }

  // Issue #3922: non-null equality sanity check still works
  @Test
  void caseValueEqualsValueMatches() {
    final Database db = newIsolatedDatabase("case-val-val");
    try {
      final ResultSet rs = db.query("opencypher",
          "RETURN CASE 1 WHEN 1 THEN 'matched' ELSE 'not_matched' END AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("result")).isEqualTo("matched");
    } finally {
      db.drop();
    }
  }

  // Issue #3922: CASE null WHEN null with no ELSE returns null
  @Test
  void caseNullWithoutElseReturnsNull() {
    final Database db = newIsolatedDatabase("case-null-noelse");
    try {
      final ResultSet rs = db.query("opencypher",
          "RETURN CASE null WHEN null THEN 'matched' END AS result");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("result")).isNull();
    } finally {
      db.drop();
    }
  }

  // Issue #3928: list || list concatenates with nulls preserved
  @SuppressWarnings("unchecked")
  @Test
  void listConcatenationWithNull() {
    final Database db = newIsolatedDatabase("concat-list-null");
    try {
      final ResultSet resultSet = db.query("opencypher", "RETURN [1, 2] || [3, null] AS result");
      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      final List<Object> list = (List<Object>) r.getProperty("result");
      assertThat(list).containsExactly(1L, 2L, 3L, null);
      assertThat(resultSet.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3928 (duplicate #3921): basic list concatenation via ||
  @SuppressWarnings("unchecked")
  @Test
  void listConcatenationBasic() {
    final Database db = newIsolatedDatabase("concat-list-basic");
    try {
      final ResultSet resultSet = db.query("opencypher", "RETURN [1, 2] || [3, 4] AS result");
      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      final List<Object> list = (List<Object>) r.getProperty("result");
      assertThat(list).containsExactly(1L, 2L, 3L, 4L);
    } finally {
      db.drop();
    }
  }

  // Issue #3928: string || string concatenation
  @Test
  void stringConcatenation() {
    final Database db = newIsolatedDatabase("concat-string");
    try {
      final ResultSet resultSet = db.query("opencypher", "RETURN 'Hello ' || 'World' AS result");
      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      assertThat(r.<String>getProperty("result")).isEqualTo("Hello World");
    } finally {
      db.drop();
    }
  }

  // Issue #3928 (#3926): null propagates through || on strings
  @Test
  void stringConcatenationWithPipeAndNull() {
    final Database db = newIsolatedDatabase("concat-string-null");
    try {
      final ResultSet resultSet = db.query("opencypher", "RETURN 'Hello' || null AS result");
      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      assertThat(r.<Object>getProperty("result")).isNull();
      assertThat(resultSet.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3928 (#3927): chained || concatenation with spaces
  @Test
  void stringConcatenationWithSpaces() {
    final Database db = newIsolatedDatabase("concat-string-spaces");
    try {
      final ResultSet resultSet = db.query("opencypher",
          "RETURN 'Alpha' || 'Beta' AS result1, 'Alpha' || ' ' || 'Beta' AS result2");
      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      assertThat(r.<String>getProperty("result1")).isEqualTo("AlphaBeta");
      assertThat(r.<String>getProperty("result2")).isEqualTo("Alpha Beta");
      assertThat(resultSet.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3928: chained list concatenation [1] || [2] || [3]
  @SuppressWarnings("unchecked")
  @Test
  void listConcatenationChained() {
    final Database db = newIsolatedDatabase("concat-list-chained");
    try {
      final ResultSet resultSet = db.query("opencypher", "RETURN [1] || [2] || [3] AS result");
      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      final List<Object> list = (List<Object>) r.getProperty("result");
      assertThat(list).containsExactly(1L, 2L, 3L);
    } finally {
      db.drop();
    }
  }

  // Issue #4099: list literal with duration() materializes one row
  @SuppressWarnings("unchecked")
  @Test
  void listLiteralWithDurationProducesRow() {
    final Database db = newIsolatedDatabase("list-duration-row");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN [1, duration({months: 1000000})] AS x");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("x");
      assertThat(list).hasSize(2);
      assertThat(list.get(0)).isEqualTo(1L);
      assertThat(list.get(1)).isNotNull();
    } finally {
      db.drop();
    }
  }

  // Issue #4099: duration() alone returns a row
  @Test
  void durationAloneProducesRow() {
    final Database db = newIsolatedDatabase("duration-alone");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN duration({months: 1000000}) AS d");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("d")).isNotNull();
    } finally {
      db.drop();
    }
  }

  // Issue #4099: UNWIND with list literal containing duration() yields all rows
  @Test
  void unwindWithListLiteralContainingDurationProducesAllRows() {
    final Database db = newIsolatedDatabase("unwind-list-duration");
    try {
      final ResultSet rs = db.query("opencypher",
          "UNWIND [1, 2, 3] AS num RETURN num, [1, duration({months: 1000000})] AS x");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
    } finally {
      db.drop();
    }
  }

  // Issue #4100: list subscript with inline aggregate index uses aggregate value
  @Test
  void aggregateAsListSubscriptReturnsValue() {
    final Database db = newIsolatedDatabase("agg-subscript");
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice', scores:[85,92,78]})"));
      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person {name:'Alice'}) "
              + "OPTIONAL MATCH (p)-[:HAS_FRIEND]->(f:Person) "
              + "WITH p, collect(f) AS friends "
              + "RETURN p.scores[toInteger(avg(size(friends)))] AS selectedScore");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("selectedScore").longValue()).isEqualTo(85L);
    } finally {
      db.drop();
    }
  }

  // Issue #4100: control - non-aggregate subscript still works
  @Test
  void nonAggregateSubscriptControl() {
    final Database db = newIsolatedDatabase("nonagg-subscript");
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice', scores:[85,92,78]})"));
      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person {name:'Alice'}) "
              + "OPTIONAL MATCH (p)-[:HAS_FRIEND]->(f:Person) "
              + "WITH p, collect(f) AS friends "
              + "RETURN p.scores[size(friends)] AS selectedScore");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("selectedScore").longValue()).isEqualTo(85L);
    } finally {
      db.drop();
    }
  }

  // Issue #4107: reduce() over inline collect() folds into per-group aggregator
  @Test
  void reduceOverInlineCollectPerGroup() {
    final Database db = newIsolatedDatabase("reduce-collect-group");
    try {
      seedThreePersons(db);
      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person) "
              + "RETURN p.name AS name, "
              + "       reduce(total = 0, n IN collect(p.age) | total + n) AS total_age_sum "
              + "ORDER BY name");
      final List<Long> values = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        values.add(r.<Number>getProperty("total_age_sum").longValue());
      }
      assertThat(values).containsExactly(30L, 25L, 35L);
    } finally {
      db.drop();
    }
  }

  // Issue #4107: reduce() over inline [sum(...)] per group
  @Test
  void reduceOverInlineSumPerGroup() {
    final Database db = newIsolatedDatabase("reduce-sum-group");
    try {
      seedThreePersons(db);
      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person) "
              + "RETURN p.name AS name, "
              + "       reduce(total = 0, n IN [sum(p.age)] | total + n) AS s "
              + "ORDER BY name");
      final List<Long> values = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        values.add(r.<Number>getProperty("s").longValue());
      }
      assertThat(values).containsExactly(30L, 25L, 35L);
    } finally {
      db.drop();
    }
  }

  // Issue #4107: reduce() over inline [count(*)] per group
  @Test
  void reduceOverInlineCountStarPerGroup() {
    final Database db = newIsolatedDatabase("reduce-count-group");
    try {
      seedThreePersons(db);
      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person) "
              + "RETURN p.name AS name, "
              + "       reduce(total = 0, n IN [count(*)] | total + n) AS s "
              + "ORDER BY name");
      final List<Long> values = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        values.add(r.<Number>getProperty("s").longValue());
      }
      assertThat(values).containsExactly(1L, 1L, 1L);
    } finally {
      db.drop();
    }
  }

  // Issue #4107: reduce() over inline collect() at global scope
  @Test
  void reduceOverInlineCollectGlobal() {
    final Database db = newIsolatedDatabase("reduce-collect-global");
    try {
      seedThreePersons(db);
      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person) "
              + "RETURN reduce(total = 0, n IN collect(p.age) | total + n) AS total_age_sum");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("total_age_sum").longValue()).isEqualTo(90L);
    } finally {
      db.drop();
    }
  }

  private static void seedThreePersons(final Database db) {
    db.transaction(() -> {
      db.command("opencypher", "CREATE (:Person {name:'Alice', age:30})");
      db.command("opencypher", "CREATE (:Person {name:'Bob', age:25})");
      db.command("opencypher", "CREATE (:Person {name:'Charlie', age:35})");
    });
  }

  // Issue #4093: unknown escape sequence preserves the backslash literally
  @Test
  void unknownEscapePreservesBackslash() {
    final Database db = newIsolatedDatabase("backslash-unknown");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN 'Alice\\Bob' AS name");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice\\Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4093: backslash in property string round-trips through storage
  @Test
  void backslashInPropertyRoundTrips() {
    final Database db = newIsolatedDatabase("backslash-roundtrip");
    try {
      db.transaction(() -> db.command("opencypher", "CREATE (:Person {name: 'Alice\\Bob'})"));
      final ResultSet rs = db.query("opencypher", "MATCH (n:Person) RETURN n.name AS name");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice\\Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4093: backslash-containing property must not match unescaped form
  @Test
  void backslashInPropertyDoesNotMatchUnescapedForm() {
    final Database db = newIsolatedDatabase("backslash-noescape");
    try {
      db.transaction(() -> db.command("opencypher", "CREATE (:Person {name: 'Alice\\Bob'})"));
      final ResultSet rs = db.query("opencypher",
          "MATCH (n:Person) WHERE n.name = 'AliceBob' RETURN count(*) AS c");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(0L);
    } finally {
      db.drop();
    }
  }

  // Issue #4093: recognized escape sequences (e.g. \n) are still processed
  @Test
  void recognizedEscapeStillProcessed() {
    final Database db = newIsolatedDatabase("backslash-recognized");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN 'a\\nb' AS name");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("a\nb");
    } finally {
      db.drop();
    }
  }

  // Issue #4189: RETURN DISTINCT * preserves distinct rows by expanded variables
  @Test
  void returnDistinctStarKeepsDistinctRows() {
    final Database db = newIsolatedDatabase("distinct-star-keep");
    try {
      db.getSchema().createVertexType("Person");
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})"));
      final ResultSet rs = db.query("opencypher", "MATCH (p:Person) RETURN DISTINCT *");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex p = r.<Vertex>getProperty("p");
        assertThat(p).isNotNull();
        names.add(p.getString("name"));
      }
      assertThat(names).hasSize(2).containsExactlyInAnyOrder("Alice", "Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4189: RETURN DISTINCT * after a trivial WITH still preserves rows
  @Test
  void returnDistinctStarAfterTrivialWith() {
    final Database db = newIsolatedDatabase("distinct-star-with");
    try {
      db.getSchema().createVertexType("Person");
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})"));
      final ResultSet rs = db.query("opencypher", "MATCH (p:Person) WITH p RETURN DISTINCT *");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        final Vertex p = rs.next().<Vertex>getProperty("p");
        assertThat(p).isNotNull();
        names.add(p.getString("name"));
      }
      assertThat(names).hasSize(2).containsExactlyInAnyOrder("Alice", "Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4189: RETURN * (no DISTINCT) keeps all rows
  @Test
  void returnStarWithoutDistinctKeepsAllRows() {
    final Database db = newIsolatedDatabase("star-no-distinct");
    try {
      db.getSchema().createVertexType("Person");
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})"));
      final ResultSet rs = db.query("opencypher", "MATCH (p:Person) RETURN * ORDER BY p.name");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    } finally {
      db.drop();
    }
  }

  // Issue #4189: RETURN DISTINCT * still collapses actual duplicates
  @Test
  void returnDistinctStarStillCollapsesActualDuplicates() {
    final Database db = newIsolatedDatabase("distinct-star-dupes");
    try {
      final ResultSet rs = db.query("opencypher", "UNWIND [1,1,2] AS x RETURN DISTINCT *");
      final List<Long> values = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        values.add(r.<Long>getProperty("x"));
      }
      assertThat(values).hasSize(2).containsExactlyInAnyOrder(1L, 2L);
    } finally {
      db.drop();
    }
  }

  // Issue #3925: RETURN ALL is accepted as a no-op set-quantifier (GQL grammar)
  @Test
  void returnAllKeyword() {
    final Database db = newIsolatedDatabase("return-all-keyword");
    try {
      final ResultSet rs = db.query("opencypher", "RETURN ALL 1 AS result");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.getPropertyNames()).containsExactly("result");
      assertThat(r.<Long>getProperty("result")).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3925: RETURN ALL preserves duplicates (opposite of DISTINCT)
  @Test
  void returnAllMultipleRowsNoDedup() {
    final Database db = newIsolatedDatabase("return-all-nodedup");
    try {
      final ResultSet rs = db.query("opencypher",
          "UNWIND [1, 1, 2] AS x RETURN ALL x AS result");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
    } finally {
      db.drop();
    }
  }

  // Issue #4183: id() must return a numeric Long value (Neo4j-compatible)
  @Test
  void idFunctionReturnsNumericValue() {
    final Database db = seedIssue4183Persons("id-numeric");
    try {
      final ResultSet rs = db.query("opencypher",
          "MATCH (n:Person) RETURN id(n) AS ident ORDER BY n.name");
      assertThat(rs.hasNext()).isTrue();
      final Result first = rs.next();
      final Object firstId = first.getProperty("ident");
      assertThat(firstId).isInstanceOf(Number.class);

      assertThat(rs.hasNext()).isTrue();
      final Result second = rs.next();
      final Object secondId = second.getProperty("ident");
      assertThat(secondId).isInstanceOf(Number.class);

      assertThat(((Number) firstId).longValue()).isNotEqualTo(((Number) secondId).longValue());
    } finally {
      db.drop();
    }
  }

  // Issue #4183: numeric predicate id(n) >= 0 returns all rows (was 0 before fix)
  @Test
  void idNumericPredicateGreaterOrEqualZeroReturnsAllRows() {
    final Database db = seedIssue4183Persons("id-ge-zero");
    try {
      final ResultSet rs = db.query("opencypher",
          "MATCH (n:Person) WHERE id(n) >= 0 RETURN n.name AS name ORDER BY name");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
      assertThat(names).containsExactly("Alice", "Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4183: id() IS NOT NULL returns all rows
  @Test
  void idIsNotNullReturnsAllRows() {
    final Database db = seedIssue4183Persons("id-not-null");
    try {
      final ResultSet rs = db.query("opencypher",
          "MATCH (n:Person) WHERE id(n) IS NOT NULL RETURN n.name AS name ORDER BY name");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
      assertThat(names).containsExactly("Alice", "Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4183: id() combined with a string predicate
  @Test
  void idCombinedWithStringPredicate() {
    final Database db = seedIssue4183Persons("id-string-pred");
    try {
      final ResultSet rs = db.query("opencypher",
          "MATCH (n:Person) WHERE n.name > 'Alice' AND id(n) >= 0 RETURN n.name AS name ORDER BY name");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
      assertThat(names).containsExactly("Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4183: id() for relationships returns numeric Long
  @Test
  void idForRelationshipReturnsNumeric() {
    final Database db = seedIssue4183Persons("id-rel-numeric");
    try {
      db.transaction(() -> db.command("opencypher",
          "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)"));

      final ResultSet rs = db.query("opencypher",
          "MATCH ()-[r:KNOWS]->() RETURN id(r) AS ident");
      assertThat(rs.hasNext()).isTrue();
      final Object value = rs.next().getProperty("ident");
      assertThat(value).isInstanceOf(Number.class);
    } finally {
      db.drop();
    }
  }

  // Issue #4183: numeric predicate id(r) >= 0 for relationships
  @Test
  void idForRelationshipNumericPredicate() {
    final Database db = seedIssue4183Persons("id-rel-pred");
    try {
      db.transaction(() -> db.command("opencypher",
          "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)"));

      final ResultSet rs = db.query("opencypher",
          "MATCH ()-[r:KNOWS]->() WHERE id(r) >= 0 RETURN count(*) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(1L);
    } finally {
      db.drop();
    }
  }

  // Issue #4183: id() value can be passed back as a parameter to look up the vertex
  @Test
  void idCanBeUsedToLookupVertexAsParameter() {
    final Database db = seedIssue4183Persons("id-lookup");
    try {
      final ResultSet rs = db.query("opencypher",
          "MATCH (n:Person {name:'Alice'}) RETURN id(n) AS ident");
      final Object aliceId = rs.next().getProperty("ident");
      assertThat(aliceId).isInstanceOf(Number.class);

      final ResultSet rs2 = db.query("opencypher",
          "MATCH (n) WHERE id(n) = $id RETURN n.name AS name",
          Map.of("id", aliceId));
      assertThat(rs2.hasNext()).isTrue();
      assertThat(rs2.next().<String>getProperty("name")).isEqualTo("Alice");
      assertThat(rs2.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #4183: elementId() still returns the RID-string form
  @Test
  void elementIdStillReturnsStringRid() {
    final Database db = seedIssue4183Persons("elementid");
    try {
      final ResultSet rs = db.query("opencypher",
          "MATCH (n:Person {name:'Alice'}) RETURN elementId(n) AS ident");
      assertThat(rs.hasNext()).isTrue();
      final Object value = rs.next().getProperty("ident");
      assertThat(value).isInstanceOf(String.class);
      assertThat((String) value).startsWith("#");
    } finally {
      db.drop();
    }
  }

  // Issue #4183: id() round-tripped via List<Long> in an IN parameter matches all rows
  @Test
  void idInArrayParameterOfLongsMatchesAll() {
    final Database db = seedIssue4183Chunks("longs");
    try {
      final List<Long> ids = collectChunkIdsAsLong(db);
      assertThat(ids).hasSize(3);

      final List<String> texts = runChunkInQuery(db, ids);
      assertThat(texts).containsExactlyInAnyOrder("chunk1", "chunk2", "chunk3");
    } finally {
      db.drop();
    }
  }

  // Issue #4183: legacy List<String> of RID-strings still matches all rows in an IN parameter
  @Test
  void idInArrayParameterOfRidStringsMatchesAll() {
    final Database db = seedIssue4183Chunks("ridstrings");
    try {
      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = db.query("opencypher", "MATCH (n:CHUNK) RETURN elementId(n) AS id ORDER BY n.text")) {
        while (rs.hasNext())
          ids.add((String) rs.next().getProperty("id"));
      }
      assertThat(ids).hasSize(3);

      final List<String> texts = runChunkInQuery(db, ids);
      assertThat(texts).containsExactlyInAnyOrder("chunk1", "chunk2", "chunk3");
    } finally {
      db.drop();
    }
  }

  // Issue #4183: numeric-string forms must NOT match (Cypher TCK: 5 IN ["5"] is false)
  @Test
  void idInArrayParameterOfNumericStringsMatchesNothing() {
    final Database db = seedIssue4183Chunks("numstrings");
    try {
      final List<String> ids = new ArrayList<>();
      for (final Long v : collectChunkIdsAsLong(db))
        ids.add(Long.toString(v));
      assertThat(ids).hasSize(3);

      final List<String> texts = runChunkInQuery(db, ids);
      assertThat(texts).isEmpty();
    } finally {
      db.drop();
    }
  }

  private Database seedIssue4183Persons(final String tag) {
    final Database db = newIsolatedDatabase("4183-" + tag);
    db.getSchema().createVertexType("Person");
    db.getSchema().createEdgeType("KNOWS");
    db.transaction(() -> {
      db.command("opencypher", "CREATE (:Person {name:'Alice'})");
      db.command("opencypher", "CREATE (:Person {name:'Bob'})");
    });
    return db;
  }

  private Database seedIssue4183Chunks(final String tag) {
    final Database db = newIsolatedDatabase("4183-chunks-" + tag);
    db.getSchema().createVertexType("CHUNK");
    db.transaction(() -> {
      db.command("opencypher", "CREATE (n:CHUNK {text: 'chunk1'})");
      db.command("opencypher", "CREATE (n:CHUNK {text: 'chunk2'})");
      db.command("opencypher", "CREATE (n:CHUNK {text: 'chunk3'})");
    });
    return db;
  }

  private static List<Long> collectChunkIdsAsLong(final Database db) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = db.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id ORDER BY n.text")) {
      while (rs.hasNext())
        ids.add(((Number) rs.next().getProperty("id")).longValue());
    }
    return ids;
  }

  private static List<String> runChunkInQuery(final Database db, final List<?> ids) {
    final List<String> texts = new ArrayList<>();
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text AS text",
        Map.of("ids", ids))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        texts.add(r.getProperty("text"));
      }
    }
    return texts;
  }
}
