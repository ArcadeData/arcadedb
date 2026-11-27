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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A pattern comprehension whose start node no outer variable binds walked the pattern from every vertex of its label, also when
 * its {@code WHERE} pinned that node to an indexed property ({@code a.id = startNode(r).id}, LDBC Interactive IC14), and the walk
 * never noticed {@code arcadedb.command.timeout}: it runs inside one row, where no step checks the deadline (issue #13622).
 * <p>
 * The start is now sought through the index, as the MATCH spelling is. Whether a candidate was visited is observed with a
 * "poison" person whose inline {@code WHERE} raises a division by zero: a scan reaches it and fails, a seek of other ids never
 * does. Each seek test is paired with the shape that cannot be sought, which still fails on it, so the probe can fail.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherPatternComprehensionUncorrelatedStartIssue13622Test extends TestHelper {
  /** The IC14 weight of a relationship r, as the LDBC Interactive implementation writes it. */
  private static final String IC14_WEIGHT = """
      [(a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b:Person)
       WHERE (a.id = startNode(r).id AND b.id = endNode(r).id) OR (a.id = endNode(r).id AND b.id = startNode(r).id)
       | 1.0]""";

  private static final String WEIGHTS = "MATCH (p:Person)-[r:KNOWS]->(q:Person) RETURN p.id AS p, q.id AS q, size(" + IC14_WEIGHT
      + ") AS w ORDER BY p, q";

  /** The same weights with MATCH: the reference the comprehension must agree with. */
  private static final String WEIGHTS_BY_MATCH = """
      MATCH (p:Person)-[r:KNOWS]->(q:Person)
      OPTIONAL MATCH (a:Person)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b:Person)
      WHERE (a.id = p.id AND b.id = q.id) OR (a.id = q.id AND b.id = p.id)
      RETURN p.id AS p, q.id AS q, count(c) AS w ORDER BY p, q""";

  /** Raises "/ by zero" for the poison person, and is true for every other one. */
  private static final String POISON_CHECK = "(a:Person WHERE 10 / a.d > 0)";

  @Override
  protected void beginTest() {
    final VertexType person = database.getSchema().createVertexType("Person");
    person.createProperty("id", Long.class);
    person.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    database.getSchema().createVertexType("Comment");
    database.getSchema().createVertexType("Post");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("HAS_CREATOR");
    database.getSchema().createEdgeType("REPLY_OF");

    database.transaction(() -> {
      final List<MutableVertex> persons = new ArrayList<>();
      for (long i = 1; i <= 6; i++)
        persons.add(database.newVertex("Person").set("id", i).set("d", 1).save());
      // Reached by nothing the tests seek: only a scan of :Person meets it
      database.newVertex("Person").set("id", 99L).set("d", 0).save();

      for (int i = 0; i < persons.size(); i++)
        for (int j = 1; j <= 2; j++)
          persons.get(i).newEdge("KNOWS", persons.get((i + j) % persons.size())).save();

      // Posts by persons i % 6, comments by person (i * 5) % 6 replying to post i % 4: several directions and counts per pair
      final List<MutableVertex> posts = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        final MutableVertex post = database.newVertex("Post").set("id", (long) i).save();
        post.newEdge("HAS_CREATOR", persons.get(i % persons.size())).save();
        posts.add(post);
      }
      for (int i = 0; i < 24; i++) {
        final MutableVertex comment = database.newVertex("Comment").set("id", (long) i).save();
        comment.newEdge("HAS_CREATOR", persons.get((i * 5) % persons.size())).save();
        comment.newEdge("REPLY_OF", posts.get(i % posts.size())).save();
      }
    });
  }

  @Test
  void ic14WeightsAgreeWithTheMatchSpellingWithAndWithoutTheIndex() {
    final List<Map<String, Object>> expected = rows(WEIGHTS_BY_MATCH);
    assertThat(expected).hasSize(12);
    assertThat(expected).anySatisfy(row -> assertThat(((Number) row.get("w")).longValue()).isGreaterThan(0L));

    assertThat(weights(rows(WEIGHTS))).isEqualTo(weights(expected));

    // The scan the comprehension falls back to without an index answers the same
    dropPersonIdIndex();
    assertThat(weights(rows(WEIGHTS))).isEqualTo(weights(expected));
  }

  @Test
  void anEqualityOnTheIndexedPropertySeeksTheStart() {
    // The poison person is never visited: a scan would raise the division by zero
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = 2 | b.id] AS l")).containsExactlyInAnyOrder(3L, 4L);
    // Either side of =, and a value read from the outer row
    assertThat(list("WITH 3 AS x RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE x = a.id | b.id] AS l"))
        .containsExactlyInAnyOrder(4L, 5L);
    // The probe can fail: without the index the same comprehension scans and meets the poison person
    dropPersonIdIndex();
    assertThatThrownBy(() -> list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = 2 | b.id] AS l"))
        .hasStackTraceContaining("/ by zero");
  }

  @Test
  void eachBranchOfAnOrPinningTheSamePropertyIsSought() {
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE (a.id = 1 AND b.id = 2) OR (a.id = 5 AND b.id = 1) | b.id] AS l"))
        .containsExactlyInAnyOrder(2L, 1L);
    // A branch that does not pin a.id leaves every person a candidate: the scan answers, and meets the poison person
    assertThatThrownBy(() -> list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = 1 OR b.id = 2 | b.id] AS l"))
        .hasStackTraceContaining("/ by zero");
  }

  @Test
  void aValueReadingAVariableOfThePatternIsNotSought() {
    // b is bound by the pattern, so b.id is not known before the walk: the start is scanned
    assertThatThrownBy(() -> list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = b.id - 1 | b.id] AS l"))
        .hasStackTraceContaining("/ by zero");
  }

  @Test
  void anInListAndAnInlinePropertyAreSought() {
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id IN [1, 6] | b.id] AS l"))
        .containsExactlyInAnyOrder(2L, 3L, 1L, 2L);
    assertThat(list("WITH [2, null, 2] AS ids RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id IN ids | b.id] AS l"))
        .containsExactlyInAnyOrder(3L, 4L);
    assertThat(list("RETURN [(a:Person {id: 4} WHERE 10 / a.d > 0)-[:KNOWS]->(b) | b.id] AS l")).containsExactlyInAnyOrder(5L, 6L);
    // A null value equals nothing: nothing is walked and nothing matches
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = null | b.id] AS l")).isEmpty();
  }

  @Test
  void anAndPinningTheSamePropertyTwiceSeeksAndKeepsBothPredicates() {
    // Both sides pin a.id: the seek takes the fewer values, every predicate is still evaluated on each candidate
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id IN [1, 2, 3] AND a.id = 2 | b.id] AS l"))
        .containsExactlyInAnyOrder(3L, 4L);
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = 2 AND a.id IN [1, 2, 3] | b.id] AS l"))
        .containsExactlyInAnyOrder(3L, 4L);
    // Disjoint pins: the seek yields candidates, the predicates reject them all
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = 2 AND a.id = 3 | b.id] AS l")).isEmpty();
    // An inline pin and a WHERE pin on the same property
    assertThat(list("RETURN [(a:Person {id: 2} WHERE 10 / a.d > 0)-[:KNOWS]->(b) WHERE a.id IN [2, 3] | b.id] AS l"))
        .containsExactlyInAnyOrder(3L, 4L);
  }

  @Test
  void aNegatedInequalityIsSoughtAsTheEquality() {
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE NOT (a.id <> 2) | b.id] AS l")).containsExactlyInAnyOrder(3L, 4L);
  }

  @Test
  void aPinOnAPropertyWithNoIndexIsSkippedForTheIndexedOne() {
    // a.d has no index: the seek uses a.id
    assertThat(list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.d = 1 AND a.id = 2 | b.id] AS l"))
        .containsExactlyInAnyOrder(3L, 4L);
    // Only a.d is pinned: nothing to seek with, the scan meets the poison person
    assertThatThrownBy(() -> list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.d = 1 | b.id] AS l"))
        .hasStackTraceContaining("/ by zero");
  }

  @Test
  void shapesThatPinNothingExactlyAreScanned() {
    // XOR pins nothing
    assertThatThrownBy(() -> list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = 2 XOR b.id = 3 | b.id] AS l"))
        .hasStackTraceContaining("/ by zero");
    // An IN element that reads a variable of the pattern
    assertThatThrownBy(() -> list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id IN [2, b.id] | b.id] AS l"))
        .hasStackTraceContaining("/ by zero");
    // A value that can change between two evaluations
    assertThatThrownBy(() -> list("RETURN [" + POISON_CHECK + "-[:KNOWS]->(b) WHERE a.id = toInteger(rand() * 0) + 2 | b.id] AS l"))
        .hasStackTraceContaining("/ by zero");
    // The same inline: the inline map rejects the poison person before its WHERE runs, so only the answer can be checked
    assertThat(list("RETURN [(a:Person {id: toInteger(rand() * 0) + 2})-[:KNOWS]->(b) | b.id] AS l")).containsExactlyInAnyOrder(3L, 4L);
  }

  @Test
  void anUnknownLabelAnswersNoMatch() {
    assertThat(list("RETURN [(a:Nobody)-[:KNOWS]->(b) WHERE a.id = 2 | b.id] AS l")).isEmpty();
  }

  @Test
  void aSubtypeIsSoughtThroughTheIndexOfItsSupertype() {
    database.getSchema().createVertexType("Student").addSuperType("Person");
    database.transaction(() -> {
      final MutableVertex student = database.newVertex("Student").set("id", 7L).set("d", 1).save();
      student.newEdge("KNOWS", database.query("opencypher", "MATCH (p:Person {id: 1}) RETURN p").next().getVertex().get()).save();
    });
    // The index of Person holds the persons 2 and 7: only the Student is a candidate
    assertThat(list("RETURN [(a:Student)-[:KNOWS]->(b) WHERE a.id IN [2, 7] | b.id] AS l")).containsExactly(1L);
  }

  @Test
  void theWalkOfAComprehensionStopsAtTheCommandTimeout() {
    // Persons with no index on x, 30 KNOWS each: the 4-hop walk from every one of them is ~800M expansions
    database.getSchema().createVertexType("Walker");
    database.transaction(() -> {
      final List<MutableVertex> walkers = new ArrayList<>();
      for (int i = 0; i < 1000; i++)
        walkers.add(database.newVertex("Walker").set("x", i).save());
      for (int i = 0; i < walkers.size(); i++)
        for (int j = 1; j <= 30; j++)
          walkers.get(i).newEdge("KNOWS", walkers.get((i * 31 + j * 17) % walkers.size())).save();
    });

    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 500L);
    try {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> list(
          "RETURN [(a:Walker)-[:KNOWS]->()-[:KNOWS]->()-[:KNOWS]->()-[:KNOWS]->(b:Walker) WHERE a.x = b.x + 1000 | 1] AS l"))
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      stopwatch.assertGaveUpWithin(30_000L, "a comprehension walk aborted from inside, not run to the end");
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, GlobalConfiguration.COMMAND_TIMEOUT.getDefValue());
    }
  }

  private void dropPersonIdIndex() {
    for (final TypeIndex index : database.getSchema().getType("Person").getAllIndexes(false))
      database.getSchema().dropIndex(index.getName());
  }

  private List<Object> list(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final List<Object> values = rs.next().getProperty("l");
      final List<Object> normalized = new ArrayList<>(values.size());
      for (final Object value : values)
        normalized.add(value instanceof Number n ? (Object) n.longValue() : value);
      return normalized;
    }
  }

  private List<Map<String, Object>> rows(final String query) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.toMap());
      }
    }
    return rows;
  }

  private static List<String> weights(final List<Map<String, Object>> rows) {
    final List<String> weights = new ArrayList<>(rows.size());
    for (final Map<String, Object> row : rows)
      weights.add(row.get("p") + "-" + row.get("q") + ":" + ((Number) row.get("w")).longValue());
    return weights;
  }
}
