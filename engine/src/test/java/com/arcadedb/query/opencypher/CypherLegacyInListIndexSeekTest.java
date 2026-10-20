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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code WHERE n.prop IN <list>} on an indexed property seeks the index on the legacy MATCH path too, which every query
 * with an OPTIONAL MATCH takes, instead of scanning the type and comparing each record with every element. Each query is
 * checked against the same predicate on {@code idCopy}, an unindexed copy of {@code id}, so the scan defines the answer.
 * <p>
 * The rows are compared as they come back, never sorted first (issue #10392): a comparison that sorts cannot see a difference of
 * order between the two plans, for any input. Without an ORDER BY the order is not part of the contract, and the seek emits the
 * keys in the order of the list while the scan emits the records in storage order, so most cases compare the rows in any order
 * and say so; what IS pinned is what a client can rely on - the same rows, a LIMIT taking rows of the answer, pages that
 * partition it, and one order under an ORDER BY.
 */
class CypherLegacyInListIndexSeekTest extends TestHelper {
  private static final String OPTIONAL_TAIL = " OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) RETURN p.id AS id";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var person = database.getSchema().createVertexType("Person");
      person.createProperty("id", Integer.class);
      person.createProperty("idCopy", Integer.class);
      person.createProperty("code", String.class);
      person.createProperty("codeCopy", String.class);
      person.createProperty("name", String.class);
      person.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      person.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "code");
      database.getSchema().createEdgeType("KNOWS");
    });

    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.command("opencypher",
            "CREATE (p:Person {id: $i, idCopy: $i, code: $c, codeCopy: $c, name: $n})",
            Map.of("i", i, "c", "c" + (i % 10), "n", "Person" + i));
      database.command("opencypher", "MATCH (a:Person), (b:Person) WHERE b.id = a.id + 1 CREATE (a)-[:KNOWS]->(b)");
    });
  }

  @Test
  void parameterListSeeksTheIndexAndAnswersAsTheScan() {
    final Map<String, Object> params = new HashMap<>();
    params.put("ids", Arrays.asList(3, 7, 7, 42, null, 999));

    final String seek = "MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL;
    final String scan = "MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL;

    assertThat(ids(seek, params)).containsExactlyInAnyOrder(3, 7, 42).containsExactlyInAnyOrderElementsOf(ids(scan, params));
    assertThat(plan(seek, params)).contains("[index: Person[id]]");
    assertThat(plan(scan, params)).doesNotContain("[index:");
  }

  @Test
  void listLiteralSeeksTheIndex() {
    final String seek = "MATCH (p:Person) WHERE p.id IN [1, 2, 3]" + OPTIONAL_TAIL;

    assertThat(ids(seek, null)).containsExactlyInAnyOrder(1, 2, 3);
    assertThat(plan(seek, null)).contains("[index: Person[id]]");
  }

  @Test
  void emptyOrAllNullListMatchesNothing() {
    final Map<String, Object> params = new HashMap<>();
    params.put("ids", Collections.singletonList(null));

    assertThat(ids("MATCH (p:Person) WHERE p.id IN []" + OPTIONAL_TAIL, null)).isEmpty();
    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL, params)).isEmpty();
  }

  @Test
  void otherConjunctsStillFilterTheSeek() {
    final Map<String, Object> params = Map.of("ids", List.of(2, 3, 4));

    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids AND p.name <> 'Person3'" + OPTIONAL_TAIL, params))
        .containsExactlyInAnyOrder(2, 4);
  }

  @Test
  void disjunctionIsNotSought() {
    final String query = "MATCH (p:Person) WHERE p.id IN [1, 2] OR p.name = 'Person5'" + OPTIONAL_TAIL;

    assertThat(ids(query, null)).containsExactlyInAnyOrder(1, 2, 5);
    assertThat(plan(query, null)).doesNotContain("[index:");
  }

  @Test
  void negatedInIsNotSought() {
    final String query = "MATCH (p:Person) WHERE NOT p.id IN [1, 2] AND p.id < 5" + OPTIONAL_TAIL;

    assertThat(ids(query, null)).containsExactlyInAnyOrder(0, 3, 4);
    assertThat(plan(query, null)).doesNotContain("[index:");
  }

  @Test
  void rowDependentListIsSoughtPerRow() {
    final String seek = "UNWIND [[1, 2], [3], [2, 4], []] AS ids MATCH (p:Person) WHERE p.id IN ids" + OPTIONAL_TAIL;
    final String scan = "UNWIND [[1, 2], [3], [2, 4], []] AS ids MATCH (p:Person) WHERE p.idCopy IN ids" + OPTIONAL_TAIL;

    assertThat(ids(seek, null)).containsExactlyInAnyOrder(1, 2, 2, 3, 4).containsExactlyInAnyOrderElementsOf(ids(scan, null));
  }

  @Test
  void textAgainstIntegerAnswersAsTheScan() {
    final Map<String, Object> params = Map.of("ids", List.of("5", 6, "x"));

    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL, params))
        .containsExactlyInAnyOrderElementsOf(ids("MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL, params));
  }

  @Test
  void nonUniqueIndexReturnsEveryRecordOfEveryKey() {
    final Map<String, Object> params = Map.of("codes", List.of("c1", "c2", "c1"));
    final String seek = "MATCH (p:Person) WHERE p.code IN $codes" + OPTIONAL_TAIL;

    assertThat(ids(seek, params)).hasSize(10)
        .containsExactlyInAnyOrderElementsOf(ids("MATCH (p:Person) WHERE p.codeCopy IN $codes" + OPTIONAL_TAIL, params));
    assertThat(plan(seek, params)).contains("[index: Person[code]]");
  }

  @Test
  void moreEntriesThanOneConsistentReadStreamKeyByKey() {
    database.transaction(() -> {
      for (int i = 1000; i < 1300; i++)
        database.command("opencypher", "CREATE (p:Person {id: $i, idCopy: $i, code: 'bulk', codeCopy: 'bulk'})", Map.of("i", i));
    });
    final Map<String, Object> params = Map.of("codes", List.of("bulk", "c1", "bulk"));
    final String seek = "MATCH (p:Person) WHERE p.code IN $codes" + OPTIONAL_TAIL;

    assertThat(ids(seek, params)).hasSize(305)
        .containsExactlyInAnyOrderElementsOf(ids("MATCH (p:Person) WHERE p.codeCopy IN $codes" + OPTIONAL_TAIL, params));
    assertThat(plan(seek, params)).contains("[index: Person[code]]");
    assertThat(ids(seek + " LIMIT 3", params)).hasSize(3);
  }

  @Test
  void aListNotInSortedOrderAnswersTheSameRowsAsTheScan() {
    final Map<String, Object> params = Map.of("ids", List.of(42, 3, 7));
    final String seek = "MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL;
    final String scan = "MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL;

    assertThat(ids(seek, params)).containsExactlyInAnyOrder(3, 7, 42).containsExactlyInAnyOrderElementsOf(ids(scan, params));
    assertThat(plan(seek, params)).contains("[index: Person[id]]");
  }

  /**
   * The seek answers key by key in the order of the list, each key once at its first place, where the scan answers in storage order
   * (issue #10392). Neither is a promise to a query without an ORDER BY; the seek's is pinned because a client that pages such a
   * query depends on it being the same on every run, and a change of it should be a decision.
   */
  @Test
  void theSeekAnswersKeyByKeyInTheOrderOfTheList() {
    final Map<String, Object> params = Map.of("ids", List.of(42, 3, 42, 7));

    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL, params)).containsExactly(42, 3, 7);
    assertThat(ids("MATCH (p:Person) WHERE p.id IN [42, 3, 7]" + OPTIONAL_TAIL, null)).containsExactly(42, 3, 7);
  }

  @Test
  void aLimitTakesRowsOfTheAnswerOnBothPlans() {
    final Map<String, Object> params = Map.of("ids", List.of(42, 3, 7));

    for (final String property : new String[] { "id", "idCopy" })
      assertThat(ids("MATCH (p:Person) WHERE p." + property + " IN $ids" + OPTIONAL_TAIL + " LIMIT 2", params))
          .as(property).hasSize(2).isSubsetOf(3, 7, 42).doesNotHaveDuplicates();
  }

  @Test
  void pagesOfTheSeekPartitionTheAnswer() {
    final Map<String, Object> params = Map.of("ids", List.of(42, 3, 7, 11, 25));

    for (final String property : new String[] { "id", "idCopy" }) {
      final List<Integer> paged = new ArrayList<>();
      for (int skip = 0; skip < 6; skip += 2)
        paged.addAll(ids("MATCH (p:Person) WHERE p." + property + " IN $ids" + OPTIONAL_TAIL + " SKIP " + skip + " LIMIT 2", params));

      assertThat(paged).as(property).containsExactlyInAnyOrder(3, 7, 11, 25, 42);
    }
  }

  @Test
  void underAnOrderByTheSeekAndTheScanAnswerInTheSameOrder() {
    final Map<String, Object> params = Map.of("ids", List.of(42, 3, 7, 11));
    final String order = " ORDER BY id";

    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL + order, params)).containsExactly(3, 7, 11, 42);
    assertThat(ids("MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL + order, params)).containsExactly(3, 7, 11, 42);
    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL + order + " LIMIT 2", params)).containsExactly(3, 7);
    assertThat(ids("MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL + order + " LIMIT 2", params)).containsExactly(3, 7);
  }

  @Test
  void singleElementListSeeksTheIndex() {
    final String seek = "MATCH (p:Person) WHERE p.id IN [5]" + OPTIONAL_TAIL;

    assertThat(ids(seek, null)).containsExactlyInAnyOrder(5);
    assertThat(plan(seek, null)).contains("[index: Person[id]]");
  }

  @Test
  void elementsOfOtherTypesAnswerAsTheScan() {
    final Map<String, Object> params = new HashMap<>();
    params.put("ids", Arrays.asList(true, 3.0, 4.5, "7", "7.0", LocalDate.of(2020, 1, 1), 8L, List.of(9), Map.of("a", 1)));

    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL, params))
        .containsExactlyInAnyOrderElementsOf(ids("MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL, params));
    params.put("ids", Arrays.asList(true, 3.0, 4.5, "7", "7.0", LocalDate.of(2020, 1, 1), 8L));
    assertThat(ids("MATCH (p:Person) WHERE p.id IN $ids" + OPTIONAL_TAIL, params))
        .containsExactlyInAnyOrderElementsOf(ids("MATCH (p:Person) WHERE p.idCopy IN $ids" + OPTIONAL_TAIL, params));
    params.put("ids", Arrays.asList(1, 2.0, "3"));
    assertThat(ids("MATCH (p:Person) WHERE p.code IN $ids" + OPTIONAL_TAIL, params))
        .containsExactlyInAnyOrderElementsOf(ids("MATCH (p:Person) WHERE p.codeCopy IN $ids" + OPTIONAL_TAIL, params));
  }

  @Test
  void indexDeclaredOnASupertypeAnswersOnlyTheLabel() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Employee", 1).addSuperType("Person");
      database.command("opencypher", "CREATE (:Employee {id: 100, idCopy: 100})");
    });

    assertThat(ids("MATCH (p:Employee) WHERE p.id IN [3, 100]" + OPTIONAL_TAIL, null)).containsExactlyInAnyOrder(100);
    assertThat(ids("MATCH (p:Person) WHERE p.id IN [3, 100]" + OPTIONAL_TAIL, null)).containsExactlyInAnyOrder(3, 100);
  }

  @Test
  void listElementReadingTheMatchedNodeItselfIsNotDroppedAsNull() {
    // an element only the candidate can answer is no null: the seek has no value for it, so the scan and the filter answer
    assertThat(ids("MATCH (p:Person) WHERE p.id IN [1, p.idCopy]" + OPTIONAL_TAIL, null)).hasSize(50);
    assertThat(ids("MATCH (p:Person) WHERE p.id IN [p.idCopy]" + OPTIONAL_TAIL, null)).hasSize(50);
    assertThat(ids("MATCH (p:Person) WHERE p.id IN [p.idCopy + 1, 2]" + OPTIONAL_TAIL, null)).containsExactlyInAnyOrder(2);
    assertThat(ids("MATCH (p:Person) WHERE p.id IN [size(p.name) - 7]" + OPTIONAL_TAIL, null)).containsExactlyInAnyOrder(0);
    assertThat(plan("MATCH (p:Person) WHERE p.id IN [1, p.idCopy]" + OPTIONAL_TAIL, null)).doesNotContain("[index:");
  }

  @Test
  void listElementReadingTheMatchedNodeOfAChainedMatchIsNotDropped() {
    final String query = "UNWIND [1, 2] AS k MATCH (p:Person) WHERE p.id IN [k, p.idCopy + 100]" + OPTIONAL_TAIL;

    assertThat(ids(query, null)).containsExactlyInAnyOrder(1, 2);
    assertThat(ids("UNWIND [1] AS k MATCH (p:Person) WHERE p.id IN [p.idCopy, k]" + OPTIONAL_TAIL, null)).hasSize(50);
  }

  @Test
  void fixedListOfAChainedMatchGivesEveryOuterRowTheSameCandidates() {
    final Map<String, Object> params = Map.of("codes", List.of("c1", "c2"));
    final String seek = "UNWIND [1, 2, 3] AS k MATCH (p:Person) WHERE p.code IN $codes" + OPTIONAL_TAIL;

    assertThat(ids(seek, params)).hasSize(30)
        .containsExactlyInAnyOrderElementsOf(ids("UNWIND [1, 2, 3] AS k MATCH (p:Person) WHERE p.codeCopy IN $codes" + OPTIONAL_TAIL, params));
    assertThat(plan(seek, params)).contains("[index: Person[code]]");
  }

  @Test
  void theListWithTheFewestElementsIsSought() {
    final Map<String, Object> params = Map.of("codes", List.of("c1", "c2", "c3", "c4"));
    final String seek = "MATCH (p:Person) WHERE p.code IN $codes AND p.id IN [3, 4, 99]" + OPTIONAL_TAIL;

    assertThat(ids(seek, params)).containsExactlyInAnyOrder(3, 4);
    assertThat(plan(seek, params)).contains("[index: Person[id]]");
  }

  /**
   * A seek over more than {@code ConsistentKeyLookup.MAX_ENTRIES} entries remembers the RIDs it returned and charges them to the heap
   * budget all queries share. A LIMIT, or a client that closes the stream, stops the seek before its last key: closing the step gives
   * the charge back instead of leaving it to the garbage collector.
   */
  @Test
  void abandonedSeekGivesItsHeapChargeBack() {
    final int total = 30_000;
    database.transaction(() -> {
      final var big = database.getSchema().createVertexType("Big");
      big.createProperty("k", Integer.class);
      big.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
      for (int i = 0; i < total; i++)
        database.newVertex("Big").set("k", i).save();
    });
    final List<Integer> keys = new ArrayList<>(total);
    for (int i = 0; i < total; i++)
      keys.add(i);

    final long baseline = QueryHeapBudget.getReservedBytes();
    final String query = "MATCH (b:Big) WHERE b.k IN $keys OPTIONAL MATCH (b)-[:KNOWS]->(q) RETURN b.k AS id";
    assertThat(plan(query + " LIMIT 1", Map.of("keys", keys))).contains("[index: Big[k]]");

    final ResultSet rs = database.query("opencypher", query, Map.of("keys", keys));
    try {
      for (int i = 0; i < 25_000; i++)
        rs.next();
      assertThat(QueryHeapBudget.getReservedBytes()).as("charged while the seek is open").isGreaterThan(baseline);
    } finally {
      rs.close();
    }
    assertThat(QueryHeapBudget.getReservedBytes()).as("given back when the stream is closed").isLessThanOrEqualTo(baseline);
  }

  /**
   * A chained MATCH re-opens its scan once per outer row, and caches the candidates of a scan no row can steer (issue #6602, #8695). An
   * IN-list is only seeked per row when an index answers it, so one over an unindexed property keeps the cache even when its list reads
   * the outer row, and the type is scanned once rather than once per row.
   */
  @Test
  void rowDependentListOverAnUnindexedPropertyScansTheTypeOnce() {
    final String unindexed = "UNWIND ['c1', 'c2', 'c3', 'c4'] AS a MATCH (p:Person) WHERE p.codeCopy IN [a]" + OPTIONAL_TAIL;
    final String indexed = "UNWIND ['c1', 'c2', 'c3', 'c4'] AS a MATCH (p:Person) WHERE p.code IN [a]" + OPTIONAL_TAIL;

    final long before = typeScans();
    assertThat(ids(unindexed, null)).hasSize(20).containsExactlyInAnyOrderElementsOf(ids(indexed, null));
    final long scans = typeScans() - before;
    assertThat(scans).as("scans of the type for the unindexed list and the indexed one, which seeks and scans none").isEqualTo(1L);
  }

  private long typeScans() {
    return ((Number) ((DatabaseInternal) database).getStats().get("iterateType")).longValue();
  }

  /** The ids in the order the query returned them: sorting here would hide the very difference of order between plans (issue #10392). */
  private List<Integer> ids(final String cypher, final Map<String, Object> params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = params != null ? database.query("opencypher", cypher, params) : database.query("opencypher", cypher)) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    return ids;
  }

  private String plan(final String cypher, final Map<String, Object> params) {
    try (final ResultSet rs = params != null ?
        database.query("opencypher", "PROFILE " + cypher, params) :
        database.query("opencypher", "PROFILE " + cypher)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
