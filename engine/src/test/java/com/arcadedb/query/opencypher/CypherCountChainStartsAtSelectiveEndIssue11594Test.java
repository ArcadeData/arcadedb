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
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11594: the chain count push-down ({@code COUNT CHAIN PATHS}) always enumerated the label of the
 * chain's first position, so {@code MATCH (c:Chunk)-[:OF_RECORD]->(r:Record {external_id: $id}) RETURN count(c)} walked
 * every chunk of the database to keep the ones of one record. An uncorrelated chain now starts from its far end when that
 * end carries the only node predicate, or, with predicates on neither or both ends, when its label holds fewer vertices.
 * The count is the same read either way; each one is checked against the row pipeline, reached through a {@code sum(1)}
 * that the push-downs decline. An end pinned by a UNIQUE key is not walked at all: the vertex is looked up when the count runs
 * and the chain walked from it alone.
 */
class CypherCountChainStartsAtSelectiveEndIssue11594Test extends TestHelper {

  private static final int RECORDS = 20;
  private static final int CHUNKS  = 400;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Record");
    database.command("sql", "CREATE PROPERTY Record.external_id STRING");
    database.command("sql", "CREATE INDEX ON Record (external_id) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Chunk");
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE EDGE TYPE OF_RECORD");
    database.command("sql", "CREATE EDGE TYPE MENTIONS");
    database.transaction(() -> {
      final List<MutableVertex> records = new ArrayList<>();
      for (int i = 0; i < RECORDS; i++)
        records.add(database.newVertex("Record").set("external_id", "r" + i).save());
      final List<MutableVertex> entities = new ArrayList<>();
      for (int i = 0; i < 5; i++)
        entities.add(database.newVertex("Entity").set("id", i).save());
      for (int i = 0; i < CHUNKS; i++) {
        final MutableVertex chunk = database.newVertex("Chunk").set("i", i).save();
        chunk.newEdge("OF_RECORD", records.get(i % RECORDS));
        if (i % 3 == 0)
          chunk.newEdge("MENTIONS", entities.get(i % 5));
        // a second, parallel mention: two MENTIONS hops can then come back to the same chunk over two edges, which only the
        // inequality of aFlippedChainKeepsItsInequality rules out
        if (i % 9 == 0)
          chunk.newEdge("MENTIONS", entities.get(i % 5));
      }
    });
  }

  @Test
  void aChunkFirstCountStartsFromTheRecord() {
    final String[] queries = {
        "MATCH (c:Chunk)-[:OF_RECORD]->(r:Record {external_id: 'r7'}) RETURN count(c) AS n",
        "MATCH (c:Chunk)-[:OF_RECORD]->(r:Record) WHERE r.external_id = 'r7' RETURN count(*) AS n",
        "MATCH (r:Record {external_id: 'r7'})<-[:OF_RECORD]-(c:Chunk) RETURN count(c) AS n" };
    for (final String query : queries) {
      final String plan = plan(query);
      assertThat(plan).as(query).contains("COUNT CHAIN PATHS");
      // the record is looked up by its key, not found by walking the label: the count costs the seek and its own chunks
      assertThat(plan).as(query).contains("seeded anchor: unique key Record[external_id] = [r7]");
      assertThat(chainLine(plan)).as(query).startsWith("(Record");
      assertThat(count(query)).as(query).isEqualTo(CHUNKS / RECORDS);
    }
  }

  @Test
  void countsMatchTheRowPipelineInEitherOrientation() {
    final String[] patterns = {
        "(c:Chunk)-[:OF_RECORD]->(r:Record {external_id: 'r3'})",
        "(e:Entity)<-[:MENTIONS]-(c:Chunk)-[:OF_RECORD]->(r:Record {external_id: 'r3'})",
        "(r:Record {external_id: 'r3'})<-[:OF_RECORD]-(c:Chunk)-[:MENTIONS]->(e:Entity)",
        "(c:Chunk {i: 6})-[:OF_RECORD]->(r:Record)",
        "(c:Chunk)-[:OF_RECORD]->(r:Record)",
        "(c:Chunk)-[:MENTIONS]->(e:Entity)",
        "(e:Entity {id: 1})<-[:MENTIONS]-(c:Chunk)-[:OF_RECORD]->(r:Record)" };
    for (final String pattern : patterns) {
      final String counted = "MATCH " + pattern + " RETURN count(*) AS n";
      final String oracle = "MATCH " + pattern + " RETURN sum(1) AS n";
      assertThat(plan(counted)).as(counted).contains("COUNT CHAIN PATHS");
      assertThat(count(counted)).as(counted).isEqualTo(count(oracle));
    }
  }

  @Test
  void anEndWithTheOnlyPredicateIsTheStartWhateverItsSize() {
    // The predicate is on the large label: the chain starts there and filters, rather than enumerating every record and
    // walking all of their chunks
    final String query = "MATCH (c:Chunk {i: 6})-[:OF_RECORD]->(r:Record) RETURN count(*) AS n";
    assertThat(chainLine(plan(query))).startsWith("(Chunk");
    // Neither end has one: the smaller label starts
    assertThat(chainLine(plan("MATCH (c:Chunk)-[:OF_RECORD]->(r:Record) RETURN count(*) AS n"))).startsWith("(Record");
  }

  /**
   * A flipped chain renumbers the positions of its node inequality. The two MENTIONS hops can bind one edge only when c1 and c2
   * are one chunk, which the inequality rules out, so the push-down keeps the chain, now walked from Record; and a chunk with
   * two parallel mentions reaches itself over two edges, so an inequality left on the wrong positions changes the count.
   */
  @Test
  void aFlippedChainKeepsItsInequality() {
    final String[] patterns = {
        "(c2:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(c1:Chunk)-[:OF_RECORD]->(r:Record) WHERE c1 <> c2",
        "(c:Chunk)-[:MENTIONS]->(e:Entity) WHERE c <> e" };
    for (final String pattern : patterns) {
      final String counted = "MATCH " + pattern + " RETURN count(*) AS n";
      final String oracle = "MATCH " + pattern + " RETURN sum(1) AS n";
      final String plan = plan(counted);
      assertThat(plan).as(counted).contains("COUNT CHAIN PATHS");
      assertThat(chainLine(plan)).as(counted).doesNotStartWith("(Chunk");
      assertThat(count(counted)).as(counted).isEqualTo(count(oracle));
    }
    // The walk seeded from a key applies no inequality, and a walk of the Record label to apply it would read every record to
    // keep one: the ordinary plan seeks the key instead (issue #11680), and counts the same
    final String keyed = "MATCH (c2:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(c1:Chunk)-[:OF_RECORD]->(r:Record {external_id: 'r3'}) "
        + "WHERE c1 <> c2";
    assertThat(plan(keyed + " RETURN count(*) AS n")).doesNotContain("COUNT CHAIN PATHS");
    assertThat(count(keyed + " RETURN count(*) AS n")).isEqualTo(count(keyed + " RETURN sum(1) AS n")).isPositive();
    // the inequality removes the paths that come back to the same chunk over its two parallel mentions: a count that ignored it
    // would be larger
    final String withInequality = "MATCH " + patterns[0] + " RETURN count(*) AS n";
    final String without = "MATCH (c2:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(c1:Chunk)-[:OF_RECORD]->(r:Record) "
        + "RETURN sum(1) AS n";
    assertThat(count(withInequality)).isLessThan(count(without));
  }

  /**
   * The record is looked up when the count runs, not when it is planned, and the count sees what the transaction wrote: a key no
   * record holds counts 0, a record and its chunks created in the same transaction are counted, and so are the chunks of a
   * record found by a parameter.
   */
  @Test
  void theSeededRecordIsLookedUpWhenTheCountRuns() {
    final String query = "MATCH (c:Chunk)-[:OF_RECORD]->(r:Record {external_id: $id}) RETURN count(c) AS n";
    assertThat(plan(query, Map.of("id", "nope"))).contains("seeded anchor: unique key Record[external_id] = [nope]");
    assertThat(count(query, Map.of("id", "nope"))).isZero();
    assertThat(count(query, Map.of("id", "r4"))).isEqualTo(CHUNKS / RECORDS);

    database.begin();
    try {
      database.command("opencypher", "CREATE (r:Record {external_id: 'fresh'}) WITH r UNWIND range(1, 3) AS i "
          + "CREATE (:Chunk {i: -i})-[:OF_RECORD]->(r)");
      assertThat(count(query, Map.of("id", "fresh"))).isEqualTo(3);
      database.command("opencypher", "MATCH (c:Chunk {i: -1}) DETACH DELETE c");
      assertThat(count(query, Map.of("id", "fresh"))).isEqualTo(2);
    } finally {
      database.rollback();
    }
    assertThat(count(query, Map.of("id", "fresh"))).isZero();
  }

  /**
   * The seeded walk carries the number of paths reaching each vertex rather than one entry per path: parallel edges multiply
   * the count, not the frontier. Every shape is checked against the row pipeline, without a graph view (the OLTP walk) and with
   * one (the CSR walk): parallel edges at every hop, an undirected hop over a self loop, a predicate in the middle and at the far
   * end, and the chain read from its last node.
   */
  @Test
  void theSeededWalkCountsPathsPerVertexReached() {
    database.command("sql", "CREATE VERTEX TYPE Hub");
    database.command("sql", "CREATE VERTEX TYPE Leaf");
    database.command("sql", "CREATE EDGE TYPE P1");
    database.command("sql", "CREATE EDGE TYPE P2");
    database.command("sql", "CREATE EDGE TYPE P3");
    database.transaction(() -> {
      final MutableVertex record = database.newVertex("Record").set("external_id", "mult").save();
      final MutableVertex h1 = database.newVertex("Hub").set("n", 1).save();
      final MutableVertex h2 = database.newVertex("Hub").set("n", 2).save();
      final List<MutableVertex> leaves = new ArrayList<>();
      for (int i = 0; i < 4; i++)
        leaves.add(database.newVertex("Leaf").set("n", i).save());
      for (int i = 0; i < 3; i++)
        record.newEdge("P1", h1);
      for (int i = 0; i < 2; i++)
        record.newEdge("P1", h2);
      for (final MutableVertex leaf : leaves)
        h1.newEdge("P2", leaf);
      h1.newEdge("P2", leaves.getFirst());
      h2.newEdge("P2", leaves.get(1));
      h2.newEdge("P2", leaves.get(2));
      leaves.getFirst().newEdge("P3", leaves.getFirst());
      leaves.get(1).newEdge("P3", leaves.get(2));
      leaves.get(1).newEdge("P3", leaves.get(2));
    });
    final String[] patterns = {
        "(r:Record {external_id: 'mult'})-[:P1]->(h:Hub)-[:P2]->(l:Leaf)",
        "(r:Record {external_id: 'mult'})-[:P1]->(h:Hub)-[:P2]->(l:Leaf)-[:P3]-(m:Leaf)",
        "(r:Record {external_id: 'mult'})-[:P1]->(h:Hub)-[:P2]->(l:Leaf {n: 1})",
        "(r:Record {external_id: 'mult'})-[:P1]-(h:Hub {n: 2})-[:P2]->(l:Leaf)-[:P3]->(m)",
        "(m:Leaf)-[:P3]-(l:Leaf)<-[:P2]-(h:Hub)<-[:P1]-(r:Record {external_id: 'mult'})" };
    final long[] oracle = new long[patterns.length];
    for (int i = 0; i < patterns.length; i++) {
      final String counted = "MATCH " + patterns[i] + " RETURN count(*) AS n";
      assertThat(plan(counted)).as(counted).contains("seeded anchor: unique key Record[external_id] = [mult]");
      oracle[i] = count("MATCH " + patterns[i] + " RETURN sum(1) AS n");
      assertThat(oracle[i]).as(counted).isPositive();
      assertThat(count(counted)).as(counted + " (OLTP)").isEqualTo(oracle[i]);
    }
    // 3 parallel P1 edges reach h1 and its 5 P2 edges, 2 reach h2 and its 2: one entry per path would hold 19
    assertThat(oracle[0]).isEqualTo(3 * 5 + 2 * 2);

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("seeded")
        .withVertexTypes("Record", "Chunk", "Entity", "Hub", "Leaf").withEdgeTypes("P1", "P2", "P3").build();
    try {
      assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "P1", "P2", "P3")).isNotNull();
      for (int i = 0; i < patterns.length; i++) {
        final String counted = "MATCH " + patterns[i] + " RETURN count(*) AS n";
        assertThat(count(counted)).as(counted + " (CSR)").isEqualTo(oracle[i]);
      }
    } finally {
      view.drop();
    }
  }

  /** A UNIQUE key on a document type pins no vertex: a node pattern naming that type matches nothing. */
  @Test
  void aKeyOnADocumentTypeIsNotSeeded() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
    database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");
    database.transaction(() -> database.newDocument("Doc").set("id", 1).save());
    final String query = "MATCH (d:Doc {id: 1})-[:OF_RECORD]->(r:Record) RETURN count(*) AS n";
    assertThat(plan(query)).doesNotContain("seeded anchor");
    assertThat(count(query)).isZero();
  }

  /** A key that cannot pin one vertex is not looked up: the chain is walked as before and counts the same. */
  @Test
  void aKeyThatPinsNoVertexIsNotSeeded() {
    // a null key matches nothing, while a unique index holds an entry per record whose key is null
    database.transaction(() -> database.newVertex("Record").save());
    final String nullKey = "MATCH (c:Chunk)-[:OF_RECORD]->(r:Record {external_id: $id}) RETURN count(*) AS n";
    final Map<String, Object> nullParam = new HashMap<>();
    nullParam.put("id", null);
    assertThat(plan(nullKey, nullParam)).doesNotContain("seeded anchor");
    assertThat(count(nullKey, nullParam)).isZero();
    // a computed key is a filter, never a seek of the index (RowIndependentValues.isSeekable)
    final String computed = "MATCH (c:Chunk)-[:OF_RECORD]->(r:Record {external_id: 'r' + '5'}) RETURN count(*) AS n";
    assertThat(plan(computed, Map.of())).doesNotContain("seeded anchor");
    assertThat(count(computed, Map.of())).isEqualTo(CHUNKS / RECORDS);
    // a property the index does not cover
    final String unindexed = "MATCH (c:Chunk {i: 6})-[:OF_RECORD]->(r:Record) RETURN count(*) AS n";
    assertThat(plan(unindexed, Map.of())).contains("COUNT CHAIN PATHS").doesNotContain("seeded anchor");
    assertThat(count(unindexed, Map.of())).isEqualTo(1);
  }

  /** The incoming side of a unidirectional edge type is stored on no vertex: such a chain keeps its written orientation. */
  @Test
  void aChainOverAUnidirectionalTypeIsNotFlipped() {
    database.command("sql", "CREATE EDGE TYPE BELONGS UNIDIRECTIONAL");
    database.transaction(() -> database.command("opencypher",
        "MATCH (c:Chunk), (r:Record) WHERE c.i % 20 = 3 AND r.external_id = 'r3' CREATE (c)-[:BELONGS]->(r)"));
    final String counted = "MATCH (c:Chunk)-[:BELONGS]->(r:Record {external_id: 'r3'}) RETURN count(*) AS n";
    assertThat(count(counted)).isEqualTo(CHUNKS / RECORDS);
    assertThat(count("MATCH (c:Chunk)-[:BELONGS]->(r:Record) RETURN count(*) AS n")).isEqualTo(CHUNKS / RECORDS);
    final String plan = plan(counted);
    assertThat(plan).contains("COUNT CHAIN PATHS");
    assertThat(chainLine(plan)).startsWith("(Chunk");
  }

  /** An unlabeled first end is flipped away from (it would enumerate every vertex); an unlabeled last end is never the start. */
  @Test
  void anUnlabeledEndIsNeverTheStart() {
    final String unlabeledFirst = "(x)-[:OF_RECORD]->(r:Record {external_id: 'r3'})";
    final String unlabeledLast = "(c:Chunk)-[:OF_RECORD]->(x)";
    for (final String pattern : new String[] { unlabeledFirst, unlabeledLast }) {
      final String counted = "MATCH " + pattern + " RETURN count(*) AS n";
      final String oracle = "MATCH " + pattern + " RETURN sum(1) AS n";
      final String plan = plan(counted);
      assertThat(plan).as(counted).contains("COUNT CHAIN PATHS");
      assertThat(count(counted)).as(counted).isEqualTo(count(oracle));
    }
    assertThat(chainLine(plan("MATCH " + unlabeledFirst + " RETURN count(*) AS n"))).startsWith("(Record");
    assertThat(chainLine(plan("MATCH " + unlabeledLast + " RETURN count(*) AS n"))).startsWith("(Chunk");
  }

  private long count(final String query) {
    return count(query, Map.of());
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      final Number n = rs.next().getProperty("n");
      return n == null ? 0L : n.longValue();
    }
  }

  private String plan(final String query) {
    return plan(query, Map.of());
  }

  private String plan(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private static String chainLine(final String plan) {
    for (final String line : plan.split("\n"))
      if (line.trim().startsWith("chain: "))
        return line.trim().substring("chain: ".length());
    throw new AssertionError("no chain line in the plan:\n" + plan);
  }
}
