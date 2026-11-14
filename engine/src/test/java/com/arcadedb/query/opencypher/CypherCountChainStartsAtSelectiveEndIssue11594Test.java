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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11594: the chain count push-down ({@code COUNT CHAIN PATHS}) always enumerated the label of the
 * chain's first position, so {@code MATCH (c:Chunk)-[:OF_RECORD]->(r:Record {external_id: $id}) RETURN count(c)} walked
 * every chunk of the database to keep the ones of one record. An uncorrelated chain now starts from its far end when that
 * end carries the only node predicate, or, with predicates on neither or both ends, when its label holds fewer vertices.
 * The count is the same read either way; each one is checked against the row pipeline, reached through a {@code sum(1)}
 * that the push-downs decline.
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
        "(c2:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(c1:Chunk)-[:OF_RECORD]->(r:Record {external_id: 'r3'}) "
            + "WHERE c1 <> c2",
        "(c:Chunk)-[:MENTIONS]->(e:Entity) WHERE c <> e" };
    for (final String pattern : patterns) {
      final String counted = "MATCH " + pattern + " RETURN count(*) AS n";
      final String oracle = "MATCH " + pattern + " RETURN sum(1) AS n";
      final String plan = plan(counted);
      assertThat(plan).as(counted).contains("COUNT CHAIN PATHS");
      assertThat(chainLine(plan)).as(counted).doesNotStartWith("(Chunk");
      assertThat(count(counted)).as(counted).isEqualTo(count(oracle));
    }
    // the inequality removes the paths that come back to the same chunk over its two parallel mentions: a count that ignored it
    // would be larger
    final String withInequality = "MATCH " + patterns[0] + " RETURN count(*) AS n";
    final String without = "MATCH (c2:Chunk)-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(c1:Chunk)-[:OF_RECORD]->(r:Record) "
        + "RETURN sum(1) AS n";
    assertThat(count(withInequality)).isLessThan(count(without));
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
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Number n = rs.next().getProperty("n");
      return n == null ? 0L : n.longValue();
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
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
