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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10376: the overlay of a query over a unidirectional type kept its deletions in a SET keyed by the edge identity,
 * and the identity of a lightweight edge is the triple (type, out, in) its twins share. Deleting ONE twin therefore hid
 * EVERY twin of that pair, for the rest of the statement, from the incoming side: from the scanned edges, and from the
 * twins the statement itself had created.
 * <p>
 * The persisted state was always right, so every test reads the transaction-local view of a statement that deletes first
 * and counts after, and then the committed state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10376LightweightTwinTombstoneTest extends TestHelper {
  private RID v;
  private RID w;

  @Test
  void deletingOneCommittedTwinLeavesTheOtherOnTheIncomingSide() {
    createSchema(true);
    createTwins(2);

    final long inStatement = single("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one "
        + "WITH w MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");

    assertThat(inStatement).isEqualTo(1L);
    assertThat(committedIncoming()).isEqualTo(1L);
  }

  @Test
  void theRecordBackedControlAnswersTheSame() {
    createSchema(false);
    createTwins(2);

    final long inStatement = single("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one "
        + "WITH w MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");

    assertThat(inStatement).isEqualTo(1L);
    assertThat(committedIncoming()).isEqualTo(1L);
  }

  @Test
  void deletingOneTwinCreatedByTheSameStatementLeavesTheOther() {
    createSchema(true);

    final long inStatement = single("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-() WITH count(r) AS seed "
        + "MATCH (v:V {n:'v'}), (w:V {n:'w'}) CREATE (v)-[a:T]->(w) CREATE (v)-[b:T]->(w) DELETE a "
        + "WITH w MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");

    assertThat(inStatement).isEqualTo(1L);
    assertThat(committedIncoming()).isEqualTo(1L);
  }

  @Test
  void twinsCreatedByTheStatementAreAllDeletedByIt() {
    createSchema(true);

    database.command("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) OPTIONAL MATCH (w)<-[seed:T]-() WITH v, w, count(seed) AS seeded "
        + "UNWIND range(1, 2) AS i CREATE (v)-[:T]->(w) WITH w, count(*) AS made MATCH (w)<-[r:T]-() DELETE r").close();

    assertThat(committedIncoming()).isZero();
  }

  @Test
  void everyDeletedTwinIsGoneAndOnlyTheDeletedOnes() {
    createSchema(true);
    for (int committed = 0; committed <= 4; committed++)
      for (int created = 0; created <= 3; created++)
        for (int deleted = 0; deleted <= committed + created; deleted++)
          checkMix(committed, created, deleted);
  }

  @Test
  void aTwinCreatedAfterTheDeleteOfAnotherOneSurvives() {
    createSchema(true);
    createTwins(1);

    final ResultSet rs = database.command("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) MATCH (w)<-[r:T]-() DELETE r "
        + "WITH v, w, count(*) AS deleted CREATE (v)-[:T]->(w) WITH w, deleted MATCH (w)<-[r2:T]-() "
        + "RETURN deleted, count(r2) AS n, COUNT { (w)<-[:T]-() } AS viaCount");
    final Result row = rs.next();
    rs.close();

    assertThat(row.<Number>getProperty("deleted").longValue()).isEqualTo(1L);
    assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(1L);
    assertThat(row.<Number>getProperty("viaCount").longValue()).isEqualTo(1L);
    assertThat(committedIncoming()).isEqualTo(1L);
  }

  @Test
  void theEdgesAndTheSourcesAnsweredAgreeWithTheCount() {
    createSchema(true);
    createTwins(3);

    final ResultSet rs = database.command("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..2] AS one DELETE one "
        + "WITH w, count(one) AS deleted MATCH (w)<-[r2:T]-(s) RETURN deleted, count(r2) AS n, collect(s) AS sources");
    final Result row = rs.next();
    rs.close();

    assertThat(row.<Number>getProperty("deleted").longValue()).isEqualTo(2L);
    assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(1L);
    final List<Vertex> sources = row.getProperty("sources");
    assertThat(sources).hasSize(1);
    assertThat(sources.get(0).getIdentity()).isEqualTo(v);
    assertThat(committedIncoming()).isEqualTo(1L);
  }

  @Test
  void twinsOfAnotherPairAreNotTouched() {
    createSchema(true);
    createTwins(2);
    final RID[] other = new RID[1];
    database.transaction(() -> other[0] = database.newVertex("V").set("n", "x").save().getIdentity());
    database.transaction(() -> {
      other[0].asVertex().newEdge("T", w.asVertex());
      other[0].asVertex().newEdge("T", w.asVertex());
    });

    final ResultSet rs = database.command("opencypher", "MATCH (w:V {n:'w'})<-[r:T]-(:V {n:'v'}) WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one "
        + "WITH w, count(one) AS deleted MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");
    final long n = rs.next().<Number>getProperty("n").longValue();
    rs.close();

    assertThat(n).isEqualTo(3L);
    assertThat(committedIncoming()).isEqualTo(3L);
  }

  /**
   * Deleting nodes together with their relationships (issue comment): the twins of the pair were deleted, then the check
   * on the node found them again.
   */
  @Test
  void deletingRelationshipsAndNodesTogetherSucceedsWithTwins() {
    createSchema(true);
    database.command("opencypher", "CREATE (n0:V {k: 0}), (n1:V {k: 1}), (n0)-[:T]->(n0), (n1)-[:T]->(n0), (n1)-[:T]->(n0)").close();

    database.transaction(() -> database.command("opencypher", "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE r, n").close());

    assertThat(database.countType("V", false)).isZero();
    assertThat(database.countType("T", false)).isZero();
  }

  @Test
  void deletingRelationshipsAndNodesTogetherSucceedsOnASecondShape() {
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().buildEdgeType().withName("R").withBidirectional(false).withLightweight(true).create();
    database.getSchema().buildEdgeType().withName("S").withBidirectional(false).withLightweight(true).create();
    database.command("opencypher", "CREATE (n0:B {k: 0}), (n1:B {k: 1}), (n2:A {k: 2}), (n3:B {k: 3}), (n0)-[:R]->(n0), (n0)-[:S]->(n2), "
        + "(n0)-[:S]->(n2), (n2)-[:R]->(n1)").close();

    database.transaction(() -> database.command("opencypher", "MATCH (a)-[r]->(b) DELETE a, r, b").close());

    assertThat(database.countType("R", false)).isZero();
    assertThat(database.countType("S", false)).isZero();
  }

  /**
   * One statement reads the incoming side (taking the scan), creates {@code created} twins, deletes {@code deleted} of
   * the twins it can see, and reads the incoming side again: the overlay is the only thing that can answer the last read.
   */
  private void checkMix(final int committed, final int created, final int deleted) {
    resetData();
    createTwins(committed);

    final StringBuilder statement = new StringBuilder(
        "MATCH (v:V {n:'v'}), (w:V {n:'w'}) OPTIONAL MATCH (w)<-[seed:T]-() WITH v, w, count(seed) AS seeded ");
    if (created > 0)
      statement.append("UNWIND range(1, ").append(created).append(") AS i CREATE (v)-[:T]->(w) WITH w, count(*) AS made ");
    if (deleted > 0)
      statement.append("MATCH (w)<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..").append(deleted)
          .append("] AS one DELETE one WITH w, count(one) AS gone ");
    statement.append("OPTIONAL MATCH (w)<-[r2:T]-() RETURN count(r2) AS n, COUNT { (w)<-[:T]-() } AS viaCount");

    final String label = "committed " + committed + " created " + created + " deleted " + deleted;
    final ResultSet rs = database.command("opencypher", statement.toString());
    final Result row = rs.next();
    rs.close();

    assertThat(row.<Number>getProperty("n").longValue()).as(label).isEqualTo(committed + created - deleted);
    assertThat(row.<Number>getProperty("viaCount").longValue()).as(label + " (COUNT subquery)").isEqualTo(committed + created - deleted);
    assertThat(committedIncoming()).as("persisted: " + label).isEqualTo(committed + created - deleted);
  }

  private void resetData() {
    database.transaction(() -> database.command("sql", "DELETE FROM V").close());
    createVertices();
  }

  private void createSchema(final boolean lightweight) {
    database.getSchema().createVertexType("V");
    database.getSchema().buildEdgeType().withName("T").withBidirectional(false).withLightweight(lightweight).create();
    createVertices();
  }

  private void createVertices() {
    database.transaction(() -> {
      v = database.newVertex("V").set("n", "v").save().getIdentity();
      w = database.newVertex("V").set("n", "w").save().getIdentity();
    });
  }

  private void createTwins(final int twins) {
    if (twins == 0)
      return;
    database.transaction(() -> {
      for (int i = 0; i < twins; i++)
        v.asVertex().newEdge("T", w.asVertex());
    });
  }

  /** The committed incoming side of {@code w}, from every edge of the type, read in a fresh transaction. */
  private long committedIncoming() {
    long edges = 0;
    for (final Iterator<Record> it = database.iterateType("V", false); it.hasNext(); )
      for (final Vertex target : it.next().asVertex().getVertices(Vertex.DIRECTION.OUT, "T"))
        if (target.getIdentity().equals(w))
          ++edges;
    return edges;
  }

  private long single(final String language, final String query) {
    try (final ResultSet rs = database.command(language, query)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }
}
