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
import java.util.Random;

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

    // THE EDGES ARE LIGHTWEIGHT, SO THERE IS NO RECORD TO COUNT: THE VERTICES GOING IS THE RESULT, AND THEY GO ONLY ONCE NONE HOLDS AN EDGE
    assertThat(database.countType("V", false)).isZero();
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

    // ONLY THE ISOLATED n3 IS LEFT, AND THE EDGES ARE LIGHTWEIGHT: THEY ARE COUNTED THROUGH THE LISTS OF THE VERTEX, NOT AS RECORDS
    assertThat(database.countType("A", false) + database.countType("B", false)).isEqualTo(1L);
    for (final Iterator<Record> it = database.iterateType("B", false); it.hasNext(); )
      assertThat(it.next().asVertex().countEdges(Vertex.DIRECTION.BOTH, "R", "S")).isZero();
  }

  /**
   * One statement reads the incoming side (taking the scan), creates {@code created} twins, deletes {@code deleted} of
   * the twins it can see, and reads the incoming side again: the overlay is the only thing that can answer the last read.
   */
  /** Each lookup builds its own overlay and asks it once per entry: asking again, in the same statement, answers the same. */
  @Test
  void aLookupAskedAgainInTheSameStatementAnswersTheSame() {
    createSchema(true);
    createTwins(3);

    final ResultSet rs = database.command("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) MATCH (w)<-[r:T]-() WITH v, w, collect(r) AS rs "
        + "UNWIND rs[0..1] AS one DELETE one WITH v, w, count(one) AS deleted CREATE (v)-[:T]->(w) WITH w, deleted "
        + "MATCH (w)<-[r1:T]-() WITH w, count(r1) AS first MATCH (w)<-[r2:T]-() WITH w, first, count(r2) AS second "
        + "RETURN first, second, COUNT { (w)<-[:T]-() } AS third, COUNT { (w)<-[:T]-() } AS fourth");
    final Result row = rs.next();
    rs.close();

    for (final String name : new String[] { "first", "second", "third", "fourth" })
      assertThat(row.<Number>getProperty(name).longValue()).as(name).isEqualTo(3L);
    assertThat(committedIncoming()).isEqualTo(3L);
  }

  @Test
  void hundredsOfTwinsAreCountedAndNumberedOnePerRelationship() {
    createSchema(true);
    createTwins(300);

    final ResultSet rs = database.command("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) OPTIONAL MATCH (w)<-[seed:T]-() "
        + "WITH v, w, count(seed) AS seeded UNWIND range(1, 200) AS i CREATE (v)-[:T]->(w) WITH w, count(*) AS made "
        + "MATCH (w)<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..250] AS one DELETE one WITH w, count(one) AS gone "
        + "MATCH (w)<-[r2:T]-() RETURN count(r2) AS n, count(DISTINCT r2) AS relationships");
    final Result row = rs.next();
    rs.close();

    assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(250L);
    assertThat(row.<Number>getProperty("relationships").longValue()).as("every twin is a relationship of its own").isEqualTo(250L);
    assertThat(committedIncoming()).isEqualTo(250L);
  }

  /** The key of a tombstone is the triple: the same for the identity read off a list and for the one stripped of it. */
  @Test
  void theKeyOfATombstoneIsTheTripleAlone() {
    createSchema(true);
    createTwins(2);

    final Edge edge = v.asVertex().getEdges(Vertex.DIRECTION.OUT, "T").iterator().next();
    final LightEdgeRID read = (LightEdgeRID) edge.getIdentity();
    assertThat(read.getOriginSegment()).as("read off a list, it knows where from").isNotNull();

    final LightEdgeRID key = read.withoutOrigin();

    assertThat(key.getOriginSegment()).isNull();
    assertThat(key).isEqualTo(read);
    assertThat(read).isEqualTo(key);
    assertThat(key.hashCode()).isEqualTo(read.hashCode());
    assertThat(new LightEdgeRID(database, read.getBucketId(), w, v)).as("another pair").isNotEqualTo(key);
  }

  /**
   * The numbering of the overlay is subtle (the run of scanned twins, where the deletes start in it, the created twins numbered
   * after them), so random statements are checked against a plain model: a count per pair, moved by every create and delete of
   * the statement. Each statement reads the incoming side first, so its writes are overlaid, creates and deletes interleave over
   * four pairs, and the last read has to answer what the model holds, with each twin a relationship of its own.
   */
  @Test
  void randomStatementsAgreeWithAPlainModelOfTheTwins() {
    createSchema(true);
    final int sources = 2;
    final int targets = 2;
    for (int seed = 0; seed < 60; seed++) {
      final Random random = new Random(seed);
      database.transaction(() -> database.command("sql", "DELETE FROM V").close());
      final RID[] sourceRids = new RID[sources];
      final RID[] targetRids = new RID[targets];
      database.transaction(() -> {
        for (int i = 0; i < sources; i++)
          sourceRids[i] = database.newVertex("V").set("n", "s" + i).save().getIdentity();
        for (int i = 0; i < targets; i++)
          targetRids[i] = database.newVertex("V").set("n", "t" + i).save().getIdentity();
      });

      final int[][] model = new int[sources][targets];
      database.transaction(() -> {
        for (int s = 0; s < sources; s++)
          for (int t = 0; t < targets; t++) {
            model[s][t] = random.nextInt(4);
            for (int i = 0; i < model[s][t]; i++)
              sourceRids[s].asVertex().newEdge("T", targetRids[t].asVertex());
          }
      });

      final StringBuilder statement = new StringBuilder("MATCH (t:V)<-[r:T]-(s:V) WITH count(r) AS seeded");
      final int stages = 3 + random.nextInt(6);
      for (int stage = 1; stage <= stages; stage++) {
        final int s = random.nextInt(sources);
        final int t = random.nextInt(targets);
        final int amount = 1 + random.nextInt(3);
        if (random.nextBoolean()) {
          statement.append(" MATCH (s:V {n:'s").append(s).append("'}), (t:V {n:'t").append(t).append("'}) UNWIND range(1, ").append(amount)
              .append(") AS i").append(stage).append(" CREATE (s)-[:T]->(t) WITH count(*) AS made").append(stage);
          model[s][t] += amount;
        } else {
          statement.append(" MATCH (t:V {n:'t").append(t).append("'})<-[r").append(stage).append(":T]-(s) WHERE s.n = 's").append(s)
              .append("' WITH collect(r").append(stage).append(") AS rs").append(stage).append(" UNWIND rs").append(stage).append("[0..")
              .append(amount).append("] AS one").append(stage).append(" DELETE one").append(stage).append(" WITH count(one").append(stage)
              .append(") AS gone").append(stage);
          model[s][t] -= Math.min(amount, model[s][t]);
        }
      }
      statement.append(" MATCH (t:V)<-[r:T]-(s:V) RETURN t.n AS t, s.n AS s, count(r) AS n, count(DISTINCT r) AS relationships");

      final int[][] answered = new int[sources][targets];
      try (final ResultSet rs = database.command("opencypher", statement.toString())) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final int s = Integer.parseInt(row.<String>getProperty("s").substring(1));
          final int t = Integer.parseInt(row.<String>getProperty("t").substring(1));
          answered[s][t] = row.<Number>getProperty("n").intValue();
          assertThat(row.<Number>getProperty("relationships").intValue()).as("seed %d: every twin is a relationship of its own", seed)
              .isEqualTo(answered[s][t]);
        }
      }

      for (int s = 0; s < sources; s++)
        for (int t = 0; t < targets; t++) {
          assertThat(answered[s][t]).as("seed %d, the statement's last read of s%d -> t%d: %s", seed, s, t, statement).isEqualTo(model[s][t]);
          assertThat(sourceRids[s].asVertex().countEdges(Vertex.DIRECTION.OUT, "T")).isEqualTo(
              model[s][0] + model[s][1]);
        }
    }
  }

  /**
   * A transaction keeps its changes across its statements, and a later statement takes a scan of its own that already holds the
   * earlier ones: only the deletes made after that scan are overlaid on it.
   */
  @Test
  void aLaterStatementOfTheTransactionIgnoresTheDeletesTheEarlierOnesMade() {
    createSchema(true);
    createTwins(3);
    final String deleteOneThenCount = "MATCH (w:V {n:'w'})<-[r:T]-() WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one "
        + "WITH w, count(one) AS deleted OPTIONAL MATCH (w)<-[r2:T]-() RETURN count(r2) AS n";
    final long[] answers = new long[3];

    database.transaction(() -> {
      answers[0] = single("opencypher", deleteOneThenCount);
      answers[1] = single("opencypher", deleteOneThenCount);
      // THE SCAN OF THIS STATEMENT HOLDS BOTH EARLIER DELETES: IT CREATES TWO TWINS OVER ITS ONE TWIN LEFT AND DELETES ONE OF THEM
      answers[2] = single("opencypher", "MATCH (v:V {n:'v'}), (w:V {n:'w'}) OPTIONAL MATCH (w)<-[s:T]-() WITH v, w, count(s) AS seeded "
          + "UNWIND range(1, 2) AS i CREATE (v)-[:T]->(w) WITH w, count(*) AS made MATCH (w)<-[r:T]-() "
          + "WITH w, collect(r) AS rs UNWIND rs[0..1] AS one DELETE one WITH w, count(one) AS deleted "
          + "OPTIONAL MATCH (w)<-[r2:T]-() RETURN count(r2) AS n");
    });

    assertThat(answers).containsExactly(2L, 1L, 2L);
    assertThat(committedIncoming()).isEqualTo(2L);
  }

  @Test
  void theDeletesOfATripleAreCountedAfterASequence() {
    createSchema(true);
    final int bucket = database.getSchema().getType("T").getFirstBucketId();
    final LightEdgeRID pair = new LightEdgeRID(database, bucket, v, w);
    final LightEdgeRID otherPair = new LightEdgeRID(database, bucket, w, v);
    final UnidirectionalEdgeChanges changes = new UnidirectionalEdgeChanges();
    changes.scanTaken();

    final long[] sequences = new long[7];
    for (int i = 0; i < sequences.length; i++) {
      changes.edgeDeleted(i % 3 == 2 ? otherPair : pair);
      sequences[i] = changes.getSequence();
    }

    assertThat(changes.hasTwinDeletions()).isTrue();
    assertThat(changes.hasDeletions()).as("the record-backed deletions are another structure").isFalse();
    // THE SEQUENCE OF A TRIPLE ASCENDS WITH GAPS WHERE THE OTHER TRIPLE WAS DELETED: PAIR TOOK 0, 1, 3, 4 AND 6
    assertThat(changes.twinsDeletedAfter(pair, 0L)).isEqualTo(5);
    assertThat(changes.twinsDeletedAfter(pair, sequences[0])).isEqualTo(4);
    assertThat(changes.twinsDeletedAfter(pair, sequences[2])).isEqualTo(3);
    assertThat(changes.twinsDeletedAfter(pair, sequences[4])).isEqualTo(1);
    assertThat(changes.twinsDeletedAfter(pair, sequences[6])).isZero();
    assertThat(changes.twinsDeletedAfter(otherPair, sequences[2] - 1)).isEqualTo(2);
    assertThat(changes.twinsDeletedAfter(otherPair, sequences[2])).isEqualTo(1);
    assertThat(changes.twinsDeletedAfter(new LightEdgeRID(database, bucket, v, v), 0L)).isZero();
    assertThat(changes.size()).isEqualTo(7);

    changes.transactionEnded();
    assertThat(changes.hasTwinDeletions()).isFalse();
    assertThat(changes.twinsDeletedAfter(pair, 0L)).isZero();
  }

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
