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
import com.arcadedb.database.Database;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #11660 and #12483, deterministically: two transactions MERGE the same relationship, and neither sees the other's
 * uncommitted one, so each creates it. Appends to the edge list of a vertex both transactions read commute at commit, so without
 * a check the two commits leave two copies of the relationship. The MERGE registers what it created with its transaction, and the
 * commit of the second transaction is refused with a retryable conflict, as a retry then matches the first one's relationship.
 * <p>
 * The function {@code race.interleave(x)}, called from {@code ON CREATE SET}, which runs after the MERGE created the relationship,
 * commits the other transaction's MERGE from another thread and waits for it, so the other commit lands at the same point on every
 * run: {@link CypherMergeConcurrentShapesTest} reaches the same window with threads and a race. A MERGE of another pattern must not
 * be refused: the check is keyed by the pattern (existing node, type, direction, what the relationship and the created node are
 * matched by), not by the existence of a concurrent MERGE.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES) // a hang detector, not a latency bound
class CypherMergeEdgeCommitConflictTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sqlscript", "CREATE VERTEX TYPE A; CREATE VERTEX TYPE B; CREATE VERTEX TYPE C; CREATE EDGE TYPE R;"
        + " CREATE EDGE TYPE RU; CREATE PROPERTY RU.uuid STRING; CREATE INDEX ON RU (uuid) NOTUNIQUE;"
        + " CREATE PROPERTY A.id INTEGER; CREATE INDEX ON A (id) UNIQUE; CREATE PROPERTY B.id INTEGER; CREATE INDEX ON B (id) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1}), (:B {id: 2})").close());
    try {
      database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("race", Race.class));
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected void endTest() {
    Race.PENDING.set(null);
    database.getSchema().unregisterFunctionLibrary("race");
  }

  /** What the ON CREATE SET of the statement under test runs once its MERGE created the relationship: the other MERGE commits. */
  public static class Race {
    static final AtomicReference<Runnable> PENDING = new AtomicReference<>();

    public static boolean interleave(final Object row) {
      final Runnable commit = PENDING.getAndSet(null);
      if (commit != null)
        commit.run();
      return true;
    }
  }

  @Test
  void aRelationshipMergedBetweenTwoExistingNodesIsNotCreatedTwice() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:R]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:R]->(b)");

    assertThat(committed).as("the second MERGE is refused").isFalse();
    assertThat(count("MATCH (:A {id: 1})-[r:R]->(:B {id: 2}) RETURN r")).isEqualTo(1);
  }

  @Test
  void aRelationshipAndItsRightNodeMergedTogetherAreNotCreatedTwice() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7})");

    assertThat(committed).as("the second MERGE is refused").isFalse();
    assertThat(count("MATCH (b:B {id: 7}) RETURN b")).isEqualTo(1);
    assertThat(count("MATCH (:A {id: 1})-[r:R]->(:B {id: 7}) RETURN r")).isEqualTo(1);
  }

  @Test
  void aRelationshipAndItsLeftNodeMergedTogetherAreNotCreatedTwice() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (b:B {id: 2}) MERGE (n:C {id: 9})-[r:R]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (b:B {id: 2}) MERGE (n:C {id: 9})-[r:R]->(b)");

    assertThat(committed).as("the second MERGE is refused").isFalse();
    assertThat(count("MATCH (n:C {id: 9}) RETURN n")).isEqualTo(1);
    assertThat(count("MATCH (:C {id: 9})-[r:R]->(:B {id: 2}) RETURN r")).isEqualTo(1);
  }

  @Test
  void anIncomingRelationshipAndItsFarNodeMergedTogetherAreNotCreatedTwice() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}) MERGE (a)<-[r:R]-(b:B {id: 7}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}) MERGE (a)<-[r:R]-(b:B {id: 7})");

    assertThat(committed).as("the second MERGE is refused").isFalse();
    assertThat(count("MATCH (b:B {id: 7}) RETURN b")).isEqualTo(1);
    assertThat(count("MATCH (:B {id: 7})-[r:R]->(:A {id: 1}) RETURN r")).isEqualTo(1);
  }

  @Test
  void aRelationshipToAnotherFarNodeIsNotAConflict() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 8})");

    assertThat(committed).isTrue();
    assertThat(count("MATCH (:A {id: 1})-[r:R]->(b:B) WHERE b.id IN [7, 8] RETURN r")).isEqualTo(2);
  }

  @Test
  void aRelationshipToTheSameFarNodeUnderAnotherLabelIsNotAConflict() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:C {id: 7})");

    assertThat(committed).isTrue();
    assertThat(count("MATCH (:A {id: 1})-[r:R]->(b) WHERE b.id = 7 RETURN r")).isEqualTo(2);
  }

  @Test
  void aRelationshipWithAnotherPropertyValueIsNotAConflictAndTheSameValueIs() {
    assertThat(mergeWithConcurrent("MATCH (a:A {id: 1}) MERGE (a)-[r:R {w: 1}]->(b:B {id: 7}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R {w: 2}]->(b:B {id: 7})")).isTrue();
    assertThat(count("MATCH (:A {id: 1})-[r:R]->(b:B {id: 7}) RETURN r")).isEqualTo(2);

    assertThat(mergeWithConcurrent("MATCH (a:A {id: 1}) MERGE (a)-[r:R {w: 3}]->(b:B {id: 11}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R {w: 3}]->(b:B {id: 11})")).isFalse();
    assertThat(count("MATCH (:A {id: 1})-[r:R {w: 3}]->(b:B {id: 11}) RETURN r")).isEqualTo(1);
  }

  @Test
  void aRelationshipToTheSameNodeOfAnotherDirectionIsNotAConflict() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}) MERGE (a)<-[r:R]-(b:B {id: 7})");

    assertThat(committed).isTrue();
    assertThat(count("MATCH (:A {id: 1})-[r:R]-(b:B {id: 7}) RETURN r")).isEqualTo(2);
  }

  @Test
  void aRelationshipOfAnotherTypeIsNotAConflict() {
    database.command("sql", "CREATE EDGE TYPE S");

    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:R]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:S]->(b)");

    assertThat(committed).isTrue();
    assertThat(count("MATCH (:A {id: 1})-[r]->(:B {id: 2}) RETURN r")).isEqualTo(2);
  }

  /**
   * The key of a MERGE that creates the far node is coarser than the pattern, on purpose (see {@code registerMergedEdgeToCreatedNode}):
   * a relationship of the same type and direction from the same node to a node that existed may be the one the pattern matches, so
   * it is refused, and one of the other direction is not.
   */
  @Test
  void aRelationshipToAnExistingNodeInTheSameDirectionConflictsWithOneMergedTogetherWithItsRightNode() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (x:B {id: 2}) MERGE (a)-[r:R]->(x)");

    assertThat(committed).isFalse();
    assertThat(count("MATCH (b:B {id: 7}) RETURN b")).isZero();
  }

  @Test
  void aRelationshipToAnExistingNodeInTheOtherDirectionDoesNotConflictWithOneMergedTogetherWithItsRightNode() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7}) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (x:B {id: 2}) MERGE (a)<-[r:R]-(x)");

    assertThat(committed).isTrue();
    assertThat(count("MATCH (:A {id: 1})-[r:R]-(:B) RETURN r")).isEqualTo(2);
  }

  @Test
  void aRelationshipToAnExistingNodeInTheSameDirectionConflictsWithOneMergedTogetherWithItsLeftNode() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (b:B {id: 2}) MERGE (n:C {id: 9})-[r:R]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:R]->(b)");

    assertThat(committed).isFalse();
    assertThat(count("MATCH (n:C {id: 9}) RETURN n")).isZero();
  }

  @Test
  void aRelationshipToAnExistingNodeInTheOtherDirectionDoesNotConflictWithOneMergedTogetherWithItsLeftNode() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (b:B {id: 2}) MERGE (n:C {id: 9})-[r:R]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (b)-[r:R]->(a)");

    assertThat(committed).isTrue();
    assertThat(count("MATCH (:B {id: 2})-[r:R]-() RETURN r")).isEqualTo(2);
  }

  @Test
  void anIncomingRelationshipToAnExistingNodeConflictsWithOneMergedTogetherWithItsLeftNodeOnlyInTheSameDirection() {
    // (n)<-[r:R]-(b) is a relationship from b: the existing node b has an outgoing one
    assertThat(mergeWithConcurrent("MATCH (b:B {id: 2}) MERGE (n:C {id: 9})<-[r:R]-(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (b)-[r:R]->(a)")).as("outgoing from b").isFalse();
    assertThat(count("MATCH (n:C {id: 9}) RETURN n")).isZero();

    assertThat(mergeWithConcurrent("MATCH (b:B {id: 2}) MERGE (n:C {id: 9})<-[r:R]-(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:R]->(b)")).as("incoming to b").isTrue();
    assertThat(count("MATCH (n:C {id: 9}) RETURN n")).isEqualTo(1);
  }

  /** The statement of a batched loader: the relationship is looked up by the index of its uuid, between two nodes that exist. */
  @Test
  void aRelationshipMergedByTheIndexOfItsKeyBetweenTwoExistingNodesIsNotCreatedTwice() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:RU {uuid: 'u1'}]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:RU {uuid: 'u1'}]->(b)");

    assertThat(committed).as("the second MERGE is refused").isFalse();
    assertThat(count("MATCH (:A {id: 1})-[r:RU {uuid: 'u1'}]->(:B {id: 2}) RETURN r")).isEqualTo(1);
  }

  /** The relationship another statement creates is marked by the commit, whatever clause created it: here a plain CREATE (issue #12483). */
  @Test
  void aRelationshipMergedByTheIndexOfItsKeyConflictsWithAPlainCreateOfTheSameKey() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:RU {uuid: 'u1'}]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) CREATE (a)-[:RU {uuid: 'u1'}]->(b)");

    assertThat(committed).as("the MERGE is refused").isFalse();
    assertThat(count("MATCH (:A {id: 1})-[r:RU {uuid: 'u1'}]->(:B {id: 2}) RETURN r")).isEqualTo(1);
  }

  @Test
  void aRelationshipMergedByTheIndexOfItsKeyDoesNotConflictWithAPlainCreateOfAnotherKey() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:RU {uuid: 'u1'}]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) CREATE (a)-[:RU {uuid: 'u2'}]->(b)");

    assertThat(committed).isTrue();
    assertThat(count("MATCH (:A {id: 1})-[r:RU]->(:B {id: 2}) RETURN r")).isEqualTo(2);
  }

  @Test
  void aRelationshipMergedByTheIndexOfAnotherKeyIsNotAConflict() {
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:RU {uuid: 'u1'}]->(b) ON CREATE SET r.h = race.interleave(r)",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) MERGE (a)-[r:RU {uuid: 'u2'}]->(b)");

    assertThat(committed).isTrue();
    assertThat(count("MATCH (:A {id: 1})-[r:RU]->(:B {id: 2}) RETURN r")).isEqualTo(2);
  }

  @Test
  void aRetryOfTheRefusedMergeMatchesTheCommittedRelationship() {
    final String statement = "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7}) ON CREATE SET r.n = 1 ON MATCH SET r.n = r.n + 1";
    final boolean committed = mergeWithConcurrent(
        "MATCH (a:A {id: 1}) MERGE (a)-[r:R]->(b:B {id: 7}) ON CREATE SET r.n = 1, r.h = race.interleave(r) ON MATCH SET r.n = r.n + 1",
        statement);
    assertThat(committed).isFalse();

    database.transaction(() -> database.command("opencypher", statement).close());

    assertThat(count("MATCH (:A {id: 1})-[r:R]->(:B {id: 7}) WHERE r.n = 2 RETURN r")).as("created once, then matched").isEqualTo(1);
    assertThat(count("MATCH (b:B {id: 7}) RETURN b")).isEqualTo(1);
  }

  private long count(final String query) {
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  /**
   * Runs {@code statement} in a READ_COMMITTED transaction of this thread; when its ON CREATE SET calls race.interleave,
   * {@code concurrent} is committed from another thread and awaited.
   *
   * @return whether the transaction committed; false when it was refused with a retryable conflict
   */
  private boolean mergeWithConcurrent(final String statement, final String concurrent) {
    final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
    Race.PENDING.set(() -> {
      final Thread thread = new Thread(() -> {
        try {
          database.transaction(() -> database.command("opencypher", concurrent).close());
        } catch (final Throwable t) {
          concurrentFailure.set(t);
        }
      });
      thread.start();
      boolean interrupted = false;
      while (thread.isAlive())
        try {
          thread.join();
        } catch (final InterruptedException e) {
          interrupted = true;
        }
      if (interrupted)
        Thread.currentThread().interrupt();
    });
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      database.command("opencypher", statement, Map.of()).close();
      try {
        database.commit();
      } catch (final RuntimeException e) {
        for (Throwable t = e; t != null; t = t.getCause())
          if (t instanceof ConcurrentModificationException)
            return false;
        throw e;
      }
      return true;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      assertThat(Race.PENDING.get()).as("the concurrent commit must have been interleaved").isNull();
      assertThat(concurrentFailure.get()).isNull();
    }
  }
}
