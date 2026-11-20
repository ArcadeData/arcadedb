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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A statement that writes a record its own MATCH or WHERE read, after a concurrent transaction deleted that record and
 * committed: the write is a conflict with that transaction, and a retry, which reads the data again and no longer finds the
 * record, completes. It was answered {@code RecordNotFoundException} instead, which the HTTP API reports as a 404 the caller
 * cannot tell from a wrong RID: 4 to 7 of 900 auto-commit Cypher {@code SET}s racing a {@code DETACH DELETE} of their record.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector for the join of the concurrent commit, not a latency bound
class WriteAfterConcurrentDeleteTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sqlscript", "CREATE VERTEX TYPE R; CREATE PROPERTY R.k STRING; CREATE INDEX ON R (k) UNIQUE; "
        + "CREATE DOCUMENT TYPE D; CREATE PROPERTY D.k STRING; CREATE INDEX ON D (k) UNIQUE; "
        + "CREATE VERTEX TYPE N; CREATE EDGE TYPE E; CREATE PROPERTY E.k STRING; CREATE INDEX ON E (k) UNIQUE");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO R SET k = 'r0', n = 0").close();
      database.command("sql", "INSERT INTO D SET k = 'd0', n = 0").close();
      database.command("sql", "CREATE EDGE E FROM (CREATE VERTEX N SET n = 1) TO (CREATE VERTEX N SET n = 2) SET k = 'e0', n = 0").close();
    });
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

  @Test
  void cypherSetOfAVertexDeletedAfterItsMatchIsARetryableConflict() {
    assertRetryableConflict("DELETE FROM R WHERE k = 'r0'",
        () -> database.command("opencypher", "MATCH (r:R {k: 'r0'}) WHERE race.interleave(r) SET r.n = r.n + 1 RETURN r.k").close());
  }

  @Test
  void sqlUpdateOfAVertexDeletedAfterItsWhereIsARetryableConflict() {
    assertRetryableConflict("DELETE FROM R WHERE k = 'r0'",
        () -> database.command("sql", "UPDATE R SET n = n + 1 WHERE k = 'r0' AND `race.interleave`(@rid)").close());
  }

  @Test
  void sqlUpdateOfADocumentDeletedAfterItsWhereIsARetryableConflict() {
    assertRetryableConflict("DELETE FROM D WHERE k = 'd0'",
        () -> database.command("sql", "UPDATE D SET n = n + 1 WHERE k = 'd0' AND `race.interleave`(@rid)").close());
  }

  @Test
  void sqlUpdateOfAnEdgeDeletedAfterItsWhereIsARetryableConflict() {
    assertRetryableConflict("DELETE FROM E WHERE k = 'e0'",
        () -> database.command("sql", "UPDATE E SET n = n + 1 WHERE k = 'e0' AND `race.interleave`(@rid)").close());
  }

  /** The same, for an edge read by a scan of its bucket (its content is read straight from the page) and not by an index. */
  @Test
  void sqlUpdateOfAnEdgeFoundByAScanAndDeletedAfterItsWhereIsARetryableConflict() {
    assertRetryableConflict("DELETE FROM E WHERE k = 'e0'",
        () -> database.command("sql", "UPDATE E SET n = n + 1 WHERE n = 0 AND `race.interleave`(@rid)").close());
  }

  /**
   * A record handed out by a scan reads its content straight from the page, so modify() reloads it when the page moved on since.
   * A vertex drops the content it read to force that reload, and the reload of a record a concurrent commit deleted is where the
   * not-found came from. A document and an edge keep the content they read when the reload finds the record gone, so they never
   * reached it: the write is refused at commit, as a conflict. Pinned for the three kinds, so that no kind answers differently.
   */
  @Test
  void aDocumentFoundByAScanAndDeletedAfterwardsIsARetryableConflictAtModify() {
    assertRetryableConflict("DELETE FROM D WHERE k = 'd0'", () -> {
      final Document document = database.iterateType("D", false).next().asDocument();
      Race.interleave(document);
      document.modify().set("n", 1).save();
    });
  }

  @Test
  void anEdgeFoundByAScanAndDeletedAfterwardsIsARetryableConflictAtModify() {
    assertRetryableConflict("DELETE FROM E WHERE k = 'e0'", () -> {
      final Edge edge = database.iterateType("E", false).next().asEdge();
      Race.interleave(edge);
      edge.modify().set("n", 1).save();
    });
  }

  @Test
  void aVertexFoundByAScanAndDeletedAfterwardsIsARetryableConflictAtModify() {
    assertRetryableConflict("DELETE FROM R WHERE k = 'r0'", () -> {
      final Vertex vertex = database.iterateType("R", false).next().asVertex();
      Race.interleave(vertex);
      vertex.modify().set("n", 1).save();
    });
  }

  /**
   * An edge created on a vertex this transaction read, which a concurrent commit deleted since: the vertex's edge list is
   * written through {@code modify()}, so it is the same conflict, where it was the non-retryable {@code VertexNotFoundException}
   * of a RID that does not exist. A retry reads the vertex again and finds it gone.
   */
  @Test
  void anEdgeFromAVertexDeletedAfterItWasReadIsARetryableConflict() {
    final RID target = database.query("sql", "SELECT FROM N WHERE n = 2").next().getIdentity().get();
    final RID source = database.query("sql", "SELECT FROM R WHERE k = 'r0'").next().getIdentity().get();
    assertRetryableConflict("DELETE FROM R WHERE k = 'r0'", () -> {
      final Vertex vertex = database.lookupByRID(source, true).asVertex();
      Race.interleave(vertex);
      vertex.newEdge("E", target);
    });
  }

  /** With retries, the retry reads the data again, finds no record and completes: what the conflict is reported for. */
  @Test
  void aRetriedCypherSetCompletesWithoutTheDeletedVertex() {
    armConcurrentCommit("DELETE FROM R WHERE k = 'r0'");
    final AtomicReference<Long> rows = new AtomicReference<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (r:R {k: 'r0'}) WHERE race.interleave(r) SET r.n = r.n + 1 RETURN r.k")) {
        rows.set(rs.stream().count());
      }
    }, false, 3);

    assertThat(Race.PENDING.get()).isNull();
    assertThat(rows.get()).isZero();
    assertThat(database.query("sql", "SELECT count(@rid) AS c FROM R").next().<Long>getProperty("c")).isZero();
  }

  /** A record read by an earlier transaction and deleted since is gone for good: no retry can find it, it stays not-found. */
  @Test
  void aVertexReadInAnEarlierTransactionKeepsTheNotFound() {
    final Vertex[] held = new Vertex[1];
    database.transaction(() -> held[0] = database.select().fromType("R").where().property("k").eq().value("r0").vertices().next());
    database.transaction(() -> database.command("sql", "DELETE FROM R WHERE k = 'r0'").close());

    assertThatThrownBy(() -> database.transaction(() -> held[0].modify().set("n", 1).save(), false, 0))
        .isInstanceOf(RecordNotFoundException.class);
  }

  /**
   * A record only referenced in this transaction (lookupByRID without loading its content, which stamps it without reading the
   * bucket) proves nothing was read: deleted before the transaction began, it stays not-found, never a conflict a retry repeats.
   */
  @Test
  void anUnloadedLookupOfARecordDeletedBeforeTheTransactionKeepsTheNotFound() {
    final RID vertex = database.query("sql", "SELECT FROM R WHERE k = 'r0'").next().getIdentity().get();
    final RID document = database.query("sql", "SELECT FROM D WHERE k = 'd0'").next().getIdentity().get();
    database.transaction(() -> {
      database.command("sql", "DELETE FROM R WHERE k = 'r0'").close();
      database.command("sql", "DELETE FROM D WHERE k = 'd0'").close();
    });

    for (final RID rid : new RID[] { vertex, document })
      assertThatThrownBy(() -> database.transaction(() -> database.lookupByRID(rid, false).asDocument().modify().set("n", 1).save(),
          false, 0)).as("modify of %s", rid).isInstanceOf(RecordNotFoundException.class);
  }

  /** Run in a transaction that commits {@code concurrentCommand} from another thread when {@code body} calls race.interleave. */
  private void assertRetryableConflict(final String concurrentCommand, final Runnable body) {
    final AtomicReference<Throwable> concurrentFailure = armConcurrentCommit(concurrentCommand);

    Throwable failure = null;
    try {
      database.transaction(body::run, false, 0);
    } catch (final RuntimeException e) {
      failure = e;
    }

    assertThat(Race.PENDING.get()).as("the concurrent delete ran").isNull();
    assertThat(concurrentFailure.get()).isNull();
    assertThat(failure).as("the write failed, as it must: its record is gone").isNotNull();
    Throwable conflict = failure;
    while (conflict != null && !(conflict instanceof ConcurrentModificationException))
      conflict = conflict.getCause();
    assertThat(conflict).as("reported as a retryable conflict, got %s", failure).isInstanceOf(ConcurrentModificationException.class);
  }

  /** The next call of race.interleave commits {@code concurrentCommand} from another thread and waits for it. */
  private AtomicReference<Throwable> armConcurrentCommit(final String concurrentCommand) {
    final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
    Race.PENDING.set(() -> {
      final Thread concurrent = new Thread(() -> {
        try {
          database.transaction(() -> database.command("sql", concurrentCommand).close());
        } catch (final Throwable t) {
          concurrentFailure.set(t);
        }
      });
      concurrent.start();
      try {
        concurrent.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    return concurrentFailure;
  }

  /** Called last in the WHERE of the statement under test: runs the armed concurrent commit, once, and answers true. */
  public static class Race {
    static final AtomicReference<Runnable> PENDING = new AtomicReference<>();

    public static boolean interleave(final Object row) {
      final Runnable commit = PENDING.getAndSet(null);
      if (commit != null)
        commit.run();
      return true;
    }
  }
}
