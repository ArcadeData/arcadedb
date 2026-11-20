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
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11818: a conditional update was not atomic. When another transaction committed a change to a record after the statement
 * evaluated its WHERE on it and before the statement wrote it, the statement wrote it anyway: {@code modify()} reloads the latest
 * committed content (and pins the page the commit is checked against), SQL UPDATE, openCypher SET and REMOVE build on it (issues
 * #5227, #8538, #8610), but the WHERE that selected the record was never evaluated again. Two concurrent
 * {@code UPDATE R SET claimed = :me WHERE k = :k AND claimed IS NULL} both claimed the record and both committed.
 * <p>
 * A conditional DELETE is covered by the refusal of issue #12472: a delete never reloads its record, so one of a record a
 * concurrent commit changed since it was read is a retryable conflict, and the retry evaluates the WHERE on the new state. The
 * delete cases here pin that for the shapes of this issue.
 * <p>
 * Each case runs the statement in a READ_COMMITTED transaction of the test thread; the function {@code race.interleave(row)},
 * called last in its WHERE, commits the concurrent claim from another thread, so the commit lands after the WHERE read the record
 * and before the write. The concurrent claim must survive: the statement writes nothing, or is refused with a retryable
 * conflict that a retry turns into "writes nothing".
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES) // a hang detector, not a latency bound
class Issue11818ConditionalUpdateTest extends TestHelper {
  private static final String CONCURRENT_CLAIM = "UPDATE R SET claimed = 'A' WHERE k = 'r0' AND claimed IS NULL";
  private static final String SQL_CLAIM        = "UPDATE R SET claimed = :who WHERE k = :k AND claimed IS NULL";
  private static final String CYPHER_CLAIM     = "MATCH (r:R {k: $k}) WHERE r.claimed IS NULL SET r.claimed = $who RETURN r.k AS k";

  @Override
  protected void beginTest() {
    database.command("sqlscript", "CREATE VERTEX TYPE R; CREATE PROPERTY R.k STRING; CREATE INDEX ON R (k) UNIQUE; CREATE DOCUMENT TYPE D; "
        + "CREATE DOCUMENT TYPE DI; CREATE PROPERTY DI.k STRING; CREATE INDEX ON DI (k) UNIQUE; CREATE EDGE TYPE E");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO R SET k = 'r0', n = 0, state = 'open'").close();
      database.command("sql", "INSERT INTO D SET k = 'd0', n = 0").close();
      database.command("sql", "INSERT INTO DI SET k = 'i0', n = 0").close();
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

  /**
   * The reported shape: the record is found through the unique index, and a vertex is reloaded by {@code modify()} every time.
   */
  @Test
  void sqlUpdateDoesNotClaimAVertexClaimedAfterItsWhere() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> updated.set(countOf(database.command("sql",
        "UPDATE R SET claimed = :who WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("k", "r0", "who", "B")))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).as("the record no longer matches the WHERE").isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  @Test
  void sqlUpdateDoesNotClaimAScannedVertexClaimedAfterItsWhere() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> updated.set(countOf(database.command("sql",
        "UPDATE R SET claimed = :who WHERE n = 0 AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("who", "B")))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * A document read by a scan is reloaded by {@code modify()} when its page moved on (issue #8312), so it lost the claim the same way.
   */
  @Test
  void sqlUpdateDoesNotClaimAScannedDocumentClaimedAfterItsWhere() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE D SET claimed = 'A' WHERE k = 'd0' AND claimed IS NULL",
        () -> updated.set(countOf(database.command("sql",
            "UPDATE D SET claimed = :who WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("k", "d0", "who", "B")))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isZero();
    assertThat(valueOf("D", "claimed")).isEqualTo("A");
  }

  /**
   * #8610 must survive the fix: a WHERE that still holds on the reloaded record lets the UPDATE build on the concurrent commit.
   */
  @Test
  void sqlUpdateWhoseWhereStillHoldsBuildsOnTheConcurrentCommit() {
    final AtomicLong updated = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> updated.set(countOf(database.command(
        "sql", "UPDATE R SET n = n + 1 WHERE k = 'r0' AND claimed IS NULL AND `race.interleave`(@rid)"))));

    assertThat(committed).isTrue();
    assertThat(updated.get()).isEqualTo(1);
    assertThat(((Number) valueOf("R", "n")).intValue()).isEqualTo(6);
  }

  /**
   * An UPSERT that loses the record it found would neither update nor insert: it is refused with a retryable conflict instead.
   */
  @Test
  void sqlUpsertOfAVertexClaimedAfterItsWhereIsRefused() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("sql",
        "UPDATE R SET claimed = 'B' UPSERT WHERE k = 'r0' AND claimed IS NULL AND `race.interleave`(@rid)").close());

    assertThat(committed).isFalse();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * The reported openCypher shape. A conflict is the only answer a SET can give once earlier clauses may have written for the row,
   * and the retry runs the statement against the new state (see {@link #retriedCypherClaimFindsTheRecordClaimed()}).
   */
  @Test
  void cypherSetDoesNotClaimAVertexClaimedAfterItsWhere() {
    final AtomicLong rows = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> rows.set(countRows(database.command("opencypher",
        "MATCH (r:R {k: $k}) WHERE r.claimed IS NULL AND race.interleave(r) SET r.claimed = $who RETURN r.k AS k",
        Map.of("k", "r0", "who", "B")))));

    if (committed)
      assertThat(rows.get()).isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * The job-queue claim: the WHERE is in front of a WITH that passes the variable on.
   */
  @Test
  void cypherSetAfterAWithDoesNotClaimAVertexClaimedAfterItsWhere() {
    final AtomicLong rows = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> rows.set(countRows(database.command("opencypher",
        "MATCH (j:R) WHERE j.claimed IS NULL AND race.interleave(j) WITH j LIMIT 1 SET j.claimed = $who RETURN j.k AS k",
        Map.of("who", "B")))));

    if (committed)
      assertThat(rows.get()).isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * The inline property map of the pattern selects the record as much as the WHERE does.
   */
  @Test
  void cypherSetDoesNotWriteAVertexWhoseInlinePropertiesChangedAfterTheMatch() {
    final AtomicLong rows = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE R SET state = 'closed' WHERE k = 'r0'", () -> rows.set(countRows(
        database.command("opencypher", "MATCH (r:R {k: 'r0', state: 'open'}) WHERE race.interleave(r) SET r.claimed = 'B' RETURN r.k",
            Map.of()))));

    if (committed)
      assertThat(rows.get()).isZero();
    assertThat(valueOf("R", "claimed")).isNull();
    assertThat(valueOf("R", "state")).isEqualTo("closed");
  }

  /**
   * #5227 and #8538 must survive the fix: a SET whose predicates still hold on the reloaded record builds on the concurrent commit.
   */
  @Test
  void cypherSetWhosePredicatesStillHoldBuildsOnTheConcurrentCommit() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race.interleave(r) SET r.n = r.n + 1", Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(((Number) valueOf("R", "n")).intValue()).isEqualTo(6);
  }

  /**
   * An inline property map selects the target through its values too: {@code b} was matched on the {@code n} of {@code r} as read,
   * and a concurrent commit changed it, so {@code b} no longer selects {@code r} although the map belongs to {@code b}. The WITH
   * runs the concurrent commit once both are bound.
   */
  @Test
  void cypherSetDoesNotWriteAVertexThatAnInlineMapOfAnotherVariableNoLongerMatches() {
    database.transaction(() -> database.command("sql", "INSERT INTO R SET k = 'b0', m = 0").close());
    final AtomicLong rows = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 7 WHERE k = 'r0'", () -> rows.set(countRows(database.command(
        "opencypher", "MATCH (r:R {k: 'r0'}) MATCH (b:R {m: r.n}) WITH r, b WHERE race.interleave(b) SET r.claimed = 'B' RETURN r.k",
        Map.of()))));

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(rows.get()).isEqualTo(-1);
    try (final ResultSet rs = database.query("sql", "SELECT claimed, n FROM R WHERE k = 'r0'")) {
      final Result record = rs.next();
      assertThat(record.<String>getProperty("claimed")).isNull();
      assertThat(((Number) record.getProperty("n")).intValue()).isEqualTo(7);
    }
  }

  /**
   * A WITH that drops a variable an inline map value reads drops the map: it can no longer be evaluated against the row, and
   * evaluating it with the variable missing refused a write whose predicates all still held.
   */
  @Test
  void cypherSetAfterAWithThatDropsAnInlineMapValueBuildsOnTheConcurrentCommit() {
    final AtomicLong rows = new AtomicLong(-1);
    final boolean committed = runWithConcurrentCommit("UPDATE R SET state = 'closed' WHERE k = 'r0'", () -> rows.set(countRows(
        database.command("opencypher", "WITH 0 AS v MATCH (r:R {n: v}) WHERE race.interleave(r) WITH r SET r.claimed = 'B' RETURN r.k",
            Map.of()))));

    assertThat(committed).isTrue();
    assertThat(rows.get()).isEqualTo(1);
    assertThat(valueOf("R", "claimed")).isEqualTo("B");
    assertThat(valueOf("R", "state")).isEqualTo("closed");
  }

  /**
   * A WITH drops only the entries of an inline map that read a variable it drops: {@code state: 'open'} still selects {@code r}.
   */
  @Test
  void cypherSetAfterAWithThatDropsOneInlineMapValueStillChecksTheOtherEntries() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET state = 'closed' WHERE k = 'r0'", () -> database.command(
        "opencypher", "WITH 0 AS v MATCH (r:R {state: 'open', n: v}) WHERE race.interleave(r) WITH r SET r.claimed = 'B'", Map.of())
        .close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(valueOf("R", "claimed")).isNull();
  }

  /**
   * The names a reduce() or a list comprehension binds are no variables of the row, so a WITH does not drop an inline map that reads
   * them.
   */
  @Test
  void cypherSetAfterAWithChecksAnInlineMapValueThatBindsLocalNames() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 7 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {n: reduce(s = 0, x IN [y IN [0] | y] | s + x)}) WHERE race.interleave(r) WITH r SET r.claimed = 'B'", Map.of())
        .close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(valueOf("R", "claimed")).isNull();
  }

  /** The same names bound inside an expression the collector does not model arm by arm (a CASE), which a WITH must not drop either. */
  @Test
  void cypherSetAfterAWithChecksAnInlineMapValueThatBindsLocalNamesInsideACase() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 7 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {n: CASE WHEN true THEN reduce(s = 0, x IN [0] | s + x) ELSE 0 END}) WHERE race.interleave(r) WITH r "
            + "SET r.claimed = 'B'", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(valueOf("R", "claimed")).isNull();
  }

  /**
   * A REMOVE builds on a concurrent commit as a SET does, so its predicates are checked the same way: releasing a lease the MATCH
   * found held by B must not release the lease A took over in between.
   */
  @Test
  void cypherRemoveDoesNotReleaseALeaseTakenOverAfterItsWhere() {
    database.transaction(() -> database.command("sql", "UPDATE R SET owner = 'B' WHERE k = 'r0'").close());
    runWithConcurrentCommit("UPDATE R SET owner = 'A' WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.owner = 'B' AND race.interleave(r) REMOVE r.owner", Map.of()).close());

    assertThat(valueOf("R", "owner")).isEqualTo("A");
  }

  @Test
  void cypherRemoveWhosePredicatesStillHoldCommitsOverTheConcurrentCommit() {
    database.transaction(() -> database.command("sql", "UPDATE R SET owner = 'B' WHERE k = 'r0'").close());
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.owner = 'B' AND race.interleave(r) REMOVE r.owner", Map.of()).close());

    assertThat(committed).isTrue();
    assertThat(valueOf("R", "owner")).isNull();
    assertThat(((Number) valueOf("R", "n")).intValue()).isEqualTo(5);
  }

  /**
   * A DELETE does not build on a concurrent commit as an UPDATE does: the delete of a record changed since its WHERE read it is
   * refused with a retryable conflict (issue #12472), and the claimed record stays. The retry evaluates the WHERE on the new state
   * (see {@link #retriedSqlDeleteFindsTheRecordClaimed()}).
   */
  @Test
  void sqlDeleteDoesNotDeleteAVertexClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("sql",
        "DELETE FROM R WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("k", "r0")).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("R")).isEqualTo(1);
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /** The conflict is retryable: the retry the auto-commit path runs finds the record claimed and deletes nothing. */
  @Test
  void retriedSqlDeleteFindsTheRecordClaimed() {
    final AtomicReference<Throwable> concurrentFailure = armConcurrentCommit(CONCURRENT_CLAIM);
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicLong deleted = new AtomicLong(-1);
    database.transaction(() -> {
      attempts.incrementAndGet();
      deleted.set(countOf(database.command("sql", "DELETE FROM R WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)",
          Map.of("k", "r0"))));
    }, false, 3);

    assertThat(Race.PENDING.get()).isNull();
    assertThat(concurrentFailure.get()).isNull();
    assertThat(attempts.get()).isEqualTo(2);
    assertThat(deleted.get()).isZero();
    assertThat(recordsOf("R")).isEqualTo(1);
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  @Test
  void sqlDeleteDoesNotDeleteAScannedDocumentClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit("UPDATE D SET claimed = 'A' WHERE k = 'd0' AND claimed IS NULL",
        () -> database.command("sql", "DELETE FROM D WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("k", "d0"))
            .close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("D")).isEqualTo(1);
    assertThat(valueOf("D", "claimed")).isEqualTo("A");
  }

  /** And for a document found through an index. */
  @Test
  void sqlDeleteDoesNotDeleteAnIndexedDocumentClaimedAfterItsWhere() {
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET claimed = 'A' WHERE k = 'i0' AND claimed IS NULL",
        () -> database.command("sql", "DELETE FROM DI WHERE k = :k AND claimed IS NULL AND `race.interleave`(@rid)", Map.of("k", "i0"))
            .close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("DI")).isEqualTo(1);
    assertThat(valueOf("DI", "claimed")).isEqualTo("A");
  }

  /**
   * A WHERE that still holds on the changed record is a conflict all the same: the delete holds the content it read, and deleting
   * with it left the index entry of the key committed in between pointing to the deleted record. The retry deletes the record as it
   * is now, with every index entry of it.
   */
  @Test
  void sqlDeleteWhoseWhereStillHoldsIsRetriedAndRemovesTheIndexEntriesOfTheCurrentContent() {
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET k = 'i9' WHERE k = 'i0'", () -> database.command("sql",
        "DELETE FROM DI WHERE n = 0 AND `race.interleave`(@rid)").close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("DI")).isEqualTo(1);
    assertIndexHolds("DI[k]", "i9");

    database.transaction(() -> database.command("sql", "DELETE FROM DI WHERE n = 0").close());
    assertThat(recordsOf("DI")).isZero();
    assertIndexHolds("DI[k]");
  }

  /**
   * A delete with no condition of a record whose indexed key a concurrent commit changed after it was read: an openCypher DETACH
   * DELETE with no predicate removed the index entry of the key it read and left the entry of the key committed in between pointing
   * to the deleted vertex (127 of 300 rounds over HTTP). It is refused with a retryable conflict, which leaves the index whole; the
   * retry deletes the record and every entry of it.
   */
  @Test
  void cypherDeleteWithNoPredicateOfAVertexChangedAfterItsReadIsARetryableConflict() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET k = 'r9' WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R) WITH r, race.interleave(r) AS raced DETACH DELETE r", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("R")).isEqualTo(1);
    assertIndexHolds("R[k]", "r9");

    database.transaction(() -> database.command("opencypher", "MATCH (r:R) DETACH DELETE r").close());
    assertThat(recordsOf("R")).isZero();
    assertIndexHolds("R[k]");
  }

  /** And for Record.delete() of a record the transaction read before the concurrent commit. */
  @Test
  void recordDeleteOfADocumentChangedAfterItsReadIsARetryableConflict() {
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET k = 'i9' WHERE k = 'i0'", () -> {
      final Document read = database.query("sql", "SELECT FROM DI WHERE k = 'i0'").next().getElement().orElseThrow();
      Race.interleave(read);
      read.delete();
    });

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("DI")).isEqualTo(1);
    assertIndexHolds("DI[k]", "i9");
  }

  /** And for the delete of the mutable copy modify() made before the concurrent commit, whether its properties were read or not. */
  @Test
  void deleteOfAModifiedDocumentChangedAfterItsReadIsARetryableConflict() {
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET k = 'i9' WHERE k = 'i0'", () -> {
      final MutableDocument modified = database.query("sql", "SELECT FROM DI WHERE k = 'i0'").next().getElement().orElseThrow().modify();
      Race.interleave(modified);
      modified.delete();
    });

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("DI")).isEqualTo(1);
    assertIndexHolds("DI[k]", "i9");
  }

  @Test
  void deleteOfAModifiedDocumentWhosePropertiesWereReadAndChangedAfterItsReadIsARetryableConflict() {
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET k = 'i9' WHERE k = 'i0'", () -> {
      final MutableDocument modified = database.query("sql", "SELECT FROM DI WHERE k = 'i0'").next().getElement().orElseThrow().modify();
      assertThat(modified.getInteger("n")).isZero();
      Race.interleave(modified);
      modified.delete();
    });

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("DI")).isEqualTo(1);
    assertIndexHolds("DI[k]", "i9");
  }

  /** A reload() of the mutable copy reads the content committed in between, which is what its delete then removes: it goes on. */
  @Test
  void deleteOfAModifiedDocumentReloadedAfterAConcurrentCommitGoesOn() {
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET k = 'i9' WHERE k = 'i0'", () -> {
      final MutableDocument modified = database.query("sql", "SELECT FROM DI WHERE k = 'i0'").next().getElement().orElseThrow().modify();
      Race.interleave(modified);
      modified.reload();
      assertThat(modified.getString("k")).isEqualTo("i9");
      modified.delete();
    });

    assertThat(committed).isTrue();
    assertThat(recordsOf("DI")).isZero();
    assertIndexHolds("DI[k]");
  }

  /**
   * A mutable copy saved by an earlier transaction and deleted by a later one is checked against the content it wrote: a commit in
   * between changed the key, so deleting with the index entries of the saved content would leave the new key's entry behind.
   */
  @Test
  void deleteOfAModifiedDocumentSavedByAnEarlierTransactionAndChangedSinceIsARetryableConflict() {
    final AtomicReference<MutableDocument> saved = new AtomicReference<>();
    database.transaction(() -> saved.set(
        database.query("sql", "SELECT FROM DI WHERE k = 'i0'").next().getElement().orElseThrow().modify().set("n", 1).save()));
    final boolean committed = runWithConcurrentCommit("UPDATE DI SET k = 'i9' WHERE k = 'i0'", () -> {
      Race.interleave(saved.get());
      saved.get().delete();
    });

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("DI")).isEqualTo(1);
    assertIndexHolds("DI[k]", "i9");
  }

  /** And deleted unchanged by a later transaction, it is deleted with every index entry of it. */
  @Test
  void deleteOfAModifiedDocumentSavedByAnEarlierTransactionGoesOn() {
    final AtomicReference<MutableDocument> saved = new AtomicReference<>();
    database.transaction(() -> saved.set(
        database.query("sql", "SELECT FROM DI WHERE k = 'i0'").next().getElement().orElseThrow().modify().set("k", "i1").save()));
    database.transaction(() -> saved.get().delete());

    assertThat(recordsOf("DI")).isZero();
    assertIndexHolds("DI[k]");
  }

  /**
   * A vertex whose edge lists a commit changed after its read is refused with a retryable conflict (issue #12472), as any other
   * record a concurrent commit changed: the edge committed in between stays, and so does the vertex, for the retry to delete.
   */
  @Test
  void deleteOfAVertexWhoseEdgesChangedAfterItsReadIsARetryableConflict() {
    database.transaction(() -> database.command("sql", "INSERT INTO R SET k = 'other'").close());
    final boolean committed = runWithConcurrentCommit(
        "CREATE EDGE E FROM (SELECT FROM R WHERE k = 'other') TO (SELECT FROM R WHERE k = 'r0')", () -> {
          final Document read = database.query("sql", "SELECT FROM R WHERE k = 'r0'").next().getElement().orElseThrow();
          Race.interleave(read);
          read.delete();
        });

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("R")).isEqualTo(2);
    assertThat(recordsOf("E")).as("the edge committed in between stays").isEqualTo(1);
    assertIndexHolds("R[k]", "other", "r0");
  }

  private void assertIndexHolds(final String index, final String... keys) {
    final List<String> found = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT key FROM index:`" + index + "`")) {
      while (rs.hasNext())
        found.add(String.valueOf(((Object[]) rs.next().getProperty("key"))[0]));
    }
    assertThat(found).as("entries of %s", index).containsExactlyInAnyOrder(keys);
  }

  /** The openCypher DELETE shapes of the issue: the claimed vertex stays, whether or not the statement is refused with the conflict. */
  @Test
  void cypherDeleteDoesNotDeleteAVertexClaimedAfterItsWhere() {
    runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race.interleave(r) DELETE r", Map.of()).close());

    assertThat(recordsOf("R")).isEqualTo(1);
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  @Test
  void cypherDetachDeleteDoesNotDeleteAVertexClaimedAfterItsWhere() {
    database.transaction(() -> database.command("opencypher", "MATCH (r:R {k: 'r0'}) CREATE (r)-[:E]->(:R {k: 'r1'})").close());
    runWithConcurrentCommit(CONCURRENT_CLAIM, () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race.interleave(r) DETACH DELETE r", Map.of()).close());

    assertThat(recordsOf("R")).isEqualTo(2);
    assertThat(recordsOf("E")).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "SELECT claimed FROM R WHERE k = 'r0'")) {
      assertThat(rs.next().<String>getProperty("claimed")).isEqualTo("A");
    }
  }

  /** A DELETE whose predicates still hold on the changed record is refused too (issue #12472), and its retry deletes the record. */
  @Test
  void cypherDeleteWhosePredicatesStillHoldIsRetriedAndDeletes() {
    final boolean committed = runWithConcurrentCommit("UPDATE R SET n = 5 WHERE k = 'r0'", () -> database.command("opencypher",
        "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL AND race.interleave(r) DELETE r", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("R")).isEqualTo(1);

    database.transaction(() -> database.command("opencypher", "MATCH (r:R {k: 'r0'}) WHERE r.claimed IS NULL DELETE r").close());
    assertThat(recordsOf("R")).isZero();
  }

  /**
   * A DELETE whose predicates read another variable than its target must not delete what a concurrent commit put at the target's
   * RID: nothing of the statement selected that record. The target is refused as changed since its read, never reloaded.
   */
  @Test
  void cypherDeleteOfAnEdgeChangedAfterItsReadUnderAPredicateOnItsVertexIsARetryableConflict() {
    database.transaction(() -> database.command("opencypher", "MATCH (r:R {k: 'r0'}) CREATE (r)-[:E {w: 1}]->(:R {k: 'r1'})").close());
    final boolean committed = runWithConcurrentCommit("UPDATE E SET w = 2", () -> database.command("opencypher",
        "MATCH (a:R {k: 'r0'})-[e:E]->(b) WHERE a.claimed IS NULL AND race.interleave(e) DELETE e", Map.of()).close());

    assertThat(committed).as("refused with a retryable conflict").isFalse();
    assertThat(recordsOf("E")).isEqualTo(1);
  }

  /**
   * A record deleted concurrently between the read and the delete is still the retryable conflict of #8984: reloading a vertex that
   * is gone must not leave it without content, which turned the conflict into a "record not found" no retry handles.
   */
  @Test
  void sqlDeleteOfAVertexDeletedAfterItsWhereIsARetryableConflict() {
    final boolean committed = runWithConcurrentCommit("DELETE FROM R WHERE k = 'r0'", () -> database.command("sql",
        "DELETE FROM R WHERE k = 'r0' AND claimed IS NULL AND `race.interleave`(@rid)").close());

    assertThat(committed).isFalse();
    assertThat(recordsOf("R")).isZero();
  }

  /**
   * The conflict is retryable: the retry the auto-commit path runs finds the record claimed and claims nothing.
   */
  @Test
  void retriedCypherClaimFindsTheRecordClaimed() {
    final AtomicReference<Throwable> concurrentFailure = armConcurrentCommit(CONCURRENT_CLAIM);
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicLong rows = new AtomicLong(-1);
    database.transaction(() -> {
      attempts.incrementAndGet();
      rows.set(countRows(database.command("opencypher",
          "MATCH (r:R {k: $k}) WHERE r.claimed IS NULL AND race.interleave(r) SET r.claimed = $who RETURN r.k AS k",
          Map.of("k", "r0", "who", "B"))));
    }, false, 3);

    assertThat(Race.PENDING.get()).isNull();
    assertThat(concurrentFailure.get()).isNull();
    assertThat(attempts.get()).isEqualTo(2);
    assertThat(rows.get()).isZero();
    assertThat(valueOf("R", "claimed")).isEqualTo("A");
  }

  /**
   * The reporter's shape, without forcing the interleaving: several claims of each record at once, each in an auto-retried
   * transaction. Exactly one of them claims the record, and the record holds its claim.
   */
  @Test
  void concurrentSqlClaimsHaveOneWinner() throws Exception {
    assertOneWinnerPerRecord("sql", SQL_CLAIM);
  }

  @Test
  void concurrentCypherClaimsHaveOneWinner() throws Exception {
    assertOneWinnerPerRecord("opencypher", CYPHER_CLAIM);
  }

  /** Called last in the WHERE of the statements under test: runs the armed concurrent commit, once, and answers true. */
  public static class Race {
    static final AtomicReference<Runnable> PENDING = new AtomicReference<>();

    public static boolean interleave(final Object row) {
      final Runnable commit = PENDING.getAndSet(null);
      if (commit != null)
        commit.run();
      return true;
    }
  }

  private void assertOneWinnerPerRecord(final String language, final String claim) throws Exception {
    final int records = 40;
    final int claimants = 4;
    database.transaction(() -> {
      for (int i = 1; i <= records; i++)
        database.command("sql", "INSERT INTO R SET k = ?", "c" + i).close();
    });

    for (int i = 1; i <= records; i++) {
      final String key = "c" + i;
      final CyclicBarrier start = new CyclicBarrier(claimants);
      final List<String> winners = new ArrayList<>();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final List<Thread> threads = new ArrayList<>();
      for (int c = 0; c < claimants; c++) {
        final String who = "w" + c;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            final AtomicLong claimed = new AtomicLong();
            database.transaction(() -> {
              try (final ResultSet rs = database.command(language, claim, Map.of("k", key, "who", who))) {
                claimed.set(language.equals("sql") ? ((Number) rs.next().getProperty("count")).longValue() : rs.stream().count());
              }
            }, false, 50);
            if (claimed.get() > 0)
              synchronized (winners) {
                winners.add(who);
              }
          } catch (final Throwable t) {
            failure.compareAndSet(null, t);
          }
        });
        threads.add(thread);
        thread.start();
      }
      for (final Thread thread : threads)
        thread.join();

      assertThat(failure.get()).isNull();
      assertThat(winners).as("claims of %s that committed", key).hasSize(1);
      try (final ResultSet rs = database.query("sql", "SELECT claimed FROM R WHERE k = ?", key)) {
        assertThat(rs.next().<String>getProperty("claimed")).isEqualTo(winners.getFirst());
      }
    }
  }

  private long recordsOf(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private Object valueOf(final String type, final String property) {
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " AS value FROM " + type)) {
      return rs.next().getProperty("value");
    }
  }

  private static long countOf(final ResultSet rs) {
    try (rs) {
      return ((Number) rs.next().getProperty("count")).longValue();
    }
  }

  private static long countRows(final ResultSet rs) {
    try (rs) {
      return rs.stream().count();
    }
  }

  /** The next call of {@code race.interleave} commits {@code concurrentCommand} from another thread and waits for it. */
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

  /**
   * Runs {@code body} in a READ_COMMITTED transaction, with {@code concurrentCommand} committed from another thread when the body
   * calls {@code race.interleave}.
   *
   * @return whether the transaction committed; false when it was refused with a retryable conflict
   */
  private boolean runWithConcurrentCommit(final String concurrentCommand, final Runnable body) {
    final AtomicReference<Throwable> concurrentFailure = armConcurrentCommit(concurrentCommand);
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      body.run();
      database.commit();
      return true;
    } catch (final RuntimeException e) {
      // a query engine may report the conflict wrapped in its own exception
      for (Throwable t = e; t != null; t = t.getCause())
        if (t instanceof ConcurrentModificationException)
          return false;
      throw e;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      assertThat(Race.PENDING.get()).as("the concurrent commit must have been interleaved").isNull();
      assertThat(concurrentFailure.get()).isNull();
    }
  }
}
