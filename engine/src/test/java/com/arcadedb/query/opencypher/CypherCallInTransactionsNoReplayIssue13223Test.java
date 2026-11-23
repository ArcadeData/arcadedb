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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13223: the server wrappers (HTTP command handler, gRPC ExecuteCommand, MCP execute_command) run a statement in an implicit
 * transaction they open with {@code database.transaction(block, false, retries)}, and retry the whole block on a
 * {@code NeedRetryException}. The batches of a {@code CALL { ... } IN TRANSACTIONS} commit on their own, in a transaction nested in
 * that implicit one, so a conflict raised after them (on the wrapper's final commit of the writes that follow the CALL, for example)
 * replayed the statement and applied the committed batches a second time. The retry loop now sees them as a partial commit of the
 * block and refuses the replay, as it does for a {@code BATCH n} boundary (#7916, #8188).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherCallInTransactionsNoReplayIssue13223Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("U13223");
      type.createProperty("k", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "U13223", "k");
      database.getSchema().createVertexType("B13223");
      database.getSchema().createVertexType("Pre13223");
      database.command("opencypher", "CREATE (:U13223 {k: 1})");
    });
  }

  /** The post-CALL write breaks a unique key, which the wrapper's commit finds and the retry loop would retry once. */
  @Test
  void committedBatchesAreNotReplayedWhenTheWrapperCommitConflicts() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> serverWrapper(attempts, 3, "UNWIND range(1, 2) AS i "//
        + "CALL { WITH i CREATE (:B13223 {i: i}) } IN TRANSACTIONS OF 1 ROWS "//
        + "WITH count(*) AS c CREATE (:U13223 {k: 1})")).isInstanceOf(DuplicatedKeyException.class);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(count("B13223")).isEqualTo(2);
    assertThat(count("U13223")).isEqualTo(1);
  }

  /** A conflict raised after the statement returned, still inside the wrapper's block, is not retried either. */
  @Test
  void committedBatchesAreNotReplayedOnALaterConflictInTheBlock() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      database.command("opencypher", "UNWIND range(1, 3) AS i CALL { WITH i CREATE (:B13223 {i: i}) } IN TRANSACTIONS OF 2 ROWS");
      throw new ConcurrentModificationException("simulated conflict after the batches committed");
    }, false, 3)).isInstanceOf(ConcurrentModificationException.class);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(count("B13223")).isEqualTo(3);
  }

  /** The write before the CALL is committed before the first batch (#13172): a replay would create it twice. */
  @Test
  void preCallWritesAreNotReplayed() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> serverWrapper(attempts, 3, "CREATE (:Pre13223) WITH 1 AS one "//
        + "UNWIND range(1, 2) AS i "//
        + "CALL { WITH i CREATE (:B13223 {i: i}) } IN TRANSACTIONS OF 1 ROWS "//
        + "WITH count(*) AS c CREATE (:U13223 {k: 1})")).isInstanceOf(DuplicatedKeyException.class);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(count("Pre13223")).isEqualTo(1);
    assertThat(count("B13223")).isEqualTo(2);
  }

  /** A statement whose batches committed nothing yet is still retried by the wrapper as any other. */
  @Test
  void aConflictBeforeAnyBatchCommittedIsStillRetried() {
    final AtomicInteger attempts = new AtomicInteger();

    database.transaction(() -> {
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      if (attempts.incrementAndGet() == 1)
        throw new ConcurrentModificationException("simulated conflict before the statement");
      database.command("opencypher", "UNWIND range(1, 2) AS i CALL { WITH i CREATE (:B13223 {i: i}) } IN TRANSACTIONS OF 1 ROWS");
    }, false, 3);

    assertThat(attempts.get()).isEqualTo(2);
    assertThat(count("B13223")).isEqualTo(2);
  }

  /** Batches that wrote nothing leave nothing durable: a conflict after them is still retried, and the retry succeeds. */
  @Test
  void batchesThatWroteNothingDoNotStopTheRetry() {
    final AtomicInteger attempts = new AtomicInteger();

    database.transaction(() -> {
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      database.command("opencypher", "UNWIND range(1, 3) AS i "//
          + "CALL { WITH i MATCH (n:B13223 {i: i}) SET n.seen = true } IN TRANSACTIONS OF 1 ROWS");
      if (attempts.incrementAndGet() == 1)
        throw new ConcurrentModificationException("simulated conflict after batches that wrote nothing");
      database.command("opencypher", "CREATE (:Pre13223)");
    }, false, 3);

    assertThat(attempts.get()).isEqualTo(2);
    assertThat(count("Pre13223")).isEqualTo(1);
  }

  /** What the HTTP auto-commit wrapper does around every command it runs. */
  private void serverWrapper(final AtomicInteger attempts, final int retries, final String statement) {
    database.transaction(() -> {
      attempts.incrementAndGet();
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      database.command("opencypher", statement);
    }, false, retries);
  }

  private long count(final String label) {
    return database.query("opencypher", "MATCH (x:" + label + ") RETURN count(x) AS c").next().<Number>getProperty("c").longValue();
  }
}
