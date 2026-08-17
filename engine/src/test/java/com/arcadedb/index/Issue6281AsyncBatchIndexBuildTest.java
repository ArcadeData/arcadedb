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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6281: an index is built by SCANNING the buckets, so everything the asynchronous executor was asked to write
 * has to be committed before the build starts. The guard that was supposed to ensure that polled
 * {@code database.isAsyncProcessing()} and drained the executor only when it answered {@code true}.
 * <p>
 * That predicate answers about TASKS, while a worker opens one transaction when it starts and keeps it open across up
 * to {@link com.arcadedb.GlobalConfiguration#ASYNC_TX_BATCH_SIZE} tasks (10240 by default). An executor whose queues
 * happen to be drained therefore reports itself idle while still holding every record of the current batch
 * uncommitted, the guard was skipped entirely, and the index was built over a bucket that did not contain them. The
 * records then commit afterwards, with no entry ever added for them: the index ends up EMPTY and every lookup answers
 * nothing, silently, on a database that {@code CHECK DATABASE} calls healthy.
 * <p>
 * The barrier is now unconditional ({@link DatabaseInternal#waitForAsyncCompletion()}), because there is no cheap
 * predicate to test first: only {@code waitCompletion()} closes the open batch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6281AsyncBatchIndexBuildTest extends TestHelper {
  private static final int TOT = 200;

  /**
   * The exact shape of the bug: every async task has already RUN - nothing is queued and no worker is executing - and
   * the records are all still uncommitted in the workers' open batch transaction when the index is created.
   */
  @Test
  void anIndexCreatedOverAnIdleAsyncExecutorStillCoversItsUncommittedBatch() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

    database.async().setParallelLevel(2);

    final CountDownLatch executed = new CountDownLatch(TOT);
    for (int i = 0; i < TOT; i++) {
      final MutableDocument v = database.newDocument("V");
      v.set("id", i);
      database.async().createRecord(v, record -> executed.countDown());
    }

    // Every task has run...
    assertThat(executed.await(30, TimeUnit.SECONDS)).isTrue();
    // ...and the workers are back to parking on their empty queues, which is the state the old guard read as "idle".
    waitForIdleAsyncExecutor();

    // The precondition the whole issue rests on, and the reason "idle" was never the same as "done": not one of
    // those records is committed yet. This is a property of the batching (TOT < ASYNC_TX_BATCH_SIZE), not a race.
    assertThat(database.countType("V", false)).as("the async batch must still be open").isZero();

    database.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

    database.async().waitCompletion();

    assertIndexCoversEveryRecord();
  }

  /**
   * The same creation with the executor genuinely busy. This path always worked - the old predicate answered
   * {@code true} and the drain ran - and it must keep working now that the drain is unconditional.
   */
  @Test
  void anIndexCreatedWhileTheAsyncExecutorIsBusyCoversEveryRecord() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

    database.async().setParallelLevel(2);

    for (int i = 0; i < TOT; i++) {
      final MutableDocument v = database.newDocument("V");
      v.set("id", i);
      database.async().createRecord(v, null);
    }

    database.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

    database.async().waitCompletion();

    assertIndexCoversEveryRecord();
  }

  private void assertIndexCoversEveryRecord() {
    assertThat(database.countType("V", false)).isEqualTo(TOT);

    final Index index = database.getSchema().getIndexByName("V[id]");
    assertThat(index.countEntries()).as("every record must have an entry in the index").isEqualTo(TOT);

    for (int i = 0; i < TOT; i++)
      try (final ResultSet rs = database.query("sql", "select from V where id = ?", i)) {
        assertThat(rs.hasNext()).as("record id " + i + " must be reachable through the index").isTrue();
        assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(i);
      }
  }

  /**
   * Settles on the state the old guard mistook for "everything is written": queues empty, no task executing. Bounded
   * because it is a set-up step and not what is under test - if the executor never settles the assertions below still
   * hold, they just stop reproducing the bug.
   */
  private void waitForIdleAsyncExecutor() throws InterruptedException {
    final DatabaseInternal db = (DatabaseInternal) database;
    for (int i = 0; i < 500 && db.isAsyncProcessing(); i++)
      Thread.sleep(10);
  }
}
