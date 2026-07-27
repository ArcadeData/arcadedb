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
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5470: on a replicated database the WAL cannot be skipped, because the
 * replication layer ships the WAL bytes captured during commit phase 1 to the followers. {@link GraphBatch}
 * therefore forces {@code useWAL} back on, but the "no WAL means one commit per flush" shortcut used to be
 * decided on the <i>requested</i> value instead of the effective one. A replicated bulk load left at the
 * defaults (which is what {@code PostBatchHandler} does) consequently ran with {@code commitEvery == 0} and
 * committed a whole flush - up to {@code batchSize} edges - in a single transaction, shipped as one Raft
 * entry that easily exceeds the maximum replicated entry size and fails the load with
 * {@code ReplicatedEntryTooLargeException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5470ReplicatedCommitEveryTest extends TestHelper {

  private static final int DEFAULT_COMMIT_EVERY = 50_000;

  @Test
  void replicatedDatabaseKeepsCommittingInChunksWhenWALIsLeftOff() {
    try (final GraphBatch batch = GraphBatch.builder(replicatedView((DatabaseInternal) database)).build()) {
      assertThat(batch.isUseWAL()).as("WAL is forced on because the database is replicated").isTrue();
      assertThat(batch.getCommitEvery()).as("a replicated flush must not commit in one oversized transaction")
          .isEqualTo(DEFAULT_COMMIT_EVERY);
    }
  }

  @Test
  void replicatedDatabaseHonoursAnExplicitCommitEvery() {
    try (final GraphBatch batch = GraphBatch.builder(replicatedView((DatabaseInternal) database))
        .withCommitEvery(5_000).build()) {
      assertThat(batch.getCommitEvery()).isEqualTo(5_000);
    }
  }

  @Test
  void nonReplicatedDatabaseWithoutWALStillCommitsOncePerFlush() {
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      assertThat(batch.isUseWAL()).isFalse();
      assertThat(batch.getCommitEvery()).as("the single-commit-per-flush shortcut is still taken when the WAL is really off")
          .isZero();
    }
  }

  @Test
  void nonReplicatedDatabaseWithWALCommitsInChunks() {
    try (final GraphBatch batch = GraphBatch.builder(database).withWAL(true).build()) {
      assertThat(batch.getCommitEvery()).isEqualTo(DEFAULT_COMMIT_EVERY);
    }
  }

  /**
   * A view of the database that only differs in {@link DatabaseInternal#isReplicated()}. Avoids pulling a
   * mocking framework into the engine module just to flip one flag.
   */
  private static DatabaseInternal replicatedView(final DatabaseInternal delegate) {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("isReplicated".equals(method.getName()) && (args == null || args.length == 0))
            return Boolean.TRUE;
          try {
            return method.invoke(delegate, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }
}
