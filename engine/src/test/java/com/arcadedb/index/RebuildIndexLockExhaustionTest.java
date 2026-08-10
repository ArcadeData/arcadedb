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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #6040: before this fix, {@code RebuildIndexStatement.buildIndex()} silently fell off the end
 * of the method once its retry budget was exhausted, so a caller of {@code REBUILD INDEX} had no way to tell a
 * failed rebuild from a successful one. It now throws, matching the fact that the statement is a synchronous DDL
 * command that either succeeds or reports why it didn't.
 *
 * <p>Uses {@code maxAttempts = 1} so a single forced {@code LockTimeoutException} (held from a second thread via
 * {@link TestHelper.LockHoldingThread}, which wraps the same {@code DatabaseInternal.executeLockingFiles}
 * primitive the statement itself uses) immediately exhausts the budget - this is a pure lock-acquisition failure,
 * so the index must come out of it completely untouched. The hold time is 1s past the hardcoded 5s
 * {@code tryLockFiles} timeout, as margin against scheduling jitter on a loaded CI runner (see
 * {@code engine/CLAUDE.md} on that timeout being "a common source of intermittent failures in contention tests").
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RebuildIndexLockExhaustionTest extends TestHelper {

  @Test
  void rebuildThrowsAndLeavesIndexUntouchedWhenLockCannotBeAcquired() throws Exception {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE INDEX myIdx ON Doc (name) NOTUNIQUE");
      database.command("sql", "INSERT INTO Doc SET name = 'alpha'");
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    final List<Integer> fileIds = ((IndexInternal) db.getSchema().getIndexByName("myIdx")).getFileIds();

    final LockHoldingThread holder = new LockHoldingThread(db, fileIds, 6_000);
    holder.start();
    try {
      assertThat(holder.lockAcquired.await(5, TimeUnit.SECONDS))
          .as("background thread must acquire the lock before REBUILD INDEX runs").isTrue();

      // executeDDL wraps every failure (pre-existing behavior, unrelated to #6040) into an IndexException; the
      // CommandExecutionException this fix raises on exhaustion survives as its cause and its message.
      assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX myIdx WITH maxAttempts = 1"))
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("Cannot rebuild index 'myIdx'")
          .hasMessageContaining("the index lock could not be acquired")
          .hasMessageContaining("The index itself is unchanged")
          .cause().isInstanceOf(CommandExecutionException.class);
    } finally {
      holder.join(15_000);
    }
    assertThat(holder.isAlive()).isFalse();
    assertThat(holder.error.get()).isNull();

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("myIdx")).isTrue();
      final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE name = 'alpha'");
      assertThat(rs.hasNext()).isTrue();
    });
  }
}
