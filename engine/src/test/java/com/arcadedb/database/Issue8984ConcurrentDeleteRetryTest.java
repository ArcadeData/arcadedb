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
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #8984: a {@code DELETE} that selected a record another transaction then deleted and committed failed with a
 * {@code RecordNotFoundException} (or, for a vertex, a {@code VertexNotFoundException} advising {@code CHECK DATABASE FIX}),
 * neither retryable, so {@code database.transaction(block, false, retries)} gave up after the first attempt. The record was
 * read by this very transaction, so its absence at the delete can only be a concurrent delete: a retryable conflict.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// hang detector only, not a latency bound
@Timeout(120)
class Issue8984ConcurrentDeleteRetryTest extends TestHelper {

  private void runRace(final String type, final boolean vertex, final String sql, final int retries) throws Exception {
    final ExecutorService otherThread = Executors.newSingleThreadExecutor();
    final AtomicReference<RID> target = new AtomicReference<>();
    final AtomicBoolean armed = new AtomicBoolean();
    final BeforeRecordDeleteListener listener = record -> {
      if (record.getIdentity().equals(target.get()) && armed.compareAndSet(true, false))
        try {
          otherThread.submit(() -> database.transaction(() -> database.deleteRecord(database.lookupByRID(target.get(), true))))
              .get();
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      return true;
    };
    database.getEvents().registerListener(listener);
    try {
      database.transaction(() -> target.set(
          (vertex ? database.newVertex(type) : database.newDocument(type)).set("key", 1).save().getIdentity()));
      armed.set(true);

      final AtomicInteger attempts = new AtomicInteger();
      database.transaction(() -> {
        attempts.incrementAndGet();
        database.command("sql", sql).close();
      }, false, retries);

      assertThat(armed.get()).as("the concurrent delete must have run").isFalse();
      assertThat(attempts.get()).as("the statement must have been retried").isEqualTo(2);
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
    } finally {
      database.getEvents().unregisterListener(listener);
      otherThread.shutdown();
    }
  }

  @Test
  void documentDeleteLosingTheRaceIsRetried() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.key INTEGER");
    database.command("sql", "CREATE INDEX ON Doc (key) UNIQUE");
    runRace("Doc", false, "DELETE FROM Doc WHERE key = 1", 3);
  }

  @Test
  void vertexDeleteLosingTheRaceIsRetried() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE Vtx");
    database.command("sql", "CREATE PROPERTY Vtx.key INTEGER");
    database.command("sql", "CREATE INDEX ON Vtx (key) UNIQUE");
    runRace("Vtx", true, "DELETE FROM Vtx WHERE key = 1", 3);
  }

  @Test
  void theConflictIsRetryableWhenSurfaced() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Doc2");
    final ExecutorService otherThread = Executors.newSingleThreadExecutor();
    try {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc2").set("key", 1).save().getIdentity());

      database.begin();
      final Document read = database.lookupByRID(rid[0], true).asDocument();
      otherThread.submit(() -> database.transaction(() -> database.deleteRecord(database.lookupByRID(rid[0], true)))).get();

      final Throwable thrown = catchThrowable(() -> database.deleteRecord(read));
      database.rollback();
      assertThat(thrown).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
    } finally {
      otherThread.shutdown();
    }
  }
}
