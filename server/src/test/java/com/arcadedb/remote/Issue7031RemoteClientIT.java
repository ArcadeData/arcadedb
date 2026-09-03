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
package com.arcadedb.remote;

import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7031: a failed {@link RemoteGraphBatch#flush()} must not leave its payload buffered
 * for {@link RemoteGraphBatch#close()} to send a second time, and {@link RemoteSchema#existsBucket(String)} must
 * answer from the bucket list rather than from the buckets attached to the types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7031RemoteClientIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-client-7031";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }

  /**
   * The bug: an interior auto-flush failed inside {@code createVertex()}, the buffer kept its payload, and the
   * {@code close()} of the try-with-resources sent the very same payload again. When the failure was a response
   * the client never saw rather than a server-side rejection, that second send created every record twice.
   */
  @Test
  void failedInteriorFlushIsNotResentOnClose() {
    try (final RecordingBatchDatabase database = newRecordingDatabase()) {
      database.command("sql", "CREATE VERTEX TYPE Person");

      database.failNextSend = true;

      assertThatThrownBy(() -> {
        try (final RemoteGraphBatch batch = database.batch().withFlushEvery(2).build()) {
          batch.createVertex("Person", "name", "Alice");
          // Trips the auto-flush, which fails.
          batch.createVertex("Person", "name", "Bob");
          batch.createVertex("Person", "name", "Carl");
        }
      }).isInstanceOf(ArcadeDBException.class).hasMessageContaining("simulated");

      assertThat(database.sentPayloads).as("the payload of the failed flush must not be sent a second time").hasSize(1);
      assertThat(database.sentPayloads.getFirst()).contains("Alice").contains("Bob");
    }
  }

  /**
   * The same for an explicit {@code flush()}: the failure is the caller's to handle, and the {@code close()} that
   * follows must not quietly repeat the attempt (nor raise a second failure on top of the real one).
   */
  @Test
  void failedExplicitFlushLeavesTheBatchUnusableAndClosesQuietly() {
    try (final RecordingBatchDatabase database = newRecordingDatabase()) {
      database.command("sql", "CREATE VERTEX TYPE Person");

      final RemoteGraphBatch batch = database.batch().build();
      batch.createVertex("Person", "name", "Alice");

      database.failNextSend = true;
      assertThatThrownBy(batch::flush).isInstanceOf(ArcadeDBException.class).hasMessageContaining("simulated");

      // Even if the transport recovers, the batch must not decide on its own to send that payload again.
      database.failNextSend = false;
      assertThatThrownBy(batch::flush).isInstanceOf(IllegalStateException.class).hasMessageContaining("previous flush failed");

      assertThatNoException().isThrownBy(batch::close);
      assertThat(database.sentPayloads).hasSize(1);

      // Nothing was created: the only request the client made is the one that failed.
      assertThat(countOf(database, "Person")).isZero();
    }
  }

  /**
   * A batch that never fails keeps working exactly as before: the buffered records are sent by {@code close()}.
   */
  @Test
  void successfulBatchStillFlushesOnClose() {
    try (final RecordingBatchDatabase database = newRecordingDatabase()) {
      database.command("sql", "CREATE VERTEX TYPE Person");

      try (final RemoteGraphBatch batch = database.batch().build()) {
        batch.createVertex("Person", "name", "Alice");
        batch.createVertex("Person", "name", "Bob");
      }

      assertThat(database.sentPayloads).hasSize(1);
      assertThat(countOf(database, "Person")).isEqualTo(2);
    }
  }

  /**
   * The bug: {@code existsBucket()} resolved the name against {@code schema:types}, so a bucket attached to no
   * type - including one just created through this same API - answered {@code false}.
   */
  @Test
  void existsBucketSeesABucketAttachedToNoType() {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      assertThat(database.getSchema().existsBucket("standalone")).isFalse();

      database.getSchema().createBucket("standalone");
      assertThat(database.getSchema().existsBucket("standalone")).isTrue();

      // A bucket that belongs to a type keeps answering true, and an unknown name still answers false.
      database.command("sql", "CREATE DOCUMENT TYPE Person BUCKETS 1");
      assertThat(database.getSchema().existsBucket("Person_0")).isTrue();
      assertThat(database.getSchema().existsBucket("NotThere")).isFalse();

      database.getSchema().dropBucket("standalone");
      assertThat(database.getSchema().existsBucket("standalone")).isFalse();
    }
  }

  private RecordingBatchDatabase newRecordingDatabase() {
    return new RecordingBatchDatabase("127.0.0.1", 2480, DATABASE_NAME, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private static long countOf(final RemoteDatabase database, final String typeName) {
    return database.countType(typeName, false);
  }

  /**
   * Records every payload handed to the {@code /batch} endpoint and can fail the next one, which is how a
   * response the client never sees - a socket reset after the server committed, a proxy timeout - reaches this
   * class: indistinguishable, from here, from a server-side rejection.
   */
  private static class RecordingBatchDatabase extends RemoteDatabase {
    final List<String> sentPayloads = new ArrayList<>();
    boolean failNextSend = false;

    RecordingBatchDatabase(final String server, final int port, final String databaseName, final String userName,
        final String userPassword) {
      super(server, port, databaseName, userName, userPassword);
    }

    @Override
    JSONObject sendBatch(final String content, final Map<String, String> queryParams) {
      sentPayloads.add(content);
      if (failNextSend)
        throw new ArcadeDBException("simulated failure of the batch request");
      return super.sendBatch(content, queryParams);
    }
  }
}
