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
package com.arcadedb.server.ws;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7471, item 2: a chunk whose transaction failed as a whole was folded into the SESSION totals anyway, and
 * the protocol then tells the client to replay that chunk - so the replay's rows were counted a second time. The
 * final summary double-counted them and reported as {@code failed} rows the replay had actually written. A client
 * reconciling "rows sent" against "rows written" off the summary got a mismatch on exactly the path it had been
 * instructed to take.
 * <p>
 * The contract the fix writes down: the totals count each chunk EXACTLY ONCE, as its LATEST attempt left it. A
 * chunk that failed as a whole is held out of them until it is either replayed - in which case the replay's
 * outcome is what counts - or the session ends with it still outstanding, in which case it is added back, so
 * {@code received} still means "rows this session was given" and {@code failed} still means "rows it did not
 * write".
 * <p>
 * The whole-chunk failure is produced with a unique index, which the engine only checks when the chunk's
 * transaction commits: the rows all apply, and the COMMIT is what fails - which is precisely the case
 * {@code wholeChunkFailed} exists for, and the one a per-row failure (a missing type, a dangling edge) does not
 * reach.
 */
class Issue7471ChunkReplayTotalsIT extends BaseGraphServerTest {

  private static final String TYPE = "Issue7471Ticket";

  @Test
  void aReplayedChunkIsCountedOnceAndAsItsReplayLeftIt() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    createTypeWithATakenCode(database);

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start("per_batch"))).getString("sessionId");

      // Chunk 1 fails as a whole: every row applied, and the transaction's commit hit the unique index.
      final JSONObject failed = new JSONObject(client.send(chunk(sessionId, 1, "taken")));
      assertThat(failed.getLong("failed", -1)).isEqualTo(1);
      assertThat(failed.getLong("inserted", -1)).isZero();
      assertThat(failed.getBoolean("replay", false)).isFalse();

      // The watermark did not advance, so the SAME sequence is applied rather than acknowledged as a duplicate:
      // that is the replay the protocol asks the client for.
      final JSONObject replay = new JSONObject(client.send(chunk(sessionId, 1, "free")));
      assertThat(replay.getBoolean("replay", false)).isFalse();
      assertThat(replay.getLong("inserted", -1)).isEqualTo(1);

      final JSONObject summary = new JSONObject(client.send(control("commit", sessionId))).getJSONObject("summary");
      assertThat(summary.getLong("received", -1))
          .as("the replayed chunk's rows must be counted once, not once per attempt").isEqualTo(1);
      assertThat(summary.getLong("inserted", -1)).isEqualTo(1);
      assertThat(summary.getLong("failed", -1))
          .as("a row the replay wrote must not still be reported as failed").isZero();
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(2);
  }

  /**
   * The other half of the same contract, and what keeps the assertion above from simply hiding failures: a chunk
   * that failed and was NEVER replayed is still in the summary. Dropping it there would trade a double count for a
   * silent loss.
   */
  @Test
  void aFailedChunkThatIsNeverReplayedIsStillReportedInTheSummary() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    createTypeWithATakenCode(database);

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start("per_batch"))).getString("sessionId");

      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "taken"))).getLong("failed", -1)).isEqualTo(1);

      final JSONObject summary = new JSONObject(client.send(control("commit", sessionId))).getJSONObject("summary");
      assertThat(summary.getLong("received", -1)).isEqualTo(1);
      assertThat(summary.getLong("failed", -1)).isEqualTo(1);
      assertThat(summary.getLong("inserted", -1)).isZero();
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  /** A session where nothing fails is unaffected: the ordinary totals are still the sum of the chunks. */
  @Test
  void anOrdinarySessionStillSumsItsChunks() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    createTypeWithATakenCode(database);

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start("per_batch"))).getString("sessionId");

      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "a"))).getLong("inserted", -1)).isEqualTo(1);
      assertThat(new JSONObject(client.send(chunk(sessionId, 2, "b"))).getLong("inserted", -1)).isEqualTo(1);

      final JSONObject summary = new JSONObject(client.send(control("commit", sessionId))).getJSONObject("summary");
      assertThat(summary.getLong("received", -1)).isEqualTo(2);
      assertThat(summary.getLong("inserted", -1)).isEqualTo(2);
      assertThat(summary.getLong("failed", -1)).isZero();
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(3);
  }

  private static void createTypeWithATakenCode(final Database database) {
    database.command("sqlscript", "CREATE DOCUMENT TYPE " + TYPE + ";" //
        + "CREATE PROPERTY " + TYPE + ".code STRING;" //
        + "CREATE INDEX ON " + TYPE + " (code) UNIQUE;" //
        + "INSERT INTO " + TYPE + " SET code = 'taken';");
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String start(final String transactionMode) {
    return new JSONObject().put("action", "start").put("database", getDatabaseName())
        .put("options", new JSONObject().put("targetType", TYPE).put("transactionMode", transactionMode)).toString();
  }

  private static String chunk(final String sessionId, final long chunkSeq, final String code) {
    return new JSONObject().put("action", "chunk").put("sessionId", sessionId).put("chunkSeq", chunkSeq)
        .put("records", new JSONArray().put(new JSONObject().put("code", code))).toString();
  }

  private static String control(final String action, final String sessionId) {
    return new JSONObject().put("action", action).put("sessionId", sessionId).toString();
  }

  @Override
  protected void populateDatabase() {
  }
}
