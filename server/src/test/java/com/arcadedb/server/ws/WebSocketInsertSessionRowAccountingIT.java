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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How a {@code /ws} insert session accounts for its rows: a row acknowledged as a conflict is not committed, a
 * replayed chunk is counted once, and a row a listener vetoed is reported as ignored rather than written. Each
 * fixture creates its own types, so each is kept as a nested class.
 */
class WebSocketInsertSessionRowAccountingIT {

  /**
   * Issue #7467, end to end on the route it was reported against: a {@code /ws} insert row acknowledged as
   * {@code CONFLICT} must not be in the chunk the server then commits.
   * <p>
   * The shape matters. With {@code keyColumns} the session finds the conflict itself, with a {@code SELECT}
   * BEFORE it writes anything, which was never the defect. Without them the duplicate is the engine's to find,
   * and it finds it inline at {@code save()} when the twin was written by this same transaction - by which point
   * the record body, its identity and the bucket's record delta were already in the transaction. The row was
   * tallied {@code CONFLICT}, the chunk carried on to its commit, and the body went with it: a record in the
   * bucket, counted by {@code count(*)}, absent from the unique index that was supposed to forbid it, and
   * acknowledged to the client as not written.
   * <p>
   * The retraction is the engine's ({@code RefusedWriteTest.FailedCreateLeavesNothingInTheTransaction} pins it
   * there). This pins what the client sees: the tally, the commit, and the database afterwards agreeing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConflictingRowIsNotCommitted extends BaseGraphServerTest {
    private static final String TYPE = "Keyed7467";

    private Database database;

    @BeforeEach
    void createKeyedType() {
      database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
      database.command("sql", "CREATE PROPERTY " + TYPE + ".name STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE + " (name) UNIQUE");
    }

    @AfterEach
    void dropKeyedType() {
      database.command("sql", "DROP TYPE " + TYPE + " IF EXISTS UNSAFE");
    }

    /**
     * {@code per_stream}: one transaction for the whole session, so the twin of row 0 is refused at row 1's own
     * {@code save()} and the session's {@code commit} frame writes what is left.
     */
    @Test
    void aConflictingRowInOneChunkIsNotCommittedUnderPerStream() throws Throwable {
      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start("per_stream"))).getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1,
            record("dup", 1), record("dup", 2), record("other", 3))));
        assertThat(ack.getString("action", "")).isEqualTo("batchAck");
        assertThat(ack.getLong("inserted", -1)).isEqualTo(2);
        assertThat(ack.getLong("failed", -1)).isEqualTo(1);
        assertThat(ack.getJSONArray("errors").getJSONObject(0).getInt("rowIndex", -1)).isEqualTo(1);
        assertThat(ack.getJSONArray("errors").getJSONObject(0).getString("code", "")).isEqualTo("CONFLICT");

        final JSONObject committed = new JSONObject(client.send(control("commit", sessionId)));
        assertThat(committed.getString("action", "")).isEqualTo("committed");
        assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isEqualTo(2);
      }

      assertTheAcknowledgementAndTheDatabaseAgree();
    }

    /** {@code per_batch}: the chunk is one transaction, and the refused row must not ride its commit either. */
    @Test
    void aConflictingRowInOneChunkIsNotCommittedUnderPerBatch() throws Throwable {
      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start("per_batch"))).getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1,
            record("dup", 1), record("dup", 2), record("other", 3))));
        assertThat(ack.getLong("inserted", -1)).isEqualTo(2);
        assertThat(ack.getLong("failed", -1)).isEqualTo(1);

        new JSONObject(client.send(control("commit", sessionId)));
      }

      assertTheAcknowledgementAndTheDatabaseAgree();
    }

    /**
     * {@code per_row}: the row IS the transaction, so a refused row takes its own transaction down rather than
     * being tallied inside it and committed anyway.
     */
    @Test
    void aConflictingRowIsNotCommittedUnderPerRow() throws Throwable {
      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start("per_row"))).getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1,
            record("dup", 1), record("dup", 2), record("other", 3))));
        assertThat(ack.getLong("inserted", -1)).isEqualTo(2);
        assertThat(ack.getLong("failed", -1)).isEqualTo(1);
        assertThat(ack.getJSONArray("errors").getJSONObject(0).getInt("rowIndex", -1)).isEqualTo(1);
        assertThat(ack.getJSONArray("errors").getJSONObject(0).getString("code", "")).isEqualTo("CONFLICT");

        new JSONObject(client.send(control("commit", sessionId)));
      }

      assertTheAcknowledgementAndTheDatabaseAgree();
    }

    /**
     * The three views the defect made disagree: what the client was told, what {@code count(*)} says, and what the
     * unique index holds.
     */
    private void assertTheAcknowledgementAndTheDatabaseAgree() {
      assertThat(database.countType(TYPE, false)).as("count(*) must match what the session acknowledged").isEqualTo(2);

      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE name = 'dup'")) {
        assertThat(rs.stream().count()).as("the unique index must answer for the one row that took the key").isEqualTo(1);
      }

      final long scanned;
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
        scanned = rs.stream().count();
      }
      assertThat(scanned).as("a full scan must not find a record no lookup by key can reach").isEqualTo(2);

      try (final ResultSet rs = database.query("sql", "SELECT row FROM " + TYPE + " WHERE name = 'dup'")) {
        assertThat(rs.next().<Integer>getProperty("row")).as("the row that took the key is the one that kept it")
            .isEqualTo(1);
      }
    }

    private WebSocketClientHelper newClient() throws Exception {
      return new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }

    /** No {@code keyColumns}: the duplicate has to be the engine's to find, which is the whole point. */
    private String start(final String transactionMode) {
      final JSONObject message = new JSONObject();
      message.put("action", "start");
      message.put("database", getDatabaseName());
      message.put("options", new JSONObject().put("targetType", TYPE).put("transactionMode", transactionMode));
      return message.toString();
    }

    private static JSONObject record(final String name, final int row) {
      return new JSONObject().put("name", name).put("row", row);
    }

    private static String chunk(final String sessionId, final long chunkSeq, final JSONObject... records) {
      final JSONObject message = new JSONObject();
      message.put("action", "chunk");
      message.put("sessionId", sessionId);
      message.put("chunkSeq", chunkSeq);
      message.put("records", new JSONArray(records));
      return message.toString();
    }

    private static String control(final String action, final String sessionId) {
      return new JSONObject().put("action", action).put("sessionId", sessionId).toString();
    }
  }

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
  @Nested
  class ChunkReplayTotals extends BaseGraphServerTest {

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

  /**
   * Issue #10084: the {@code /ws} insert session counted a row a before-create or before-update listener vetoed as inserted or
   * updated, because it called the {@code void} {@code save()} and threw the engine's answer away - so a {@code batchAck} told a
   * client driving the load off its counters that rows the engine refused were durable. A vetoed row is {@code ignored}, as in the
   * gRPC loader this session is the twin of, and a vetoed upsert does not fall through to inserting a second record for the key.
   * The edge arm hit a {@link NullPointerException} out of the adjacency append (issue #10085) and answered {@code DB_ERROR}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ListenerVeto extends BaseGraphServerTest {
    private static final String DOC  = "Doc10084";
    private static final String VTX  = "Vtx10084";
    private static final String EDGE = "Link10084";

    private Database database;

    @BeforeEach
    void createTypes() {
      database = getServerDatabase(0, getDatabaseName());
      database.getSchema().createDocumentType(DOC).getEvents().registerListener(refuseCreate());
      database.getSchema().getType(DOC).getEvents().registerListener(refuseUpdate());
      database.getSchema().createVertexType(VTX).getEvents().registerListener(refuseCreate());
      database.getSchema().createEdgeType(EDGE).getEvents().registerListener(refuseCreate());
      database.getSchema().getType(EDGE).getEvents().registerListener(refuseUpdate());
    }

    @AfterEach
    void dropTypes() {
      database.command("sql", "DROP TYPE " + EDGE + " IF EXISTS UNSAFE");
      database.command("sql", "DROP TYPE " + VTX + " IF EXISTS UNSAFE");
      database.command("sql", "DROP TYPE " + DOC + " IF EXISTS UNSAFE");
    }

    /** The reporter's first repro: a chunk the create listener refuses is acknowledged as ignored, and nothing is stored. */
    @Test
    void aRefusedCreateIsIgnoredAndNotInserted() throws Throwable {
      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", DOC)
            .put("transactionMode", "per_batch")))).getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
            row("refused-1"), row("kept"), row("refused-2"))));
        assertThat(ack.getLong("received", -1)).isEqualTo(3);
        assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
        assertThat(ack.getLong("ignored", -1)).isEqualTo(2);
        assertThat(ack.getLong("updated", -1)).isZero();
        assertThat(ack.getLong("failed", -1)).isZero();

        final JSONObject committed = new JSONObject(client.send(control("commit", sessionId)));
        assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isEqualTo(1);
        assertThat(committed.getJSONObject("summary").getLong("ignored", -1)).isEqualTo(2);
      }

      assertThat(database.countType(DOC, false)).isEqualTo(1);
    }

    @Test
    void aRefusedVertexCreateIsIgnoredAndNotInserted() throws Throwable {
      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", VTX)
            .put("transactionMode", "per_batch")))).getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, row("refused"), row("kept"))));
        assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
        assertThat(ack.getLong("ignored", -1)).isEqualTo(1);
        assertThat(ack.getLong("failed", -1)).isZero();
        new JSONObject(client.send(control("commit", sessionId)));
      }

      assertThat(database.countType(VTX, false)).isEqualTo(1);
    }

    /** The reporter's second repro: a refused upsert is ignored, the stored value stays, and no second record is inserted. */
    @Test
    void aRefusedUpdateIsIgnoredAndDoesNotFallThroughToAnInsert() throws Throwable {
      database.transaction(() -> database.newDocument(DOC).set("name", "probe0").set("city", "before").save());

      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", DOC)
            .put("transactionMode", "per_batch").put("conflictMode", "update").put("keyColumns", new JSONArray().put("name")))))
            .getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
            new JSONObject().put("name", "probe0").put("city", "refused"))));
        assertThat(ack.getLong("received", -1)).isEqualTo(1);
        assertThat(ack.getLong("updated", -1)).isZero();
        assertThat(ack.getLong("inserted", -1)).isZero();
        assertThat(ack.getLong("ignored", -1)).isEqualTo(1);
        assertThat(ack.getLong("failed", -1)).isZero();
        new JSONObject(client.send(control("commit", sessionId)));
      }

      assertThat(database.countType(DOC, false)).isEqualTo(1);
      try (final ResultSet rs = database.query("sql", "select from " + DOC + " where name = 'probe0'")) {
        assertThat(rs.next().<String>getProperty("city")).isEqualTo("before");
      }
    }

    /** An update the listener allows is still counted as an update. */
    @Test
    void anAllowedUpdateIsStillCountedAsUpdated() throws Throwable {
      database.transaction(() -> database.newDocument(DOC).set("name", "probe1").set("city", "before").save());

      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", DOC)
            .put("transactionMode", "per_batch").put("conflictMode", "update").put("keyColumns", new JSONArray().put("name")))))
            .getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
            new JSONObject().put("name", "probe1").put("city", "after"))));
        assertThat(ack.getLong("updated", -1)).isEqualTo(1);
        assertThat(ack.getLong("ignored", -1)).isZero();
        new JSONObject(client.send(control("commit", sessionId)));
      }

      try (final ResultSet rs = database.query("sql", "select from " + DOC + " where name = 'probe1'")) {
        assertThat(rs.next().<String>getProperty("city")).isEqualTo("after");
      }
    }

    /** The edge arm: a refused edge is ignored, not a {@code DB_ERROR} carrying the JDK's NullPointerException text. */
    @Test
    void aRefusedEdgeIsIgnoredAndNotADatabaseError() throws Throwable {
      final String[] rids = new String[3];
      database.transaction(() -> {
        rids[0] = database.newVertex(VTX).set("name", "kept").save().getIdentity().toString();
        rids[1] = database.newVertex(VTX).set("name", "kept").save().getIdentity().toString();
        rids[2] = database.newVertex(VTX).set("name", "kept").save().getIdentity().toString();
      });
      // The source owns a committed edge segment: the ordinary case, where the adjacency append dereferenced the null RID
      database.transaction(() -> database.lookupByRID(database.newRID(rids[0]), false).asVertex().newEdge(EDGE, database.newRID(rids[1]),
          "name", "kept"));

      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", EDGE)
            .put("transactionMode", "per_batch")))).getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
            new JSONObject().put("name", "refused").put("@from", rids[0]).put("@to", rids[2]),
            new JSONObject().put("name", "kept").put("@from", rids[0]).put("@to", rids[2]))));
        assertThat(ack.getLong("received", -1)).isEqualTo(2);
        assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
        assertThat(ack.getLong("ignored", -1)).isEqualTo(1);
        assertThat(ack.getLong("failed", -1)).isZero();
        assertThat(ack.has("errors")).isFalse();
        new JSONObject(client.send(control("commit", sessionId)));
      }

      assertThat(database.countType(EDGE, false)).isEqualTo(2);
    }

    /** An edge upsert the listener refuses is ignored too, and the edge keeps its stored value. */
    @Test
    void aRefusedEdgeUpdateIsIgnored() throws Throwable {
      final String[] rids = new String[2];
      database.transaction(() -> {
        final var from = database.newVertex(VTX).set("name", "kept").save();
        final var to = database.newVertex(VTX).set("name", "kept").save();
        rids[0] = from.getIdentity().toString();
        rids[1] = to.getIdentity().toString();
        from.newEdge(EDGE, to, "name", "e1", "weight", 1);
      });

      try (final var client = newClient()) {
        final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", EDGE)
            .put("transactionMode", "per_batch").put("conflictMode", "update").put("keyColumns", new JSONArray().put("name")))))
            .getString("sessionId");

        final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
            new JSONObject().put("name", "e1").put("weight", 2).put("flag", "refused").put("@from", rids[0]).put("@to", rids[1]))));
        assertThat(ack.getLong("updated", -1)).isZero();
        assertThat(ack.getLong("inserted", -1)).isZero();
        assertThat(ack.getLong("ignored", -1)).isEqualTo(1);
        new JSONObject(client.send(control("commit", sessionId)));
      }

      assertThat(database.countType(EDGE, false)).isEqualTo(1);
      try (final ResultSet rs = database.query("sql", "select from " + EDGE + " where name = 'e1'")) {
        assertThat(rs.next().<Integer>getProperty("weight")).isEqualTo(1);
      }
    }

    // --- helpers ---

    private static BeforeRecordCreateListener refuseCreate() {
      return record -> !isRefused(record.asDocument().getString("name"));
    }

    private static BeforeRecordUpdateListener refuseUpdate() {
      return record -> !"refused".equals(record.asDocument().getString("city"))
          && !"refused".equals(record.asDocument().getString("flag"));
    }

    private static boolean isRefused(final String name) {
      return name != null && name.startsWith("refused");
    }

    private static JSONObject row(final String name) {
      return new JSONObject().put("name", name);
    }

    private WebSocketClientHelper newClient() throws Exception {
      return new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }

    private String start(final JSONObject options) {
      final JSONObject message = new JSONObject();
      message.put("action", "start");
      message.put("database", getDatabaseName());
      message.put("options", options);
      return message.toString();
    }

    private static String chunkOf(final String sessionId, final long chunkSeq, final JSONObject... records) {
      final JSONObject message = new JSONObject();
      message.put("action", "chunk");
      message.put("sessionId", sessionId);
      message.put("chunkSeq", chunkSeq);
      message.put("records", new JSONArray(records));
      return message.toString();
    }

    private static String control(final String action, final String sessionId) {
      final JSONObject message = new JSONObject();
      message.put("action", action);
      message.put("sessionId", sessionId);
      return message.toString();
    }
  }
}
