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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * BOLT server state machine regression tests driven straight over the wire, so the assertions are on the
 * protocol rather than on whatever a particular driver's fetch-size configuration happens to make it send.
 * <p>
 * Issue #6804: RUN is valid in TX_STREAMING - that is the whole point of the {@code qid} field, which lets a
 * client hold several result streams open inside one explicit transaction. The server rejected it as a protocol
 * error, so a second query issued while the first stream still had rows failed the session and lost the
 * transaction; {@code qid} was parsed and never used.
 * <p>
 * Issue #6803: LOGOFF was the one request handler with neither a state check nor any cleanup, so a LOGOFF sent
 * mid-stream answered SUCCESS and left the result set and the ArcadeDB transaction pinned on a connection the
 * server had just stopped counting as authenticated.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltStateMachineIT extends BaseGraphServerTest {

  private static final int BOLT_PORT = 7687;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /**
   * A connection already past the BOLT handshake and LOGON, exposing the chunked message pair.
   */
  private static final class BoltConnection implements AutoCloseable {
    private final Socket             socket;
    private final BoltChunkedOutput  out;
    private final BoltChunkedInput   in;

    BoltConnection(final String database) throws IOException {
      socket = new Socket("localhost", BOLT_PORT);

      final OutputStream rawOut = socket.getOutputStream();
      final ByteBuffer handshake = ByteBuffer.allocate(20);
      handshake.put((byte) 0x60).put((byte) 0x60).put((byte) 0xB0).put((byte) 0x17);
      handshake.putInt(0x00000405); // v5.4
      handshake.putInt(0x00000404); // v4.4
      handshake.putInt(0x00000003); // v3.0
      handshake.putInt(0x00000000);
      handshake.flip();
      rawOut.write(handshake.array());
      rawOut.flush();

      final DataInputStream rawIn = new DataInputStream(socket.getInputStream());
      final byte[] negotiated = new byte[4];
      rawIn.readFully(negotiated);
      assertThat(negotiated[3]).as("the deferred-auth path needs Bolt 5.x").isEqualTo((byte) 5);

      out = new BoltChunkedOutput(rawOut);
      in = new BoltChunkedInput(socket.getInputStream());

      sendMap(BoltMessage.HELLO,
          Map.of("user_agent", "bolt-state-machine-it/1.0", "routing", Map.of("db", database)));
      assertThat(readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      logon();
    }

    void logon() throws IOException {
      sendMap(BoltMessage.LOGON,
          Map.of("scheme", "basic", "principal", "root", "credentials", DEFAULT_PASSWORD_FOR_TESTS));
      assertThat(readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }

    /** Writes a request whose single field is a map (HELLO, LOGON, BEGIN, PULL, DISCARD). */
    void sendMap(final byte signature, final Map<String, Object> field) throws IOException {
      final PackStreamWriter writer = new PackStreamWriter();
      writer.writeStructureHeader(signature, 1);
      writer.writeMap(field);
      out.writeMessage(writer.toByteArray());
    }

    /** Writes a request with no fields at all (COMMIT, ROLLBACK, RESET, LOGOFF, GOODBYE). */
    void sendNoFields(final byte signature) throws IOException {
      final PackStreamWriter writer = new PackStreamWriter();
      writer.writeStructureHeader(signature, 0);
      out.writeMessage(writer.toByteArray());
    }

    void begin(final String database) throws IOException {
      sendMap(BoltMessage.BEGIN, Map.of("db", database));
      assertThat(readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }

    void run(final String query) throws IOException {
      run(query, Map.of());
    }

    void run(final String query, final Map<String, Object> extra) throws IOException {
      final PackStreamWriter writer = new PackStreamWriter();
      writer.writeStructureHeader(BoltMessage.RUN, 3);
      writer.writeString(query);
      writer.writeMap(Map.of());
      writer.writeMap(extra);
      out.writeMessage(writer.toByteArray());
    }

    void pull(final long n, final long qid) throws IOException {
      sendMap(BoltMessage.PULL, streamSelector(n, qid));
    }

    void discard(final long n, final long qid) throws IOException {
      sendMap(BoltMessage.DISCARD, streamSelector(n, qid));
    }

    private static Map<String, Object> streamSelector(final long n, final long qid) {
      final Map<String, Object> extra = new LinkedHashMap<>();
      extra.put("n", n);
      extra.put("qid", qid);
      return extra;
    }

    /**
     * Reads RECORDs until the summary message (SUCCESS / FAILURE / IGNORED) that closes the exchange.
     */
    Summary readSummary() throws IOException {
      final List<Object> records = new ArrayList<>();
      while (true) {
        final byte[] response = in.readMessage();
        final byte signature = response[1];
        if (signature == BoltMessage.RECORD) {
          records.add(decodeSingleField(response));
          continue;
        }
        return new Summary(signature, asMetadata(decodeSingleField(response)), records);
      }
    }

    /** SUCCESS/FAILURE/RECORD are all single-field structures: skip the two header bytes, read the field. */
    private Object decodeSingleField(final byte[] response) throws IOException {
      final PackStreamReader reader = new PackStreamReader(response);
      reader.readRawByte();
      reader.readRawByte();
      return reader.readValue();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMetadata(final Object value) {
      return value instanceof Map ? (Map<String, Object>) value : Map.of();
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }

  private record Summary(byte signature, Map<String, Object> metadata, List<Object> records) {
  }

  /**
   * Issue #6804: two result streams open at once inside one explicit transaction, each pulled by its own qid.
   * Before the fix the second RUN answered {@code "RUN not expected in state: TX_STREAMING"}.
   */
  @Test
  @SuppressWarnings("unchecked")
  void secondRunInsideATransactionOpensASecondStream() throws Exception {
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.begin(getDatabaseName());

      bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
      final Summary firstRun = bolt.readSummary();
      assertThat(firstRun.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(firstRun.metadata()).containsEntry("qid", 0L);

      // Pull a single row, leaving the stream open: this is the state a driver configured with a small fetch
      // size is in when the application issues its next query.
      bolt.pull(1, -1);
      final Summary firstPull = bolt.readSummary();
      assertThat(firstPull.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(firstPull.records()).hasSize(1);
      assertThat(firstPull.metadata()).containsEntry("has_more", true);

      // The second RUN, the one that used to fail the session.
      bolt.run("RETURN 42 AS answer");
      final Summary secondRun = bolt.readSummary();
      assertThat(secondRun.signature()).as("RUN must be accepted in TX_STREAMING").isEqualTo(BoltMessage.SUCCESS);
      assertThat(secondRun.metadata()).containsEntry("qid", 1L);

      // Drain the second stream by its qid; the first one is untouched and the session stays in TX_STREAMING.
      bolt.pull(-1, 1);
      final Summary secondPull = bolt.readSummary();
      assertThat(secondPull.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(secondPull.records()).hasSize(1);
      assertThat(((List<Object>) secondPull.records().get(0)).get(0)).isEqualTo(42L);
      assertThat(secondPull.metadata()).containsEntry("has_more", false);

      // Then finish the first stream, which still owes rows 2 and 3.
      bolt.pull(-1, 0);
      final Summary rest = bolt.readSummary();
      assertThat(rest.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(rest.records()).hasSize(2);
      assertThat(rest.metadata()).containsEntry("has_more", false);

      bolt.sendNoFields(BoltMessage.COMMIT);
      assertThat(bolt.readSummary().signature()).as("the transaction must survive the interleaved streams")
          .isEqualTo(BoltMessage.SUCCESS);
    }
  }

  /**
   * A DISCARD naming an explicit qid must close that stream and leave the others alone.
   */
  @Test
  void discardByQidClosesOnlyTheNamedStream() throws Exception {
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.begin(getDatabaseName());

      bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
      assertThat(bolt.readSummary().metadata()).containsEntry("qid", 0L);
      bolt.pull(1, -1);
      assertThat(bolt.readSummary().metadata()).containsEntry("has_more", true);

      bolt.run("UNWIND [10, 20] AS y RETURN y");
      assertThat(bolt.readSummary().metadata()).containsEntry("qid", 1L);

      bolt.discard(-1, 0);
      final Summary discard = bolt.readSummary();
      assertThat(discard.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(discard.metadata()).containsEntry("has_more", false);

      // Stream 1 is still open and still complete.
      bolt.pull(-1, 1);
      final Summary remaining = bolt.readSummary();
      assertThat(remaining.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(remaining.records()).hasSize(2);

      bolt.sendNoFields(BoltMessage.COMMIT);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }
  }

  @Test
  void pullNamingAnUnknownQidFailsInsteadOfHittingAnotherStream() throws Exception {
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.begin(getDatabaseName());

      bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.pull(-1, 99);
      final Summary failure = bolt.readSummary();
      assertThat(failure.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(String.valueOf(failure.metadata().get("message"))).contains("qid 99");
    }
  }

  /**
   * Every open stream pins an engine result set for the life of the transaction, and nothing in the protocol
   * obliges a client to consume one, so a connection cannot be allowed to open them without limit.
   */
  @Test
  void openingMoreStreamsThanTheLimitAllowsIsRejected() throws Exception {
    final int previousLimit = GlobalConfiguration.BOLT_MAX_OPEN_STREAMS.getValueAsInteger();
    GlobalConfiguration.BOLT_MAX_OPEN_STREAMS.setValue(2);
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.begin(getDatabaseName());

      // Two streams, each left with rows outstanding so neither is released.
      for (int i = 0; i < 2; i++) {
        bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
        bolt.pull(1, -1);
        assertThat(bolt.readSummary().metadata()).containsEntry("has_more", true);
      }

      bolt.run("RETURN 1 AS one");
      final Summary rejected = bolt.readSummary();
      assertThat(rejected.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(String.valueOf(rejected.metadata().get("message"))).contains("Too many result streams open");
    } finally {
      GlobalConfiguration.BOLT_MAX_OPEN_STREAMS.setValue(previousLimit);
    }
  }

  /**
   * A ceiling of 0 would reject every query outright rather than lock anything down, so the setting is floored at
   * one open stream: the query runs, and only a second concurrent one is refused.
   */
  @Test
  void anOpenStreamLimitBelowOneStillAllowsASingleStream() throws Exception {
    final int previousLimit = GlobalConfiguration.BOLT_MAX_OPEN_STREAMS.getValueAsInteger();
    GlobalConfiguration.BOLT_MAX_OPEN_STREAMS.setValue(0);
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.begin(getDatabaseName());

      bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
      assertThat(bolt.readSummary().signature()).as("one stream must always be allowed").isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(1, -1);
      assertThat(bolt.readSummary().metadata()).containsEntry("has_more", true);

      bolt.run("RETURN 1 AS one");
      final Summary rejected = bolt.readSummary();
      assertThat(rejected.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(String.valueOf(rejected.metadata().get("message"))).contains("max 1");
    } finally {
      GlobalConfiguration.BOLT_MAX_OPEN_STREAMS.setValue(previousLimit);
    }
  }

  /**
   * Outside an explicit transaction there is exactly one stream and no qid is ever published for it, so a PULL
   * that names one anyway has to reach that stream rather than be told the qid does not exist.
   */
  @Test
  void pullWithAQidInAutoCommitStillReachesTheOnlyStream() throws Exception {
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.run("RETURN 1 AS one");
      assertThat(bolt.readSummary().metadata()).as("an auto-commit RUN publishes no qid").doesNotContainKey("qid");

      // A qid that matches nothing: honouring it would answer "No active result set for qid 99" for the one
      // stream this connection plainly has open.
      bolt.pull(-1, 99);
      final Summary summary = bolt.readSummary();
      assertThat(summary.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(summary.records()).hasSize(1);
    }
  }

  /**
   * RUN stays invalid in STREAMING: outside an explicit transaction there is nothing to multiplex onto, and the
   * BOLT state machine does not list RUN there.
   */
  @Test
  void runIsStillRejectedInAutoCommitStreaming() throws Exception {
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(1, -1);
      assertThat(bolt.readSummary().metadata()).containsEntry("has_more", true);

      bolt.run("RETURN 1 AS one");
      final Summary rejected = bolt.readSummary();
      assertThat(rejected.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(String.valueOf(rejected.metadata().get("message"))).contains("STREAMING");
    }
  }

  /**
   * Issue #6803: LOGOFF while a stream and a transaction are open used to answer SUCCESS and leave both pinned
   * on a connection the server no longer counted as authenticated.
   */
  @Test
  void logoffIsRejectedWhileAStreamAndATransactionAreOpen() throws Exception {
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.begin(getDatabaseName());

      bolt.run("CREATE (n:Issue6803Logoff {value: 1}) RETURN n");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      // n=0 consumes nothing, so the row stays buffered and the session stays in TX_STREAMING.
      bolt.pull(0, -1);
      final Summary pull = bolt.readSummary();
      assertThat(pull.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(pull.metadata()).containsEntry("has_more", true);

      bolt.sendNoFields(BoltMessage.LOGOFF);
      final Summary logoff = bolt.readSummary();
      assertThat(logoff.signature()).as("LOGOFF is valid only from READY").isEqualTo(BoltMessage.FAILURE);
      assertThat(String.valueOf(logoff.metadata().get("message"))).contains("TX_STREAMING");

      // RESET is the only way out of FAILED, and it must roll the write back.
      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.run("MATCH (n:Issue6803Logoff) RETURN n");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      final Summary check = bolt.readSummary();
      assertThat(check.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(check.records()).as("the transaction the rejected LOGOFF left behind must have been rolled back")
          .isEmpty();
    }
  }

  /**
   * An explicit transaction is bound to the database BEGIN opened it on. A RUN naming another one has to be
   * refused rather than allowed to drop the connection's handle on that database, which is the only thing a
   * later rollback has to work with.
   */
  @Test
  void runCannotSwitchDatabaseInsideAnOpenTransaction() throws Exception {
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.begin(getDatabaseName());

      bolt.run("CREATE (n:Issue6804DbSwitch {value: 1}) RETURN n");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(0, -1);
      assertThat(bolt.readSummary().metadata()).containsEntry("has_more", true);

      bolt.run("RETURN 1 AS one", Map.of("db", "a-database-that-does-not-exist"));
      final Summary rejected = bolt.readSummary();
      assertThat(rejected.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(String.valueOf(rejected.metadata().get("message"))).contains("Cannot switch database");

      // The connection must still hold the database the transaction was opened on, so RESET can roll it back.
      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.run("MATCH (n:Issue6804DbSwitch) RETURN n");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      final Summary check = bolt.readSummary();
      assertThat(check.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(check.records()).as("the transaction must have been rolled back, not stranded").isEmpty();
    }
  }

  /**
   * READY - the one state LOGOFF is valid in - still works, and the connection can be re-authenticated
   * afterwards, which is what a driver connection pool actually does with LOGOFF.
   */
  @Test
  void logoffFromReadyStillSucceedsAndAllowsReAuthentication() throws Exception {
    try (final BoltConnection bolt = new BoltConnection(getDatabaseName())) {
      bolt.sendNoFields(BoltMessage.LOGOFF);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.logon();

      bolt.run("RETURN 7 AS seven");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      final Summary summary = bolt.readSummary();
      assertThat(summary.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(summary.records()).hasSize(1);
    }
  }
}
