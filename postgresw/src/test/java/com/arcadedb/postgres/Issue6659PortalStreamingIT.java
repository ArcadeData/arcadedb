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
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #6659: an extended-protocol portal whose columns are fixed before its first row is read - by
 * a projection of declared properties, or by the layout a {@code Describe('S')} promised - keeps its result set open
 * across {@code Execute} calls and pulls only the rows each one asks for, instead of materializing the whole result up
 * front (and refusing it above {@link GlobalConfiguration#POSTGRES_QUERY_MAX_ROWS}, issue #7034).
 * <p>
 * The cursor is a live statement for its whole life: it is listed, a cancel or a termination stops it, and every way a
 * portal can end - drained, closed, rebound, its transaction block ended, the connection dropped - ends it too. Between
 * two {@code Execute} calls it does not hold a query admission slot.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6659PortalStreamingIT extends PostgresWireProtocolTestBase {
  private static final int    ROW_CAP = 5;
  private static final int    ROWS    = ROW_CAP + 10;
  private static final String TYPE    = "Issue6659Row";
  private static final String QUERY   = "SELECT id FROM " + TYPE + " ORDER BY id";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.POSTGRES_QUERY_MAX_ROWS.setValue(ROW_CAP);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.POSTGRES_QUERY_MAX_ROWS.reset();
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    super.endTest();
  }

  private void createRows(final int count) {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType(TYPE);
      type.getOrCreateProperty("id", Type.INTEGER);
      for (int i = 0; i < count; i++)
        database.newDocument(TYPE).set("id", i).set("undeclared", "u" + i).save();
    });
  }

  @Test
  @DisplayName("[#6659] an Execute-only portal is fetched past the cap, in order, the rows each Execute asks for")
  void executeOnlyPortalStreamsPastTheCap() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      beginBlock(out, in);
      sendParse(out, QUERY);
      sendBind(out, "P1");
      assertThat(fetchInBatches(out, in, "P1", 2)).isEqualTo(sequence(0, ROWS));
      endBlock(out, in);
    });
  }

  @Test
  @DisplayName("[#6659] a Describe('P') of a projection of declared properties announces its columns without reading the rows")
  void describedPortalStreamsPastTheCap() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      beginBlock(out, in);
      // The pgjdbc shape: Parse/Bind/Describe('P')/Execute(fetchSize)/Sync
      sendParse(out, QUERY);
      sendBind(out, "P1");
      sendDescribe(out, "P1");
      sendExecute(out, "P1", 3);
      sendSync(out);
      final List<WireMessage> first = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(first)).as("not refused at the cap").doesNotContain('E').contains('T');
      assertThat(dataRowIds(first)).isEqualTo(sequence(0, 3));
      assertThat(messageTypesOf(first)).contains('s').doesNotContain('C');

      final List<Integer> rest = fetchInBatches(out, in, "P1", 4);
      assertThat(rest).isEqualTo(sequence(3, ROWS));
      endBlock(out, in);
    });
  }

  @Test
  @DisplayName("[#6659] Execute(0) drains what a suspended stream has left, under one CommandComplete tagged with the total")
  void executeZeroDrainsTheRestOfAStream() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      beginBlock(out, in);
      sendParse(out, QUERY);
      sendBind(out, "P1");
      sendExecute(out, "P1", 4);
      sendSync(out);
      assertThat(dataRowIds(readUntilReadyForQuery(in))).isEqualTo(sequence(0, 4));

      sendExecute(out, "P1", 0);
      sendSync(out);
      final List<WireMessage> rest = readUntilReadyForQuery(in);
      assertThat(dataRowIds(rest)).isEqualTo(sequence(4, ROWS));
      assertThat(messageTypesOf(rest)).doesNotContain('s', 'E');
      assertThat(commandTagOf(rest)).isEqualTo("SELECT " + ROWS);

      // A drained portal stays drained: no row is sent twice
      sendExecute(out, "P1", 0);
      sendSync(out);
      final List<WireMessage> again = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(again)).doesNotContain('D', 'E').contains('C');
      endBlock(out, in);
    });
  }

  @Test
  @DisplayName("[#6659] a first Execute(0) streams the whole result past the cap, in autocommit")
  void firstExecuteZeroStreamsPastTheCap() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      sendParse(out, QUERY);
      sendBind(out, "");
      sendExecute(out, "", 0);
      sendSync(out);
      final List<WireMessage> response = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(response)).doesNotContain('E', 's');
      assertThat(dataRowIds(response)).isEqualTo(sequence(0, ROWS));
      assertThat(commandTagOf(response)).isEqualTo("SELECT " + ROWS);
      assertThat(liveEntries(QUERY)).as("a drained stream ends its statement").isZero();
    });
  }

  @Test
  @DisplayName("[#6659] a wildcard or an undeclared property is still read whole before its columns are announced")
  void onlyRowsCanNameTheColumnsOfAnUndeclaredProjection() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      for (final String query : new String[] { "SELECT FROM " + TYPE, "SELECT undeclared FROM " + TYPE }) {
        sendParse(out, query);
        sendBind(out, "");
        sendDescribe(out, "");
        sendExecute(out, "", 2);
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as(query).contains('E').doesNotContain('T', 'D');
        assertThat(errorMessageOf(response)).as(query).contains(GlobalConfiguration.POSTGRES_QUERY_MAX_ROWS.getKey());
      }
    });
  }

  @Test
  @DisplayName("[#6659] the stream is a statement until the portal ends: Close ends it")
  void closingAStreamedPortalEndsItsStatement() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      beginBlock(out, in);
      sendParse(out, QUERY);
      sendBind(out, "P1");
      sendExecute(out, "P1", 2);
      sendSync(out);
      assertThat(dataRowIds(readUntilReadyForQuery(in))).isEqualTo(sequence(0, 2));
      assertThat(liveEntries(QUERY)).as("listed while the cursor is open").isEqualTo(1);

      sendClosePortal(out, "P1");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('3').doesNotContain('E');
      assertThat(liveEntries(QUERY)).isZero();

      sendExecute(out, "P1", 2);
      sendSync(out);
      assertThat(errorCodeOf(readUntilReadyForQuery(in))).isEqualTo("34000");
      endBlock(out, in);
    });
  }

  @Test
  @DisplayName("[#6659] binding the name of a suspended stream again ends the old cursor and starts a fresh one")
  void rebindingAStreamedPortalStartsAFreshCursor() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      beginBlock(out, in);
      sendParse(out, QUERY);
      sendBind(out, "P1");
      sendExecute(out, "P1", 3);
      sendSync(out);
      assertThat(dataRowIds(readUntilReadyForQuery(in))).isEqualTo(sequence(0, 3));

      sendBind(out, "P1");
      sendExecute(out, "P1", 2);
      sendSync(out);
      assertThat(dataRowIds(readUntilReadyForQuery(in))).isEqualTo(sequence(0, 2));
      assertThat(liveEntries(QUERY)).as("the first cursor ended with its portal").isEqualTo(1);
      endBlock(out, in);
      assertThat(liveEntries(QUERY)).as("and the second with the block").isZero();
    });
  }

  @Test
  @DisplayName("[#6659] an autocommit stream ends at Sync with its implicit block, as in PostgreSQL")
  void anAutocommitStreamEndsAtSync() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      sendParse(out, QUERY);
      sendBind(out, "P1");
      sendExecute(out, "P1", 2);
      // Flush, not Sync: the implicit block is still open and so is the portal
      sendFlush(out);
      sendExecute(out, "P1", 2);
      sendSync(out);
      final List<WireMessage> first = readUntilReadyForQuery(in);
      assertThat(dataRowIds(first)).isEqualTo(sequence(0, 4));
      assertThat(liveEntries(QUERY)).isZero();

      sendExecute(out, "P1", 2);
      sendSync(out);
      assertThat(errorCodeOf(readUntilReadyForQuery(in))).isEqualTo("34000");
    });
  }

  @Test
  @DisplayName("[#6659] ROLLBACK ends the streams of its block")
  void rollbackEndsTheStreamsOfItsBlock() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      beginBlock(out, in);
      sendParse(out, QUERY);
      sendBind(out, "P1");
      sendExecute(out, "P1", 2);
      sendSync(out);
      readUntilReadyForQuery(in);
      assertThat(liveEntries(QUERY)).isEqualTo(1);

      endBlock(out, in);
      assertThat(liveEntries(QUERY)).isZero();
    });
  }

  @Test
  @DisplayName("[#6659] a client that disconnects in the middle of a stream does not leave its cursor behind")
  void aDisconnectEndsTheStream() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      beginBlock(out, in);
      sendParse(out, QUERY);
      sendBind(out, "P1");
      sendExecute(out, "P1", 2);
      sendSync(out);
      readUntilReadyForQuery(in);
      assertThat(liveEntries(QUERY)).isEqualTo(1);
    });
    await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).until(() -> liveEntries(QUERY) == 0);
  }

  @Test
  @DisplayName("[#6659] a statement of the stream's own transaction runs between two of its fetches")
  void aWriteOfTheSameTransactionRunsBetweenTwoFetches() throws Exception {
    createRows(ROWS);

    withConnection((out, in) -> {
      beginBlock(out, in);
      sendParse(out, QUERY);
      sendBind(out, "P1");
      sendExecute(out, "P1", 2);
      sendSync(out);
      assertThat(dataRowIds(readUntilReadyForQuery(in))).isEqualTo(sequence(0, 2));

      sendSimpleQuery(out, "UPDATE " + TYPE + " SET touched = true WHERE id >= 10");
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

      assertThat(fetchInBatches(out, in, "P1", 4)).isEqualTo(sequence(2, ROWS));
      endBlock(out, in);
    });
  }

  @Test
  @DisplayName("[#6659] pgjdbc's fetch-size cursor reads a result past the cap, also once the statement is server-prepared")
  void pgjdbcFetchSizeCursorStreamsPastTheCap() throws Exception {
    createRows(ROWS);

    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS)) {
      conn.setAutoCommit(false);
      // prepareThreshold is 5: the first runs Describe the portal, the later ones reuse the statement's description
      for (int run = 0; run < 7; run++) {
        try (final PreparedStatement st = conn.prepareStatement(QUERY)) {
          st.setFetchSize(4);
          final List<Integer> ids = new ArrayList<>();
          try (final ResultSet rs = st.executeQuery()) {
            while (rs.next())
              ids.add(rs.getInt("id"));
          }
          assertThat(ids).as("run %d", run).isEqualTo(sequence(0, ROWS));
        }
      }
      conn.rollback();
    }
  }

  @Test
  @DisplayName("[#6659] a fetch from the stream runs as its statement: a CancelRequest stops it")
  void aStreamedFetchIsCancellable() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      database.getSchema().getOrCreateDocumentType("Long6659").getOrCreateProperty("v", Type.INTEGER);
      for (int i = 0; i < 10_000; i++)
        database.newDocument("Long6659").set("v", i).save();
    });
    // One full scan per record, and no record matches: minutes of work inside the first fetch, none of it before
    final String longQuery =
        "SELECT v FROM Long6659 WHERE (SELECT count(*) AS c FROM Long6659 WHERE v = $parent.$current.v + 1000000)[0].c > 0";

    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS); final Statement st = conn.createStatement()) {
      conn.setAutoCommit(false);
      st.setFetchSize(10);
      final CompletableFuture<Void> running = CompletableFuture.runAsync(() -> {
        try (final ResultSet rs = st.executeQuery(longQuery)) {
          while (rs.next())
            rs.getObject(1);
        } catch (final SQLException e) {
          throw new CompletionException(e);
        }
      });

      await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> liveEntries(longQuery) == 1);
      st.cancel();
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).satisfies(e -> assertThat(sqlStateOf(e)).isEqualTo("57014"));
      conn.rollback();
      assertThat(liveEntries(longQuery)).isZero();
    }
  }

  @Test
  @DisplayName("[#6659] a terminated stream fails its next fetch, even when it was terminated between two of them")
  void aTerminatedStreamFailsItsNextFetch() throws Exception {
    createRows(ROWS);

    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS); final Statement st = conn.createStatement()) {
      conn.setAutoCommit(false);
      st.setFetchSize(2);
      try (final ResultSet rs = st.executeQuery(QUERY)) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isZero();

        RunningQuery entry = null;
        for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
          if (QUERY.equals(q.getText()))
            entry = q;
        assertThat(entry).as("the idle cursor is listed").isNotNull();
        assertThat(entry.terminate("test")).isTrue();

        assertThatThrownBy(() -> {
          while (rs.next())
            rs.getInt("id");
        }).satisfies(e -> assertThat(sqlStateOf(e)).isEqualTo("57014"));
      }
      conn.rollback();

      try (final ResultSet rs = st.executeQuery("SELECT 1 AS one")) {
        assertThat(rs.next()).isTrue();
      }
    }
  }

  @Test
  @DisplayName("[#6659] an idle cursor holds no admission slot: another connection's query starts while it waits")
  void anIdleCursorHoldsNoAdmissionSlot() throws Exception {
    createRows(ROWS);
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    // A query that cannot start at once is refused, so a slot held by the idle cursor fails the test instead of slowing it
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    try (final Connection streaming = connect("root", DEFAULT_PASSWORD_FOR_TESTS);
        final Statement st = streaming.createStatement();
        final Connection other = connect("root", DEFAULT_PASSWORD_FOR_TESTS);
        final Statement otherSt = other.createStatement()) {
      streaming.setAutoCommit(false);
      st.setFetchSize(2);
      try (final ResultSet rs = st.executeQuery(QUERY)) {
        assertThat(rs.next()).isTrue();

        try (final ResultSet otherRs = otherSt.executeQuery("SELECT count(*) AS c FROM " + TYPE)) {
          assertThat(otherRs.next()).isTrue();
          assertThat(otherRs.getLong("c")).isEqualTo(ROWS);
        }

        int read = 1;
        while (rs.next())
          read++;
        assertThat(read).isEqualTo(ROWS);
      }
      streaming.rollback();
    }
  }

  @Test
  @DisplayName("[#6659] a type the user cannot read is not described from its schema")
  void aTypeTheUserCannotReadIsNotDescribedFromItsSchema() throws Exception {
    createRows(2);
    final ServerSecurity security = getServer(0).getSecurity();
    final String group = "noread6659";
    final String user = "noread6659";
    final String password = "noread6659-secret";
    security.saveGroup(getDatabaseName(), group, new JSONObject().put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray()))));
    security.createUser(new JSONObject().put("name", user).put("password", security.encodePassword(password))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { group }))));
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in, user, password);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, QUERY);
        sendBind(out, "");
        sendDescribe(out, "");
        sendExecute(out, "", 0);
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("refused, not described").contains('E').doesNotContain('T', 'D');
      });
    } finally {
      security.dropUser(user);
      security.deleteGroup(getDatabaseName(), group);
    }
  }

  /**
   * The columns of a streamed portal come from the schema, those of the simple query protocol from the rows: for every
   * declared type a streamed projection can name, the two have to agree, on the type and on the text of the value.
   */
  @Test
  @DisplayName("[#6659] a streamed projection is announced and encoded exactly as the rows would have it")
  void streamedColumnsMatchTheColumnsOfTheRows() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final Map<String, Object> values = new LinkedHashMap<>();
    values.put("pBoolean", true);
    values.put("pInteger", 42);
    values.put("pShort", (short) 7);
    values.put("pLong", 9_000_000_000L);
    values.put("pFloat", 1.5f);
    values.put("pDouble", 2.25d);
    values.put("pByte", (byte) 3);
    values.put("pString", "text");
    values.put("pDecimal", new BigDecimal("12345.678"));
    values.put("pDate", LocalDate.of(2024, 5, 19));
    values.put("pDatetime", new Date(1_716_138_311_000L));
    values.put("pDatetimeSecond", new Date(1_716_138_311_000L));
    values.put("pDatetimeMicros", new Date(1_716_138_311_000L));
    values.put("pDatetimeNanos", new Date(1_716_138_311_000L));
    values.put("pBinary", new byte[] { 1, 2, 3 });
    values.put("pMap", Map.of("k", "v"));
    values.put("pListString", List.of("a", "b"));
    values.put("pListInteger", List.of(1, 2));
    values.put("pListLong", List.of(1L, 2L));
    values.put("pListDouble", List.of(1.5d, 2.5d));
    values.put("pListBoolean", List.of(true, false));
    values.put("pListDecimal", List.of(new BigDecimal("1.5")));

    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("Types6659");
      type.getOrCreateProperty("pBoolean", Type.BOOLEAN);
      type.getOrCreateProperty("pInteger", Type.INTEGER);
      type.getOrCreateProperty("pShort", Type.SHORT);
      type.getOrCreateProperty("pLong", Type.LONG);
      type.getOrCreateProperty("pFloat", Type.FLOAT);
      type.getOrCreateProperty("pDouble", Type.DOUBLE);
      type.getOrCreateProperty("pByte", Type.BYTE);
      type.getOrCreateProperty("pString", Type.STRING);
      type.getOrCreateProperty("pDecimal", Type.DECIMAL);
      type.getOrCreateProperty("pDate", Type.DATE);
      type.getOrCreateProperty("pDatetime", Type.DATETIME);
      type.getOrCreateProperty("pDatetimeSecond", Type.DATETIME_SECOND);
      type.getOrCreateProperty("pDatetimeMicros", Type.DATETIME_MICROS);
      type.getOrCreateProperty("pDatetimeNanos", Type.DATETIME_NANOS);
      type.getOrCreateProperty("pBinary", Type.BINARY);
      type.getOrCreateProperty("pMap", Type.MAP);
      type.getOrCreateProperty("pListString", Type.LIST, "STRING");
      type.getOrCreateProperty("pListInteger", Type.LIST, "INTEGER");
      type.getOrCreateProperty("pListLong", Type.LIST, "LONG");
      type.getOrCreateProperty("pListDouble", Type.LIST, "DOUBLE");
      type.getOrCreateProperty("pListBoolean", Type.LIST, "BOOLEAN");
      type.getOrCreateProperty("pListDecimal", Type.LIST, "DECIMAL");
      final MutableDocument doc = database.newDocument("Types6659");
      values.forEach(doc::set);
      doc.save();
    });

    final String query = "SELECT " + String.join(", ", values.keySet()) + " FROM Types6659";
    final List<String> simple = describeAndRead(query, "simple");
    final List<String> streamed = describeAndRead(query, "extended");
    assertThat(streamed).isEqualTo(simple);
  }

  // ---------------------------------------------------------------------------------------------

  /** One "name:type=value" entry per column, as pgjdbc reads them under {@code preferQueryMode}. */
  private List<String> describeAndRead(final String query, final String mode) throws Exception {
    final Properties props = new Properties();
    props.setProperty("preferQueryMode", mode);
    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS, props); final Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery(query)) {
      final ResultSetMetaData metaData = rs.getMetaData();
      assertThat(rs.next()).isTrue();
      final List<String> columns = new ArrayList<>();
      for (int i = 1; i <= metaData.getColumnCount(); i++)
        columns.add(metaData.getColumnName(i) + ":" + metaData.getColumnTypeName(i) + "=" + rs.getString(i));
      return columns;
    }
  }

  @FunctionalInterface
  private interface WireSession {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void withConnection(final WireSession session) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in, "root", DEFAULT_PASSWORD_FOR_TESTS);
      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> session.run(out, in));
    }
  }

  /** Executes the portal {@code batch} rows at a time until it completes, checking each answer's terminator. */
  private static List<Integer> fetchInBatches(final DataOutputStream out, final DataInputStream in, final String portal,
      final int batch) throws Exception {
    final List<Integer> ids = new ArrayList<>();
    while (true) {
      sendExecute(out, portal, batch);
      sendSync(out);
      final List<WireMessage> response = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(response)).as("a streamed fetch is not refused").doesNotContain('E');
      final List<Integer> rows = dataRowIds(response);
      ids.addAll(rows);
      if (messageTypesOf(response).contains('s')) {
        assertThat(rows).as("a suspended fetch sent every row it was asked for").hasSize(batch);
        assertThat(messageTypesOf(response)).doesNotContain('C');
      } else {
        assertThat(messageTypesOf(response)).contains('C');
        assertThat(rows.size()).isLessThanOrEqualTo(batch);
        return ids;
      }
    }
  }

  private int liveEntries(final String text) {
    int count = 0;
    for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
      if (text.equals(q.getText()))
        count++;
    return count;
  }

  private static List<Integer> sequence(final int from, final int to) {
    final List<Integer> ids = new ArrayList<>(to - from);
    for (int i = from; i < to; i++)
      ids.add(i);
    return ids;
  }

  private static String sqlStateOf(final Throwable e) {
    Throwable cause = e;
    while (cause != null && !(cause instanceof SQLException))
      cause = cause.getCause();
    assertThat(cause).as("%s", e).isInstanceOf(SQLException.class);
    return ((SQLException) cause).getSQLState();
  }

  private Connection connect(final String user, final String password) throws Exception {
    return connect(user, password, new Properties());
  }

  private Connection connect(final String user, final String password, final Properties props) throws Exception {
    Class.forName("org.postgresql.Driver");
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }

  private static void authenticate(final DataOutputStream out, final DataInputStream in, final String user, final String password)
      throws Exception {
    sendStartupMessage(out, user, "postgresdb");
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, password);
    readMessageOfType(in, 'Z');
  }

  private static void beginBlock(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "BEGIN");
    assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
  }

  private static void endBlock(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "ROLLBACK");
    assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
  }

  private static void sendSimpleQuery(final DataOutputStream out, final String query) throws Exception {
    final byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
    out.writeByte('Q');
    out.writeInt(4 + queryBytes.length + 1);
    out.write(queryBytes);
    out.writeByte(0);
    out.flush();
  }

  private static void sendParse(final DataOutputStream out, final String query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // unnamed statement
    writeCString(body, query);
    body.write(0);
    body.write(0); // int16 numParamDataTypes = 0
    writeFramed(out, 'P', body);
  }

  private static void sendBind(final DataOutputStream out, final String portalName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    writeCString(body, ""); // unnamed statement
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0
    body.write(0);
    body.write(0); // int16 numParamValues = 0
    body.write(0);
    body.write(0); // int16 numResultFormatCodes = 0 (all text)
    writeFramed(out, 'B', body);
  }

  private static void sendDescribe(final DataOutputStream out, final String portalName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('P');
    writeCString(body, portalName);
    writeFramed(out, 'D', body);
  }

  private static void sendClosePortal(final DataOutputStream out, final String portalName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('P');
    writeCString(body, portalName);
    writeFramed(out, 'C', body);
  }

  private static void sendExecute(final DataOutputStream out, final String portalName, final int maxRows) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    body.write((maxRows >>> 24) & 0xFF);
    body.write((maxRows >>> 16) & 0xFF);
    body.write((maxRows >>> 8) & 0xFF);
    body.write(maxRows & 0xFF);
    writeFramed(out, 'E', body);
  }

  private static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
  }

  private static void sendFlush(final DataOutputStream out) throws Exception {
    out.writeByte('H');
    out.writeInt(4);
    out.flush();
  }

  private static void writeFramed(final DataOutputStream out, final char type, final ByteArrayOutputStream body) throws Exception {
    final byte[] bodyBytes = body.toByteArray();
    out.writeByte(type);
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private record WireMessage(char type, byte[] body) {
  }

  private static WireMessage readWireMessage(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    final byte[] body = new byte[length - 4];
    in.readFully(body);
    return new WireMessage((char) type, body);
  }

  private static List<WireMessage> readUntilReadyForQuery(final DataInputStream in) throws Exception {
    final List<WireMessage> messages = new ArrayList<>();
    WireMessage message;
    do {
      message = readWireMessage(in);
      messages.add(message);
    } while (message.type() != 'Z');
    return messages;
  }

  private static List<Character> messageTypesOf(final List<WireMessage> messages) {
    final List<Character> types = new ArrayList<>(messages.size());
    for (final WireMessage message : messages)
      types.add(message.type());
    return types;
  }

  /** The first column of every DataRow, read as the integer its text format carries. */
  private static List<Integer> dataRowIds(final List<WireMessage> messages) throws Exception {
    final List<Integer> ids = new ArrayList<>();
    for (final WireMessage message : messages)
      if (message.type() == 'D') {
        final DataInputStream body = new DataInputStream(new ByteArrayInputStream(message.body()));
        assertThat(body.readUnsignedShort()).as("one column per row").isEqualTo(1);
        final byte[] value = new byte[body.readInt()];
        body.readFully(value);
        ids.add(Integer.parseInt(new String(value, StandardCharsets.UTF_8)));
      }
    return ids;
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'C')
        return new String(message.body(), 0, message.body().length - 1, StandardCharsets.UTF_8);
    throw new AssertionError("no CommandComplete in " + messageTypesOf(messages));
  }

  private static String errorCodeOf(final List<WireMessage> messages) {
    return errorField(messages, 'C');
  }

  private static String errorMessageOf(final List<WireMessage> messages) {
    return errorField(messages, 'M');
  }

  private static String errorField(final List<WireMessage> messages, final char field) {
    for (final WireMessage message : messages)
      if (message.type() == 'E') {
        final byte[] body = message.body();
        int i = 0;
        while (i < body.length && body[i] != 0) {
          final char code = (char) body[i++];
          final int start = i;
          while (body[i] != 0)
            i++;
          if (code == field)
            return new String(body, start, i - start, StandardCharsets.UTF_8);
          i++;
        }
      }
    throw new AssertionError("no ErrorResponse in " + messageTypesOf(messages));
  }
}
