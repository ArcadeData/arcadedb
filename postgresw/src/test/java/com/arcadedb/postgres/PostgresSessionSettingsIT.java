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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.firstColumnName;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendClose;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.show;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Session settings over the PostgreSQL wire protocol: {@code SET} (with its {@code SESSION} and {@code LOCAL}
 * modifiers), {@code SHOW}, {@code RESET} and {@code DISCARD}, when a {@code SET} takes effect on the simple and the
 * extended query protocol, that it is scoped to the session (and to the transaction for {@code SET LOCAL}), how an
 * unparseable or unsupported one is refused, and the {@code ParameterStatus} messages that report a change.
 */
class PostgresSessionSettingsIT extends PostgresWireProtocolTestBase {

  /*
   * Regression tests for issue #6701: two pre-existing gaps in
   * {@code PostgresNetworkExecutor.parseSetCommand()}, found by CodeRabbit
   * while reviewing PR #6697 (unrelated to that PR's own fix).
   * <ul>
   *   <li>{@code SET SESSION <name> = <value>} / {@code SET LOCAL <name> = <value>} were not recognized: the
   *   {@code SESSION }/{@code LOCAL } modifier became part of the parameter name (e.g.
   *   {@code "session datestyle"}), so the {@code datestyle} special case never fired.</li>
   *   <li>The extended query protocol's {@code Parse} dispatch passed {@code portal.query} to
   *   {@code setConfiguration()} without stripping a trailing {@code ;} first, unlike the simple-query
   *   protocol's {@code queryCommand()}, which already strips one from {@code queryText}.</li>
   * </ul>
   * These tests speak the wire protocol directly over a raw socket so the extended-query {@code Parse} path
   * is actually exercised (a JDBC client normally sends {@code SET} over the simple-query protocol, which
   * would not reach the second bug at all). Each observes the {@code datestyle} it set through {@code SHOW datestyle}
   * on the same connection: a {@code SET} is session-scoped (issue #8217), so the connection that ran it is the only
   * place its effect is visible. {@code DMY} is used because it is not the default order, so a {@code SET} that did
   * not take effect cannot pass.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6701
  @Test
  @DisplayName("[#6701] SET SESSION <name> = <value> over the simple-query protocol resolves to the plain parameter name")
  void setSessionModifierIsHonoredOverSimpleProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "SET SESSION datestyle = 'DMY'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in)))
            .as("no ErrorResponse for a SET SESSION command").doesNotContain('E');

        assertThat(PostgresWireMessages.show(out, in, "datestyle"))
            .as("SET SESSION datestyle = 'DMY' must resolve the parameter name to plain 'datestyle', "
                + "not 'session datestyle', so the datestyle special case actually fires")
            .isEqualTo("ISO, DMY");
      });
    }
  }

  // Regression: #6701
  @Test
  @DisplayName("[#6701] SET LOCAL <name> = <value> over the simple-query protocol resolves to the plain parameter name")
  void setLocalModifierIsHonoredOverSimpleProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // Inside a block: a SET LOCAL lasts only until the end of its transaction (issue #8242), and outside one the
        // statement is its own transaction, so there would be nothing left to observe.
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SET LOCAL datestyle = 'DMY'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in)))
            .as("no ErrorResponse for a SET LOCAL command").doesNotContain('E');

        assertThat(PostgresWireMessages.show(out, in, "datestyle"))
            .as("SET LOCAL datestyle = 'DMY' must resolve the parameter name to plain 'datestyle', "
                + "not 'local datestyle', so the datestyle special case actually fires")
            .isEqualTo("ISO, DMY");

        sendSimpleQuery(out, "COMMIT");
        readUntilReadyForQuery(in);
        assertThat(PostgresWireMessages.show(out, in, "datestyle")).as("SET LOCAL ends with its transaction")
            .isEqualTo("ISO, MDY");
      });
    }
  }

  // Regression: #6701
  @Test
  @DisplayName("[#6701] a semicolon-terminated SET over the extended query protocol is honored, not rejected")
  void trailingSemicolonIsStrippedOverExtendedProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // Sent as its own Parse+Bind+Execute+Sync round trip, the extended-query dispatch in
        // parseCommand() (not queryCommand()'s simple-query dispatch, which already strips a trailing ';').
        final List<WireMessage> messages = runExtendedQuery(out, in, "SET datestyle = 'DMY';");
        assertThat(messageTypesOf(messages))
            .as("a trailing ';' must not make setConfiguration() reject the value").doesNotContain('E');

        assertThat(PostgresWireMessages.show(out, in, "datestyle"))
            .as("SET datestyle = 'DMY'; over the extended protocol must still apply, "
                + "not fail to parse because of the glued-on trailing ';'")
            .isEqualTo("ISO, DMY");
      });
    }
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  /**
   * Runs one statement as its own Parse+Bind+Execute+Sync round trip over the extended query protocol (the
   * unnamed statement/portal, no parameters), then reads every message the server sends back up to and
   * including the {@code ReadyForQuery} the Sync produces.
   */
  private static List<WireMessage> runExtendedQuery(final DataOutputStream out, final DataInputStream in, final String query)
      throws Exception {
    sendParse(out, "", query);
    sendBind(out, "", "");
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  /*
   * Regression tests for issue #8135: on the extended query protocol, {@code SET} ran {@code setConfiguration()}
   * at Parse and only marked the portal {@code ignoreExecution}, so the side effect happened when the statement was
   * PREPARED rather than when it was EXECUTED - the rule {@code BEGIN}/{@code COMMIT}/{@code ROLLBACK} were moved to
   * by #6543/#7905. Two consequences:
   * <ul>
   *   <li>a {@code SET} that is only prepared (Parse, or Parse+Bind, then Sync) still changed the session;</li>
   *   <li>a cached {@code SET} bound and executed again with no Parse in between - what a pgjdbc/psycopg3 statement
   *   cache sends once the statement is promoted - answered {@code CommandComplete SET} and re-applied nothing.</li>
   * </ul>
   * A {@code SET} is session-scoped (issue #8217), so every test observes it through {@code SHOW} on the connection
   * that ran it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String PARAMETER   = "application_name";
  private static final String VALUE       = "issue8135";
  private static final String SET_VALUE   = "SET " + PARAMETER + " = '" + VALUE + "'";
  private static final String OTHER       = "other";
  private static final String REFUSED_SET = "SET server_version = '1'";

  // Regression: #8135
  @Test
  @DisplayName("[#8135] a SET that is only prepared - Parse, then Parse+Bind, each followed by Sync - does not change the session")
  void preparedButNotExecutedSetHasNoEffect() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // Parse and Sync: what a driver sends to prepare a statement ahead of using it.
        sendParse(out, "s", SET_VALUE);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the Parse is accepted").containsExactly('1', 'Z');
        assertThat(show(out, in, PARAMETER)).as("preparing a SET must not apply it").isNotEqualTo(VALUE);

        // An explicit block keeps portal "p" alive across the Syncs below: in autocommit a Sync ends the implicit
        // transaction and the portal with it, as in PostgreSQL (issue #8212).
        sendSimpleQuery(out, "BEGIN");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

        // Bound and described but still never executed.
        sendBind(out, "p", "s");
        sendDescribe(out, 'P', "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(show(out, in, PARAMETER)).as("binding and describing a SET must not apply it either").isNotEqualTo(VALUE);

        // The Execute is what applies it: this is what proves the two assertions above could have failed.
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> executed = readUntilReadyForQuery(in);
        // The ParameterStatus announces the application_name the SET changed (issue #8241)
        assertThat(messageTypesOf(executed)).containsExactly('C', 'S', 'Z');
        assertThat(show(out, in, PARAMETER)).as("executing the SET applies it").isEqualTo(VALUE);

        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
      });
    }
  }

  // Regression: #8135
  @Test
  @DisplayName("[#8135] a cached SET bound and executed again without a new Parse re-applies the setting")
  void reExecutedCachedSetIsReapplied() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "s", SET_VALUE);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(show(out, in, PARAMETER)).isEqualTo(VALUE);

        // Something else changes the value the SET established.
        setOther(out, in);

        // The statement cache re-runs the prepared SET to restore it: Bind+Execute of the same name, no Parse.
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> reuse = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(reuse)).as("the re-executed SET answers CommandComplete, and reports the application_name it restored")
            .containsExactly('2', 'C', 'S', 'Z');
        assertThat(show(out, in, PARAMETER)).as("a re-executed cached SET must apply the setting again, not only claim to")
            .isEqualTo(VALUE);
      });
    }
  }

  // Regression: #8135
  @Test
  @DisplayName("[#8135] re-running the same already-executed SET portal without a new Bind applies it only once")
  void replayingTheBoundSetPortalAppliesOnlyOnce() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // An explicit block keeps portal "p" alive across the Sync and the simple queries: in autocommit a Sync or a
        // simple Query ends the implicit transaction and the portal with it, as in PostgreSQL (issue #8212).
        sendSimpleQuery(out, "BEGIN");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

        sendParse(out, "s", SET_VALUE);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(show(out, in, PARAMETER)).isEqualTo(VALUE);

        setOther(out, in);

        // No Bind: the same boundary applyTransactionControl() draws (#7851) - the marker belongs to the bound
        // portal and is consumed by its first Execute; only a new Bind of the statement carries it again.
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
        assertThat(show(out, in, PARAMETER)).as("a replayed portal applies its SET once").isEqualTo(OTHER);

        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
      });
    }
  }

  // Regression: #8135
  @Test
  @DisplayName("[#8135] a SET whose Execute is discarded by an earlier error in the same pipeline does not take effect")
  void setDiscardedBySkipUntilSyncHasNoEffect() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // The SET is parsed BEFORE the failing statement, so its Parse succeeds; its Bind and Execute come after
        // the error and are discarded up to the Sync, as PostgreSQL discards them.
        sendParse(out, "s", SET_VALUE);
        sendParse(out, "bad", "SELEC 1");
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> pipeline = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(pipeline)).as("the malformed statement fails the pipeline").contains('E');
        assertThat(messageTypesOf(pipeline)).as("the discarded Execute answers nothing").doesNotContain('C');
        assertThat(show(out, in, PARAMETER)).as("a SET the client never got to execute must not have been applied")
            .isNotEqualTo(VALUE);
      });
    }
  }

  // Regression: #8135
  /**
   * Issue #8261: this test used to assert the retry's SQLSTATE was {@code 55P02} (refused again, same reason),
   * which was meant to prove {@code applyPendingSetting()}'s marker-clearing rule - cleared only AFTER the SET
   * applies, so a refused attempt leaves the marker standing and a replay is refused again rather than answered
   * {@code CommandComplete SET}. That specific ordering has no wire-observable form any more: in autocommit (this
   * test's shape) the Sync after the refused Execute ends the implicit transaction and drops the portal with it
   * (#8212), so the retry is answered {@code 34000} (portal missing) before it ever reaches the marker, and inside
   * an explicit block the refusal aborts the block with no way to recover it and retry the same portal, since
   * {@code ROLLBACK TO SAVEPOINT} is refused at Parse (#7846). The test still passes whether the marker is cleared
   * before or after the apply - only the weaker assertion below (an error either way, never a false success) is
   * still real and wire-observable, so that is what it now asserts. The marker-clearing order itself is pinned at
   * unit level by {@link PostgresApplyPendingSettingTest}.
   */
  @Test
  @DisplayName("[#8135] a SET refused at Execute, then replayed, is never answered as CommandComplete")
  void refusedSetIsRefusedAgainOnReplay() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // server_version is read-only, in PostgreSQL as here (issue #8217): the SET parses, and is refused at Execute.
        sendParse(out, "s", REFUSED_SET);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).as("the SET is refused at Execute").contains('E').doesNotContain('C');
        assertThat(sqlStateOf(refused)).as("cant_change_runtime_param, as PostgreSQL answers it").isEqualTo("55P02");

        // Recovered by the Sync, the client retries the SAME bound portal - no new Bind. Since #8212 the Sync that
        // ended the implicit transaction also dropped the portal, so this is answered 34000 (portal missing), not
        // 55P02 - see this method's javadoc. Either way it must not answer "CommandComplete SET" having applied
        // nothing.
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> retried = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(retried)).as("the replay is answered as an error, not reported as applied")
            .contains('E').doesNotContain('C');

        assertThat(show(out, in, "server_version")).as("a refused SET changes nothing")
            .isEqualTo(PostgresNetworkExecutor.PG_SERVER_VERSION);
      });
    }
  }

  private static void setOther(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "SET " + PARAMETER + " = '" + OTHER + "'");
    assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
    assertThat(show(out, in, PARAMETER)).isEqualTo(OTHER);
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  /*
   * Regression tests for issue #8217: a PostgreSQL {@code SET} is session-scoped, and on this server it was not in
   * either direction.
   * <ul>
   *   <li>{@code SET datestyle = ISO} rewrote the schema's date-time format, shared by every session on every protocol,
   *   and needed {@code UPDATE_DATABASE_SETTINGS} to do it;</li>
   *   <li>every other {@code SET} was recorded and never read: {@code SHOW} answered from a hard-coded table.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String RESTRICTED_USER     = "issue8217noSettings";
  private static final String RESTRICTED_PASSWORD = "issue8217noSettingsPwd";
  private static final String RESTRICTED_GROUP    = "issue8217NoSettings";

  // Regression: #8217
  @Test
  @DisplayName("[#8217] SET datestyle = ISO leaves the database-wide date-time format alone")
  void setDatestyleDoesNotRewriteTheSchemaFormat() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String formatBefore = database.getSchema().getDateTimeFormat();

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in, "root", DEFAULT_PASSWORD_FOR_TESTS);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET datestyle = 'ISO'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // Over the extended protocol too: the path #8135 moved the SET to.
        sendParse(out, "", "SET datestyle TO 'ISO'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        assertThat(show(out, in, "datestyle")).as("the session answers the style it set, in PostgreSQL's spelling")
            .isEqualTo("ISO, MDY");
      });
    }

    assertThat(database.getSchema().getDateTimeFormat()).as("a pg session's SET must not change the database-wide format")
        .isEqualTo(formatBefore);

    // What the schema write used to break for every other client: a datetime literal in the database's own format.
    database.command("sql", "CREATE DOCUMENT TYPE Issue8217 IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY Issue8217.created IF NOT EXISTS DATETIME");
    database.transaction(() -> database.command("sql", "INSERT INTO Issue8217 SET created = '2024-05-19 17:05:11'"));
    try (final var rs = database.query("sql", "SELECT count(*) AS c FROM Issue8217")) {
      assertThat(rs.next().<Long>getProperty("c")).isGreaterThanOrEqualTo(1L);
    }
  }

  // Regression: #8217
  @Test
  @DisplayName("[#8217] a user without UPDATE_DATABASE_SETTINGS can run SET datestyle, a purely per-session statement")
  void setDatestyleNeedsNoDatabasePermission() throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), RESTRICTED_GROUP, new JSONObject().put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));
    security.createUser(new JSONObject().put("name", RESTRICTED_USER).put("password", security.encodePassword(RESTRICTED_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { RESTRICTED_GROUP }))));

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in, RESTRICTED_USER, RESTRICTED_PASSWORD);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET datestyle = 'ISO, DMY'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("simple protocol").doesNotContain('E');

        sendParse(out, "", "SET datestyle = 'ISO, YMD'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("extended protocol").doesNotContain('E');

        assertThat(show(out, in, "datestyle")).isEqualTo("ISO, YMD");
      });
    } finally {
      security.dropUser(RESTRICTED_USER);
      security.deleteGroup(getDatabaseName(), RESTRICTED_GROUP);
    }
  }

  // Regression: #8217
  @Test
  @DisplayName("[#8217] SHOW answers what this session SET, and another session does not see it")
  void showReadsTheSessionsOwnSettings() throws Exception {
    try (final Socket first = connect(); final Socket second = connect()) {
      final DataOutputStream out1 = new DataOutputStream(first.getOutputStream());
      final DataInputStream in1 = new DataInputStream(first.getInputStream());
      authenticate(out1, in1, "root", DEFAULT_PASSWORD_FOR_TESTS);
      final DataOutputStream out2 = new DataOutputStream(second.getOutputStream());
      final DataInputStream in2 = new DataInputStream(second.getInputStream());
      authenticate(out2, in2, "root", DEFAULT_PASSWORD_FOR_TESTS);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out1, "SET work_mem TO x");
        assertThat(messageTypesOf(readUntilReadyForQuery(in1))).doesNotContain('E');
        sendSimpleQuery(out1, "SET datestyle = 'DMY'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in1))).doesNotContain('E');

        assertThat(show(out1, in1, "work_mem")).as("SHOW reads back what SET stored").isEqualTo("x");
        assertThat(show(out1, in1, "WORK_MEM")).as("parameter names are case-insensitive").isEqualTo("x");
        assertThat(show(out1, in1, "datestyle")).isEqualTo("ISO, DMY");

        // SHOW over the extended protocol reads the same settings.
        sendParse(out1, "", "SHOW work_mem");
        sendBind(out1, "", "");
        sendExecute(out1, "");
        sendSync(out1);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in1))).as("extended-protocol SHOW").isEqualTo("x");

        assertThat(show(out2, in2, "work_mem")).as("another session keeps its own value").isEmpty();
        assertThat(show(out2, in2, "datestyle")).as("another session keeps its own value").isEqualTo("ISO, MDY");

        sendSimpleQuery(out1, "SET work_mem TO DEFAULT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in1))).doesNotContain('E');
        assertThat(show(out1, in1, "work_mem")).as("SET ... TO DEFAULT restores the default").isEmpty();
      });
    }
  }

  // Regression: #8217
  @Test
  @DisplayName("[#8217] a SET PostgreSQL refuses is refused, with its SQLSTATE, and changes nothing")
  void invalidAndReadOnlySetsAreRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in, "root", DEFAULT_PASSWORD_FOR_TESTS);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET server_version = '99'");
        assertThat(sqlStateOf(readUntilReadyForQuery(in))).as("cant_change_runtime_param").isEqualTo("55P02");
        assertThat(show(out, in, "server_version")).isEqualTo(PostgresNetworkExecutor.PG_SERVER_VERSION);

        sendSimpleQuery(out, "SET datestyle = 'nonsense'");
        assertThat(sqlStateOf(readUntilReadyForQuery(in))).as("invalid_parameter_value").isEqualTo("22023");
        assertThat(show(out, in, "datestyle")).isEqualTo("ISO, MDY");
      });
    }
  }

  // Regression: #8217
  @Test
  @DisplayName("[#8217] run-time parameters sent in the startup packet are what SHOW answers")
  void startupParametersAreShown() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      // What pgjdbc sends: DateStyle and TimeZone in PostgreSQL's mixed case.
      final ByteArrayOutputStream body = new ByteArrayOutputStream();
      for (final String s : new String[] { "user", "root", "database", getDatabaseName(), "DateStyle", "ISO, DMY", "TimeZone",
          "Europe/Rome", "application_name", "issue8217" })
        writeCString(body, s);
      body.write(0);
      final byte[] bodyBytes = body.toByteArray();
      out.writeInt(4 + 4 + bodyBytes.length);
      out.writeInt(196608); // protocol version 3.0
      out.write(bodyBytes);
      out.flush();
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(show(out, in, "datestyle")).isEqualTo("ISO, DMY");
        assertThat(show(out, in, "timezone")).isEqualTo("Europe/Rome");
        assertThat(show(out, in, "application_name")).isEqualTo("issue8217");
        // user is a startup field, not a run-time parameter: PostgreSQL does not know it as one (issue #8573)
        sendSimpleQuery(out, "SHOW user");
        assertThat(sqlStateOf(readUntilReadyForQuery(in))).isEqualTo("42704");
      });
    }
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in, final String user, final String password)
      throws Exception {
    sendStartupMessage(out, user, getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, password);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  /*
   * Regression tests for four Postgres-wire issues around {@code SET} and Execute:
   * <ul>
   *   <li>#8241: a {@code SET} of a reported parameter sent no {@code ParameterStatus}, and startup announced only three
   *   of them;</li>
   *   <li>#8242: a {@code SET} inside a rolled-back block was not undone, {@code SET LOCAL} outlived its transaction, and
   *   {@code RESET} was not recognized;</li>
   *   <li>#8244: Execute answered with a {@code RowDescription} the client never asked for, which broke every pgjdbc
   *   connection on the execution that promoted a prepared statement;</li>
   *   <li>#8306: a {@code SET} on the simple protocol answered a one-row "Setting ignored" table instead of the bare
   *   {@code CommandComplete SET}.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #8241
  @Test
  @DisplayName("[#8241] startup announces every reported parameter, the startup packet's own values included")
  void startupReportsEveryReportedParameter() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      final ByteArrayOutputStream body = new ByteArrayOutputStream();
      for (final String s : new String[] { "user", "root", "database", getDatabaseName(), "DateStyle", "ISO, DMY", "TimeZone",
          "Europe/Rome", "application_name", "issue8241" })
        writeCString(body, s);
      body.write(0);
      final byte[] bodyBytes = body.toByteArray();
      out.writeInt(4 + 4 + bodyBytes.length);
      out.writeInt(196608); // protocol version 3.0
      out.write(bodyBytes);
      out.flush();
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);

      final Map<String, String> reported = parameterStatusesOf(readUntilReadyForQuery(in));
      assertThat(reported).containsEntry("server_version", PostgresNetworkExecutor.PG_SERVER_VERSION)
          .containsEntry("server_encoding", "UTF8")
          .containsEntry("client_encoding", "UTF8")
          .containsEntry("DateStyle", "ISO, DMY")
          .containsEntry("TimeZone", "Europe/Rome")
          .containsEntry("application_name", "issue8241")
          .containsEntry("integer_datetimes", "on")
          .containsEntry("standard_conforming_strings", "on")
          .containsEntry("IntervalStyle", "postgres")
          .containsEntry("is_superuser", "on");
    }
  }

  // Regression: #8241
  @Test
  @DisplayName("[#8241] a SET of a reported parameter is answered with a ParameterStatus carrying what SHOW answers")
  void setOfAReportedParameterSendsParameterStatus() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET application_name = 'issue8241'");
        List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(parameterStatusesOf(response)).containsExactly(Map.entry("application_name", "issue8241"));
        assertThat(messageTypesOf(response)).as("ParameterStatus right before the ReadyForQuery").endsWith('S', 'Z');

        // The value SHOW answers, never the raw SET text: pgjdbc closes the connection on a DateStyle that does not
        // start with ISO.
        sendSimpleQuery(out, "SET DateStyle = 'SQL, DMY'");
        assertThat(parameterStatusesOf(readUntilReadyForQuery(in))).containsExactly(Map.entry("DateStyle", "ISO, DMY"));

        // Over the extended protocol: reported before the ReadyForQuery the Sync answers.
        sendParse(out, "", "SET TimeZone TO 'Europe/Rome'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        response = readUntilReadyForQuery(in);
        assertThat(parameterStatusesOf(response)).containsExactly(Map.entry("TimeZone", "Europe/Rome"));
        assertThat(messageTypesOf(response)).containsExactly('1', '2', 'C', 'S', 'Z');

        // A value that does not change is not reported, and neither is a parameter PostgreSQL does not report.
        sendSimpleQuery(out, "SET client_encoding = 'LATIN1'");
        assertThat(parameterStatusesOf(readUntilReadyForQuery(in))).as("client_encoding still answers UTF8").isEmpty();
        sendSimpleQuery(out, "SET work_mem TO x");
        assertThat(parameterStatusesOf(readUntilReadyForQuery(in))).isEmpty();
        sendSimpleQuery(out, "SET application_name = 'issue8241'");
        assertThat(parameterStatusesOf(readUntilReadyForQuery(in))).as("unchanged").isEmpty();
      });
    }
  }

  // Regression: #8306
  @Test
  @DisplayName("[#8306] a SET on the simple protocol answers the bare CommandComplete SET, like the extended protocol")
  void simpleQuerySetAnswersNoRows() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET work_mem TO x");
        List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("SET");

        sendSimpleQuery(out, "SET LOCAL work_mem TO y");
        response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("SET");

        sendSimpleQuery(out, "RESET work_mem");
        response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("RESET");
      });
    }
  }

  // Regression: #8242
  @Test
  @DisplayName("[#8242] a SET inside a rolled-back block is undone, and the ROLLBACK reports the restored value")
  void setInsideARolledBackBlockIsUndone() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "SET work_mem TO before");
        simple(out, in, "BEGIN");
        simple(out, in, "SET work_mem TO x");
        assertThat(parameterStatusesOf(simple(out, in, "SET application_name = 'inside'"))).containsEntry("application_name", "inside");
        assertThat(show(out, in, "work_mem")).as("visible inside the block").isEqualTo("x");
        final List<WireMessage> rollback = simple(out, in, "ROLLBACK");
        assertThat(parameterStatusesOf(rollback)).as("the rollback restored a reported parameter")
            .containsExactly(Map.entry("application_name", ""));
        assertThat(show(out, in, "work_mem")).isEqualTo("before");

        // A committed block keeps its SET.
        simple(out, in, "BEGIN");
        simple(out, in, "SET work_mem TO committed");
        simple(out, in, "COMMIT");
        assertThat(show(out, in, "work_mem")).isEqualTo("committed");

        // An aborted block: its SETs go with the ROLLBACK that ends it.
        simple(out, in, "BEGIN");
        simple(out, in, "SET work_mem TO aborted");
        assertThat(messageTypesOf(simple(out, in, "SELECT FROM NoSuchTypeIssue8242"))).contains('E');
        simple(out, in, "ROLLBACK");
        assertThat(show(out, in, "work_mem")).isEqualTo("committed");
      });
    }
  }

  // Regression: #8242
  @Test
  @DisplayName("[#8242] SET LOCAL lasts until the end of its transaction, committed or not")
  void setLocalEndsWithItsTransaction() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "BEGIN");
        simple(out, in, "SET LOCAL work_mem TO y");
        assertThat(show(out, in, "work_mem")).isEqualTo("y");
        simple(out, in, "COMMIT");
        assertThat(show(out, in, "work_mem")).as("SET LOCAL outlived its transaction").isEmpty();

        // Outside a block the statement is its own transaction, so a SET LOCAL has no lasting effect.
        simple(out, in, "SET LOCAL work_mem TO z");
        assertThat(show(out, in, "work_mem")).isEmpty();

        // A SET after a SET LOCAL of the same parameter supersedes it and survives the commit...
        simple(out, in, "BEGIN");
        simple(out, in, "SET LOCAL work_mem TO local");
        simple(out, in, "SET work_mem TO session");
        simple(out, in, "COMMIT");
        assertThat(show(out, in, "work_mem")).isEqualTo("session");

        // ...while a SET LOCAL after a SET masks it only until the end of the transaction.
        simple(out, in, "BEGIN");
        simple(out, in, "SET work_mem TO second");
        simple(out, in, "SET LOCAL work_mem TO local");
        assertThat(show(out, in, "work_mem")).isEqualTo("local");
        simple(out, in, "COMMIT");
        assertThat(show(out, in, "work_mem")).isEqualTo("second");

        // A reported parameter set LOCAL is reported when set and again when it ends.
        simple(out, in, "BEGIN");
        assertThat(parameterStatusesOf(simple(out, in, "SET LOCAL TimeZone TO 'Asia/Tokyo'"))).containsEntry("TimeZone", "Asia/Tokyo");
        assertThat(parameterStatusesOf(simple(out, in, "COMMIT"))).containsExactly(Map.entry("TimeZone", "UTC"));
      });
    }
  }

  // Regression: #8242
  @Test
  @DisplayName("[#8242] RESET <name> and RESET ALL restore the reset value, on both protocols")
  void resetIsRecognized() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "SET work_mem TO x");
        assertThat(messageTypesOf(simple(out, in, "RESET work_mem"))).doesNotContain('E');
        assertThat(show(out, in, "work_mem")).isEmpty();

        simple(out, in, "SET work_mem TO x");
        simple(out, in, "SET TIME ZONE 'Europe/Rome'");
        assertThat(show(out, in, "timezone")).isEqualTo("Europe/Rome");
        final List<WireMessage> resetAll = simple(out, in, "RESET ALL");
        assertThat(messageTypesOf(resetAll)).doesNotContain('E');
        assertThat(parameterStatusesOf(resetAll)).containsExactly(Map.entry("TimeZone", "UTC"));
        assertThat(show(out, in, "work_mem")).isEmpty();
        assertThat(show(out, in, "timezone")).isEqualTo("UTC");

        simple(out, in, "SET work_mem TO x");
        sendParse(out, "", "RESET work_mem");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('1', '2', 'C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("RESET");
        assertThat(show(out, in, "work_mem")).isEmpty();
      });
    }
  }

  // Regression: #8242
  @Test
  @DisplayName("[#8242] a SET in an extended-protocol pipeline that fails is discarded with it at the Sync")
  void setInAFailedPipelineIsDiscarded() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "", "SET work_mem TO pipelined");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendParse(out, "", "SELECT FROM NoSuchTypeIssue8242");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('E');
        assertThat(show(out, in, "work_mem")).isEmpty();

        sendParse(out, "", "SET work_mem TO pipelined");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(show(out, in, "work_mem")).isEqualTo("pipelined");
      });
    }
  }

  // Regression: #8244
  @Test
  @DisplayName("[#8244] Bind/Execute/Sync with no Describe is answered with no RowDescription")
  void executeWithoutDescribeSendsNoRowDescription() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE Probe8244 IF NOT EXISTS");
        simple(out, in, "INSERT INTO Probe8244 SET tag = 'a'");

        sendParse(out, "Sx", "SELECT tag FROM Probe8244");
        sendBind(out, "", "Sx");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("Describe('P') is answered with the RowDescription")
            .containsExactly('1', '2', 'T', 'D', 'C', 'Z');

        for (int i = 0; i < 2; i++) {
          sendBind(out, "", "Sx");
          sendExecute(out, "");
          sendSync(out);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("no Describe, so no RowDescription")
              .containsExactly('2', 'D', 'C', 'Z');
        }

        // A statement described once, by Describe('S'), is not described again either.
        sendParse(out, "Sy", "SELECT tag FROM Probe8244");
        sendDescribe(out, 'S', "Sy");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'T', 'Z');
        sendBind(out, "", "Sy");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'D', 'C', 'Z');
      });
    }
  }

  // Regression: #8244
  @ParameterizedTest(name = "prepareThreshold={0}")
  @ValueSource(ints = { 1, 5 })
  @DisplayName("[#8244] pgjdbc runs a row-returning PreparedStatement past its prepareThreshold")
  void pgjdbcRunsAPreparedStatementPastItsPrepareThreshold(final int prepareThreshold) throws Exception {
    try (final Connection connection = openJdbcConnection(prepareThreshold)) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Jdbc8244 IF NOT EXISTS");
      }

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Jdbc8244 SET tag = ?")) {
        for (int i = 0; i < 8; i++) {
          insert.setString(1, "t" + i);
          insert.execute();
        }
      }

      try (final PreparedStatement select = connection.prepareStatement("SELECT tag FROM Jdbc8244 WHERE tag = ?")) {
        for (int i = 0; i < 8; i++) {
          select.setString(1, "t" + i);
          try (final ResultSet rs = select.executeQuery()) {
            assertThat(rs.next()).as("execution %d", i + 1).isTrue();
            assertThat(rs.getString("tag")).isEqualTo("t" + i);
          }
        }
      }

      // SET through pgjdbc: the ParameterStatus it reads back must satisfy its own DateStyle check.
      try (final Statement statement = connection.createStatement()) {
        statement.execute("SET DateStyle = 'ISO, DMY'");
        try (final ResultSet rs = statement.executeQuery("SHOW DateStyle")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString(1)).isEqualTo("ISO, DMY");
        }
      }
    }
  }

  private static Map<String, String> parameterStatusesOf(final List<WireMessage> messages) {
    final Map<String, String> statuses = new LinkedHashMap<>();
    for (final WireMessage message : messages)
      if (message.type() == 'S') {
        final byte[] body = message.body();
        int end = 0;
        while (body[end] != 0)
          end++;
        final String name = new String(body, 0, end, StandardCharsets.UTF_8);
        final int start = end + 1;
        end = start;
        while (body[end] != 0)
          end++;
        statuses.put(name, new String(body, start, end - start, StandardCharsets.UTF_8));
      }
    return statuses;
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'C')
        return new String(message.body(), 0, message.body().length - 1, StandardCharsets.UTF_8);
    throw new AssertionError("no CommandComplete among " + messageTypesOf(messages));
  }

  private Connection openJdbcConnection(final int prepareThreshold) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    properties.setProperty("prepareThreshold", String.valueOf(prepareThreshold));
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /*
   * Issue #8392: a {@code SET} the server cannot parse, or cannot honour, was answered {@code CommandComplete SET} having
   * applied nothing, on both protocols. {@code SET SESSION AUTHORIZATION} and {@code SET ROLE} were the sharpest: a client
   * that dropped privileges was told it had, and kept running with the connected user's.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #8392
  @ParameterizedTest(name = "[#8392] simple protocol: {0} -> {1}")
  @CsvSource(delimiter = '|', value = {
      "SET x|42601",
      "SET SESSION AUTHORIZATION 'someone'|0A000",
      "SET ROLE readonly|0A000",
      "SET role = 'readonly'|0A000",
      "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE|0A000",
      "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY|0A000",
      "SET CONSTRAINTS fk_a DEFERRED|42704" })
  void simpleProtocolRefusesWithAnError(final String query, final String sqlState) throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, query);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("an ErrorResponse, never the tag for having executed it")
            .containsExactly('E', 'Z');
        assertThat(sqlStateOf(response)).isEqualTo(sqlState);
        assertThat(readyForQueryStatusOf(response)).as("autocommit: the connection is idle and usable").isEqualTo('I');

        assertThat(show(out, in, "role")).isEqualTo("none");
        assertThat(show(out, in, "session_authorization")).isEqualTo("root");
      });
    }
  }

  // Regression: #8392
  @ParameterizedTest(name = "[#8392] extended protocol: {0} is refused at Parse")
  @CsvSource(delimiter = '|', value = { "SET x", "SET CONSTRAINTS ALL", "SET TRANSACTION BOGUS" })
  void extendedProtocolRefusesAnUnparseableSetAtParse(final String query) throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "", query);
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("no ParseComplete, no CommandComplete: only the ErrorResponse")
            .containsExactly('E', 'Z');
        assertThat(sqlStateOf(response)).isEqualTo("42601");
      });
    }
  }

  // Regression: #8392
  @Test
  @DisplayName("[#8392] extended protocol: SET ROLE parses, and its Execute is refused")
  void extendedProtocolRefusesSetRoleAtExecute() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "", "SET ROLE readonly");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).contains('E').doesNotContain('C');
        assertThat(sqlStateOf(response)).isEqualTo("0A000");
        assertThat(show(out, in, "role")).isEqualTo("none");
      });
    }
  }

  // Regression: #8392
  @ParameterizedTest(name = "[#8392] accepted, since it is true: {0}")
  @CsvSource(delimiter = '|', value = {
      "SET CONSTRAINTS ALL DEFERRED",
      "SET ROLE NONE",
      "RESET ROLE",
      "SET SESSION AUTHORIZATION DEFAULT",
      "RESET SESSION AUTHORIZATION",
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
      "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
      "SET TRANSACTION READ WRITE",
      "SET NAMES 'UTF8'" })
  void truthfulSetsAreStillAnsweredSet(final String query) throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, query);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

        sendParse(out, "", query);
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'C', 'Z');
      });
    }
  }

  // Regression: #8392
  @Test
  @DisplayName("[#8392] a SET refused inside a transaction block aborts the block, as any other error does")
  void refusedSetAbortsTheBlock() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SET SESSION AUTHORIZATION 'someone'");
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(sqlStateOf(response)).isEqualTo("0A000");
        assertThat(readyForQueryStatusOf(response)).as("failed transaction block").isEqualTo('E');
        sendSimpleQuery(out, "ROLLBACK");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }
  }

  // Regression: #8392
  @Test
  @DisplayName("[#8392] pgjdbc: setTransactionIsolation works for the real level and is refused for any other")
  void jdbcTransactionIsolation() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);

      assertThatThrownBy(() -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE))
          .isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"));

      try (final Statement st = connection.createStatement()) {
        assertThatThrownBy(() -> st.execute("SET SESSION AUTHORIZATION 'someone'"))
            .isInstanceOf(SQLException.class)
            .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"));
        assertThatThrownBy(() -> st.execute("SET x"))
            .isInstanceOf(SQLException.class)
            .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("42601"));
        // THE CONNECTION IS STILL USABLE
        st.execute("SET application_name = 'issue8392'");
      }
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /*
   * Regression tests for two Postgres-wire issues:
   * <ul>
   *   <li>#8573: {@code SET}, {@code SHOW} and {@code RESET} of a parameter PostgreSQL does not know succeeded, inventing
   *   it, where PostgreSQL answers {@code 42704};</li>
   *   <li>#8569: {@code SHOW TRANSACTION ISOLATION LEVEL} was answered by a branch of its own, from the database default
   *   instead of the open transaction's level and under a column named {@code LEVEL}, so it contradicted
   *   {@code SHOW transaction_isolation}.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #8573
  @Test
  @DisplayName("[#8573] an unknown parameter is refused with 42704 by SET, SHOW and RESET, on both protocols")
  void unknownParameterIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String query : new String[] { "SET bogus_param = 1", "SHOW bogus_param", "SHOW nosuchparam", "RESET nosuchparam",
            "SET LOCAL bogus_param TO 2", "SHOW databases" }) {
          final List<WireMessage> answer = simple(out, in, query);
          assertThat(messageTypesOf(answer)).as(query).containsExactly('E', 'Z');
          assertThat(sqlStateOf(answer)).as(query).isEqualTo("42704");
        }

        // The extended protocol: SHOW is answered at Parse, SET/RESET at Execute
        for (final String query : new String[] { "SHOW bogus_param", "SET bogus_param = 1", "RESET bogus_param" }) {
          sendParse(out, "", query);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> answer = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(answer)).as(query).contains('E');
          assertThat(sqlStateOf(answer)).as(query).isEqualTo("42704");
        }

        // What still works: a parameter PostgreSQL knows, and a custom placeholder
        assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = 5000"))).doesNotContain('E');
        assertThat(showValue(out, in, "statement_timeout")).isEqualTo("5000");
        assertThat(messageTypesOf(simple(out, in, "SET myapp.tenant = 'acme'"))).doesNotContain('E');
        assertThat(showValue(out, in, "myapp.tenant")).isEqualTo("acme");
        assertThat(messageTypesOf(simple(out, in, "RESET myapp.never_set"))).doesNotContain('E');

        // SHOW answers under PostgreSQL's spelling of the name
        assertThat(firstColumnName(simple(out, in, "SHOW datestyle"))).isEqualTo("DateStyle");

        // SHOW ALL: one name/setting/description row per parameter
        final List<WireMessage> all = simple(out, in, "SHOW ALL");
        assertThat(messageTypesOf(all)).doesNotContain('E').contains('D');
        assertThat(firstColumnName(all)).isEqualTo("name");
      });
    }
  }

  // Regression: #8569
  @Test
  @DisplayName("[#8569] SHOW TRANSACTION ISOLATION LEVEL answers the open transaction's level, as SHOW transaction_isolation")
  void showTransactionIsolationLevelReadsTheOpenTransaction() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        final List<WireMessage> idle = simple(out, in, "SHOW TRANSACTION ISOLATION LEVEL");
        assertThat(firstColumnName(idle)).isEqualTo("transaction_isolation");
        assertThat(firstDataRowValue(idle)).isEqualTo("read committed");

        assertThat(messageTypesOf(simple(out, in, "BEGIN ISOLATION REPEATABLE_READ"))).doesNotContain('E');
        final List<WireMessage> inTx = simple(out, in, "SHOW TRANSACTION ISOLATION LEVEL");
        assertThat(firstColumnName(inTx)).isEqualTo("transaction_isolation");
        assertThat(firstDataRowValue(inTx)).isEqualTo("repeatable read").isEqualTo(showValue(out, in, "transaction_isolation"));

        // The extended protocol reaches the same value
        sendParse(out, "", "SHOW TRANSACTION   ISOLATION LEVEL");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("repeatable read");

        assertThat(messageTypesOf(simple(out, in, "ROLLBACK"))).doesNotContain('E');

        // A prepared SHOW reads the value at each Execute, not the one current when it was parsed
        sendParse(out, "SHOWISO", "SHOW transaction_isolation");
        sendSync(out);
        readUntilReadyForQuery(in);
        assertThat(messageTypesOf(simple(out, in, "BEGIN ISOLATION REPEATABLE_READ"))).doesNotContain('E');
        sendBind(out, "", "SHOWISO");
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("repeatable read");
        assertThat(messageTypesOf(simple(out, in, "ROLLBACK"))).doesNotContain('E');
        sendBind(out, "", "SHOWISO");
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("read committed");
        assertThat(firstDataRowValue(simple(out, in, "SHOW TRANSACTION ISOLATION LEVEL"))).isEqualTo("read committed");
      });
    }
  }

  // Regression: #8569
  @Test
  @DisplayName("[#8569] pgjdbc's getTransactionIsolation() reads the level the open transaction runs at")
  void jdbcGetTransactionIsolation() throws Exception {
    try (final Connection conn = getConnection()) {
      assertThat(conn.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
      try (final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery("SHOW TRANSACTION ISOLATION LEVEL")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("transaction_isolation")).isEqualTo("read committed");
      }
    }
  }

  private Connection getConnection() throws Exception {
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }

  private String showValue(final DataOutputStream out, final DataInputStream in, final String name) throws Exception {
    return firstDataRowValue(simple(out, in, "SHOW " + name));
  }

  /*
   * Regression tests for four Postgres-wire session-command issues:
   * <ul>
   *   <li>#9326: {@code SHOW} was answered with an empty CommandComplete tag, where PostgreSQL sends {@code SHOW};</li>
   *   <li>#9327: a refused {@code SET} was concealed and logged as a server fault in production mode;</li>
   *   <li>#9328: {@code DISCARD ALL}, {@code DEALLOCATE} and {@code CLOSE} reached the SQL grammar and failed to parse;</li>
   *   <li>#9329: {@code SET SCHEMA}/{@code SET search_path} was answered and read back by {@code SHOW} while
   *   {@code current_schema()} and the name resolution ignored it, and {@code statement_timeout} was stored while nothing
   *   cancelled a statement at that deadline.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  /**
   * Every row runs the subquery once more over the whole type, and each row binds a different {@code $parent.$current},
   * so the subquery cache cannot collapse them: the statement is quadratic in the row count, 9 million record visits here,
   * about a second uncancelled where the deadline is 1 ms - a thousand times over (#10166). A plain scan of 30,000 records
   * is linear and did not clear that bar: a fast runner finished it inside the millisecond, and the statement was
   * answered instead of cancelled.
   */
  private static final String SLOW_STATEMENT =
      "SELECT count(*) FROM Issue9329Doc WHERE (SELECT count(*) FROM Issue9329Doc d WHERE d.n % 7 = $parent.$current.n % 7) > 0";

  // Regression: #9326
  @Test
  @DisplayName("[#9326] SHOW is answered with the SHOW command tag on both protocols")
  void showCarriesItsCommandTag() throws Exception {
    run((out, in) -> {
      for (final String query : new String[] { "SHOW timezone", "SHOW ALL", "SHOW search_path" })
        assertThat(commandTag(simple(out, in, query))).as(query).isEqualTo("SHOW");

      sendParse(out, "", "SHOW timezone");
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      assertThat(commandTag(readUntilReadyForQuery(in))).isEqualTo("SHOW");
    });
  }

  // Regression: #9327
  @Test
  @DisplayName("[#9327] a refused SET keeps its message and is not an incident in production mode")
  void refusedSetIsNotConcealedInProduction() throws Exception {
    final String previous = getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, "production");
    try {
      run((out, in) -> {
        final Map<String, String> expected = Map.of( //
            "SET ROLE readonly", "0A000", //
            "SET x", "42601", //
            "SET bogus_param = 1", "42704", //
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", "0A000");
        for (final Map.Entry<String, String> entry : expected.entrySet()) {
          final List<WireMessage> answer = simple(out, in, entry.getKey());
          final Map<Character, String> error = errorFields(answer.stream().filter(m -> m.type() == 'E').findFirst().orElseThrow());
          assertThat(error.get('C')).as(entry.getKey()).isEqualTo(entry.getValue());
          assertThat(error.get('M')).as(entry.getKey()).isNotEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
        }
        assertThat(errorFields(simple(out, in, "SET ROLE readonly").get(0)).get('M')).contains("SET ROLE is not supported");
      });
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
    }
  }

  // Regression: #9328
  @Test
  @DisplayName("[#9328] DISCARD ALL resets the session and drops its prepared statements, on both protocols")
  void discardAll() throws Exception {
    run((out, in) -> {
      assertThat(simple(out, in, "SET myapp.tenant = 'acme'")).extracting(WireMessage::type).doesNotContain('E');
      sendParse(out, "keep", "SELECT 1");
      sendSync(out);
      readUntilReadyForQuery(in);

      final List<WireMessage> discard = simple(out, in, "DISCARD ALL");
      assertThat(messageTypesOf(discard)).containsExactly('C', 'Z');
      assertThat(commandTag(discard)).isEqualTo("DISCARD ALL");

      assertThat(show(out, in, "myapp.tenant")).isEmpty();

      sendBind(out, "", "keep");
      sendExecute(out, "");
      sendSync(out);
      assertThat(sqlStateOf(readUntilReadyForQuery(in))).isEqualTo("26000");

      // Extended protocol
      sendParse(out, "", "DISCARD ALL");
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> extended = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(extended)).doesNotContain('E');
      assertThat(commandTag(extended)).isEqualTo("DISCARD ALL");
    });
  }

  // Regression: #9328
  @Test
  @DisplayName("[#9328] the narrower DISCARD forms are accepted and carry their own tag")
  void narrowDiscardForms() throws Exception {
    run((out, in) -> {
      for (final String form : new String[] { "PLANS", "SEQUENCES", "TEMP", "TEMPORARY" }) {
        final List<WireMessage> answer = simple(out, in, "DISCARD " + form);
        assertThat(messageTypesOf(answer)).as(form).containsExactly('C', 'Z');
        assertThat(commandTag(answer)).as(form).isEqualTo("DISCARD " + ("TEMPORARY".equals(form) ? "TEMP" : form));
      }
      assertThat(sqlStateOf(simple(out, in, "DISCARD NONSENSE"))).isEqualTo("42601");
    });
  }

  // Regression: #9328
  @Test
  @DisplayName("[#9328] DISCARD ALL is refused inside a transaction block, as in PostgreSQL")
  void discardAllInsideTransaction() throws Exception {
    run((out, in) -> {
      simple(out, in, "BEGIN");
      assertThat(sqlStateOf(simple(out, in, "DISCARD ALL"))).isEqualTo("25001");
      simple(out, in, "ROLLBACK");
      assertThat(messageTypesOf(simple(out, in, "DISCARD ALL"))).doesNotContain('E');
    });
  }

  // Regression: #9328
  @Test
  @DisplayName("[#9328] DEALLOCATE and CLOSE drop what they name")
  void deallocateAndClose() throws Exception {
    run((out, in) -> {
      sendParse(out, "s1", "SELECT 1");
      sendParse(out, "s2", "SELECT 2");
      sendSync(out);
      readUntilReadyForQuery(in);

      assertThat(commandTag(simple(out, in, "DEALLOCATE s1"))).isEqualTo("DEALLOCATE");
      assertThat(sqlStateOf(simple(out, in, "DEALLOCATE s1"))).isEqualTo("26000");
      assertThat(commandTag(simple(out, in, "DEALLOCATE PREPARE s2"))).isEqualTo("DEALLOCATE");
      assertThat(commandTag(simple(out, in, "DEALLOCATE ALL"))).isEqualTo("DEALLOCATE ALL");

      // A quoted identifier keeps its spaces and its case
      sendParse(out, "My Stmt", "SELECT 4");
      sendSync(out);
      readUntilReadyForQuery(in);
      assertThat(commandTag(simple(out, in, "DEALLOCATE \"My Stmt\""))).isEqualTo("DEALLOCATE");

      assertThat(sqlStateOf(simple(out, in, "CLOSE nosuch"))).isEqualTo("34000");
      assertThat(commandTag(simple(out, in, "CLOSE ALL"))).isEqualTo("CLOSE CURSOR ALL");

      // Extended protocol: DEALLOCATE ALL empties every prepared statement
      sendParse(out, "s3", "SELECT 3");
      sendSync(out);
      readUntilReadyForQuery(in);
      sendParse(out, "", "DEALLOCATE ALL");
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> answer = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answer)).doesNotContain('E');
      assertThat(commandTag(answer)).isEqualTo("DEALLOCATE ALL");
      sendBind(out, "", "s3");
      sendExecute(out, "");
      sendSync(out);
      assertThat(sqlStateOf(readUntilReadyForQuery(in))).isEqualTo("26000");

      // The protocol-level Close message is still answered
      sendClose(out, 'S', "nothing");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('3', 'Z');
    });
  }

  // Regression: #9329
  @Test
  @DisplayName("[#9329] SET SCHEMA / SET search_path does not contradict current_schema()")
  void searchPathAnswersTheRealSchema() throws Exception {
    run((out, in) -> {
      final String schema = firstDataRowValue(simple(out, in, "SELECT current_schema()"));
      assertThat(schema).isEqualTo(getDatabaseName());
      assertThat(show(out, in, "search_path")).isEqualTo(schema);

      assertThat(messageTypesOf(simple(out, in, "SET SCHEMA 'other'"))).doesNotContain('E');
      assertThat(show(out, in, "search_path")).isEqualTo(schema);
      assertThat(messageTypesOf(simple(out, in, "SET search_path TO nosuchschema"))).doesNotContain('E');
      assertThat(show(out, in, "search_path")).isEqualTo(schema);
      assertThat(firstDataRowValue(simple(out, in, "SELECT current_schema()"))).isEqualTo(schema);
    });
  }

  // Regression: #9329
  @Test
  @DisplayName("[#9329] statement_timeout cancels a statement at the deadline, and 0 disables it")
  void statementTimeoutIsHonoured() throws Exception {
    run((out, in) -> {
      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = 0"))).doesNotContain('E');
      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = '30s'"))).doesNotContain('E');
      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = DEFAULT"))).doesNotContain('E');
      assertThat(sqlStateOf(simple(out, in, "SET statement_timeout = 'forever'"))).isEqualTo("22023");

      final Database database = getServer(0).getDatabaseForOperator(getDatabaseName());
      database.getSchema().getOrCreateDocumentType("Issue9329Doc");
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          database.newDocument("Issue9329Doc").set("n", i).save();
      });

      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = 1"))).doesNotContain('E');
      assertThat(sqlStateOf(simple(out, in, SLOW_STATEMENT))).isEqualTo("57014");

      // The same statement is a valid one, so the cancellation above was the deadline's doing and nothing else's
      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = 0"))).doesNotContain('E');
      assertThat(messageTypesOf(simple(out, in, SLOW_STATEMENT))).doesNotContain('E');
    });
  }

  @Test
  @DisplayName("the database command timeout bounds a statement of a session that set no statement_timeout")
  void commandTimeoutOfTheDatabaseBoundsTheStatement() throws Exception {
    run((out, in) -> {
      final Database database = getServer(0).getDatabaseForOperator(getDatabaseName());
      database.getSchema().getOrCreateDocumentType("Issue9329Doc");
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          database.newDocument("Issue9329Doc").set("n", i).save();
      });

      assertThat(show(out, in, "statement_timeout")).isEqualTo("0");
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);
      try {
        assertThat(sqlStateOf(simple(out, in, SLOW_STATEMENT))).isEqualTo("57014");
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
      }

      // The same statement is a valid one, so the cancellation above was the setting's doing and nothing else's
      assertThat(messageTypesOf(simple(out, in, SLOW_STATEMENT))).doesNotContain('E');
    });
  }

  // Regression: #9329
  @Test
  @DisplayName("[#9329] statement_timeout is no longer stored and read back unhonoured")
  void statementTimeoutReadBack() throws Exception {
    run((out, in) -> {
      assertThat(show(out, in, "statement_timeout")).isEqualTo("0");
      simple(out, in, "SET statement_timeout = '45s'");
      assertThat(show(out, in, "statement_timeout")).isEqualTo("45s");
      simple(out, in, "RESET statement_timeout");
      assertThat(show(out, in, "statement_timeout")).isEqualTo("0");
    });
  }

  private interface WireScript {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void run(final WireScript script) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in);
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');
      assertTimeoutPreemptively(Duration.ofSeconds(60), () -> script.run(out, in));
    }
  }

  private static List<WireMessage> simple(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
    sendSimpleQuery(out, query);
    return readUntilReadyForQuery(in);
  }

  /**
   * The tag of the last CommandComplete among {@code messages}.
   */
  private static String commandTag(final List<WireMessage> messages) {
    final WireMessage complete = messages.stream().filter(m -> m.type() == 'C').reduce((first, second) -> second).orElseThrow();
    final byte[] body = complete.body();
    return new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
  }

  /*
   * Issue #11389: pgjdbc binds a {@code java.sql.Date} as untyped text with a zone suffix ({@code '2024-05-06 +01'}). An INSERT
   * into a DATE property failed and a predicate on it silently matched nothing.
   */
  // Regression: #11389
  @Test
  void setDateInsertsAndMatchesADeclaredDateProperty() throws Exception {
    final TimeZone previous = TimeZone.getDefault();
    // a zone ahead of UTC makes the driver render a non-trivial suffix; the day must survive whatever it is
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Lisbon"));
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE D11389 IF NOT EXISTS");
        statement.execute("CREATE PROPERTY D11389.id IF NOT EXISTS INTEGER");
        statement.execute("CREATE PROPERTY D11389.d IF NOT EXISTS DATE");
      }

      final Date day = Date.valueOf(LocalDate.of(2024, 5, 6));
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO D11389 (id, d) VALUES (?, ?)")) {
        insert.setInt(1, 1);
        insert.setDate(2, day);
        insert.execute();
      }

      for (final String[] predicate : new String[][] { { "d = ?", "1" }, { "d >= ?", "1" }, { "d <= ?", "1" }, { "d < ?", "0" },
          { "d > ?", "0" } }) {
        try (final PreparedStatement select = connection.prepareStatement("SELECT id FROM D11389 WHERE " + predicate[0])) {
          select.setDate(1, day);
          try (final ResultSet rs = select.executeQuery()) {
            int count = 0;
            while (rs.next())
              count++;
            assertThat(count).as(predicate[0]).isEqualTo(Integer.parseInt(predicate[1]));
          }
        }
      }
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  /*
   * #8391: {@code SHOW TIME ZONE}, the read-back of {@code SET TIME ZONE}, answered an empty string while
   * {@code SHOW timezone} answered the value, and {@code RESET TIME ZONE} was refused as malformed.
   */
  // Regression: #8391
  @Test
  @DisplayName("[#8391] SHOW TIME ZONE and SHOW timezone read the same parameter, and RESET TIME ZONE resets it")
  void showTimeZoneReadsTheSameParameterAsShowTimezone() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(show(out, in, "TIME ZONE")).isEqualTo(show(out, in, "timezone")).isEqualTo("UTC");

        assertThat(messageTypesOf(simple(out, in, "SET TIME ZONE 'Europe/Rome'"))).doesNotContain('E');
        assertThat(show(out, in, "timezone")).isEqualTo("Europe/Rome");
        assertThat(show(out, in, "TIME ZONE")).isEqualTo("Europe/Rome");
        assertThat(show(out, in, "time   zone")).isEqualTo("Europe/Rome");

        final List<WireMessage> reset = simple(out, in, "RESET TIME ZONE");
        assertThat(messageTypesOf(reset)).as("RESET TIME ZONE is PostgreSQL syntax, not a malformed RESET").doesNotContain('E');
        assertThat(show(out, in, "TIME ZONE")).isEqualTo("UTC");
        assertThat(show(out, in, "timezone")).isEqualTo("UTC");

        // The extended protocol takes its own SHOW arm, so it is checked too.
        sendParse(out, "", "SHOW TIME ZONE");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("UTC");
      });
    }
  }
}
