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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.DateUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #8135: on the extended query protocol, {@code SET} ran {@code setConfiguration()}
 * at Parse and only marked the portal {@code ignoreExecution}, so the side effect happened when the statement was
 * PREPARED rather than when it was EXECUTED - the rule {@code BEGIN}/{@code COMMIT}/{@code ROLLBACK} were moved to
 * by #6543/#7905. Two consequences:
 * <ul>
 *   <li>a {@code SET} that is only prepared (Parse, or Parse+Bind, then Sync) still changed the session;</li>
 *   <li>a cached {@code SET} bound and executed again with no Parse in between - what a pgjdbc/psycopg3 statement
 *   cache sends once the statement is promoted - answered {@code CommandComplete SET} and re-applied nothing.</li>
 * </ul>
 * The only {@code SET} whose effect is visible from outside the connection is {@code datestyle = ISO}, which writes
 * the schema's date-time format, so every test observes that on the server-side database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8135SetAppliedAtExecuteIT extends PostgresWireProtocolTestBase {

  private static final String SET_ISO             = "SET datestyle = 'ISO'";
  private static final String RESTRICTED_USER     = "issue8135noSettings";
  private static final String RESTRICTED_PASSWORD = "issue8135noSettingsPwd";
  private static final String RESTRICTED_GROUP    = "issue8135NoSettings";

  @AfterEach
  @Override
  public void endTest() {
    resetDateTimeFormat();
    super.endTest();
  }

  @Test
  @DisplayName("[#8135] a SET that is only prepared - Parse, then Parse+Bind, each followed by Sync - does not change the session")
  void preparedButNotExecutedSetHasNoEffect() throws Exception {
    resetDateTimeFormat();

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // Parse and Sync: what a driver sends to prepare a statement ahead of using it.
        sendParse(out, "s", SET_ISO);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the Parse is accepted").containsExactly('1', 'Z');
        assertThat(currentDateTimeFormat()).as("preparing a SET must not apply it").isNotEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);

        // An explicit block keeps portal "p" alive across the Syncs below: in autocommit a Sync ends the implicit
        // transaction and the portal with it, as in PostgreSQL (issue #8212).
        sendSimpleQuery(out, "BEGIN");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

        // Bound and described but still never executed.
        sendBind(out, "p", "s");
        sendDescribe(out, 'P', "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(currentDateTimeFormat()).as("binding and describing a SET must not apply it either")
            .isNotEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);

        // The Execute is what applies it: this is what proves the two assertions above could have failed.
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> executed = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(executed)).containsExactly('C', 'Z');
        assertThat(currentDateTimeFormat()).as("executing the SET applies it").isEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);

        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#8135] a cached SET bound and executed again without a new Parse re-applies the setting")
  void reExecutedCachedSetIsReapplied() throws Exception {
    resetDateTimeFormat();

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "s", SET_ISO);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(currentDateTimeFormat()).isEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);

        // Something else changes the value the SET established.
        resetDateTimeFormat();

        // The statement cache re-runs the prepared SET to restore it: Bind+Execute of the same name, no Parse.
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> reuse = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(reuse)).as("the re-executed SET answers CommandComplete").containsExactly('2', 'C', 'Z');
        assertThat(currentDateTimeFormat()).as("a re-executed cached SET must apply the setting again, not only claim to")
            .isEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);
      });
    }
  }

  @Test
  @DisplayName("[#8135] re-running the same already-executed SET portal without a new Bind applies it only once")
  void replayingTheBoundSetPortalAppliesOnlyOnce() throws Exception {
    resetDateTimeFormat();

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // An explicit block keeps portal "p" alive across the Sync: in autocommit a Sync ends the implicit
        // transaction and the portal with it, as in PostgreSQL (issue #8212).
        sendSimpleQuery(out, "BEGIN");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

        sendParse(out, "s", SET_ISO);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(currentDateTimeFormat()).isEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);

        resetDateTimeFormat();

        // No Bind: the same boundary applyTransactionControl() draws (#7851) - the marker belongs to the bound
        // portal and is consumed by its first Execute; only a new Bind of the statement carries it again.
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
        assertThat(currentDateTimeFormat()).as("a replayed portal applies its SET once")
            .isNotEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);

        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#8135] a SET whose Execute is discarded by an earlier error in the same pipeline does not take effect")
  void setDiscardedBySkipUntilSyncHasNoEffect() throws Exception {
    resetDateTimeFormat();

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // The SET is parsed BEFORE the failing statement, so its Parse succeeds; its Bind and Execute come after
        // the error and are discarded up to the Sync, as PostgreSQL discards them.
        sendParse(out, "s", SET_ISO);
        sendParse(out, "bad", "SELEC 1");
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> pipeline = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(pipeline)).as("the malformed statement fails the pipeline").contains('E');
        assertThat(messageTypesOf(pipeline)).as("the discarded Execute answers nothing").doesNotContain('C');
        assertThat(currentDateTimeFormat()).as("a SET the client never got to execute must not have been applied")
            .isNotEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);
      });
    }
  }

  @Test
  @DisplayName("[#8135] a SET refused at Execute is refused again when the same portal is re-executed, not silently skipped")
  void refusedSetIsRefusedAgainOnReplay() throws Exception {
    resetDateTimeFormat();

    // A user whose group grants no database-level access, so SET datestyle's LocalSchema.setDateTimeFormat() -
    // which checks UPDATE_DATABASE_SETTINGS - throws at Execute.
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
        sendParse(out, "s", SET_ISO);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the SET is refused at Execute").contains('E').doesNotContain('C');

        // Recovered by the Sync, the client retries the SAME bound portal - no new Bind. The refusal must be
        // repeated: a marker consumed by the failed attempt would answer "CommandComplete SET" having applied nothing.
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the retried SET is refused again, not reported as applied")
            .contains('E').doesNotContain('C');
      });
    } finally {
      security.dropUser(RESTRICTED_USER);
      security.deleteGroup(getDatabaseName(), RESTRICTED_GROUP);
    }

    assertThat(currentDateTimeFormat()).as("a refused SET changes nothing").isNotEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  private String currentDateTimeFormat() {
    return getServerDatabase(0, getDatabaseName()).getSchema().getDateTimeFormat();
  }

  private void resetDateTimeFormat() {
    getServerDatabase(0, getDatabaseName()).getSchema().setDateTimeFormat(GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString());
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    authenticate(out, in, "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in, final String user, final String password)
      throws Exception {
    sendStartupMessage(out, user, getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, password);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }
}
