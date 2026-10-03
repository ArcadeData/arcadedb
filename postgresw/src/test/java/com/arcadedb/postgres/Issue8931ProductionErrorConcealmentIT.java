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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8931: the PostgreSQL wire protocol answered every failure with the raw exception message
 * in the {@code M} field of the ErrorResponse, so in production mode a duplicated-key failure handed any authenticated
 * database user the stored key value, which the HTTP and gRPC surfaces replace with a placeholder. The SQLSTATE in
 * {@code C} stays, because it is the bounded part a driver acts on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8931ProductionErrorConcealmentIT extends PostgresWireProtocolTestBase {

  private static final String TYPE   = "Issue8931Unique";
  private static final String SECRET = "already-there-8931";

  @Test
  void simpleQueryDuplicatedKeyIsConcealedInProductionMode() throws Exception {
    final Map<Character, String> error = withMode("production", (out, in) -> {
      prepare(out, in);
      sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET k = '" + SECRET + "'");
      return failureOf(readUntilReadyForQuery(in));
    });

    assertThat(error.get('C')).isEqualTo("23505");
    assertThat(error.get('M')).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void extendedProtocolDuplicatedKeyIsConcealedInProductionMode() throws Exception {
    final Map<Character, String> error = withMode("production", (out, in) -> {
      prepare(out, in);
      sendParse(out, "", "INSERT INTO " + TYPE + " SET k = '" + SECRET + "'");
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      return failureOf(readUntilReadyForQuery(in));
    });

    assertThat(error.get('C')).isEqualTo("23505");
    assertThat(error.get('M')).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void failedCommitIsConcealedInProductionMode() throws Exception {
    final Map<Character, String> error = withMode("production", (out, in) -> {
      prepare(out, in);
      sendSimpleQuery(out, "BEGIN");
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET k = '" + SECRET + "'");
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "COMMIT");
      return failureOf(readUntilReadyForQuery(in));
    });

    assertThat(error.get('C')).isEqualTo("23505");
    assertThat(error.get('M')).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void syntaxErrorKeepsItsSqlStateButNotItsText() throws Exception {
    final Map<Character, String> error = withMode("production", (out, in) -> {
      prepare(out, in);
      sendSimpleQuery(out, "SELEC FROM WHERE");
      return failureOf(readUntilReadyForQuery(in));
    });

    assertThat(error.get('C')).isEqualTo("42601");
    assertThat(error.get('M')).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void developmentModeKeepsTheFullMessage() throws Exception {
    final Map<Character, String> error = withMode("development", (out, in) -> {
      prepare(out, in);
      sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET k = '" + SECRET + "'");
      return failureOf(readUntilReadyForQuery(in));
    });

    assertThat(error.get('C')).isEqualTo("23505");
    assertThat(error.get('M')).contains(SECRET);
  }

  private interface WireScript {
    Map<Character, String> run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private Map<Character, String> withMode(final String mode, final WireScript script) throws Exception {
    final String previous = getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, mode);
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in);
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');
      return script.run(out, in);
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
    }
  }

  private static void prepare(final DataOutputStream out, final DataInputStream in) throws Exception {
    for (final String statement : new String[] { "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS",
        "CREATE PROPERTY " + TYPE + ".k IF NOT EXISTS STRING", "CREATE INDEX IF NOT EXISTS ON " + TYPE + " (k) UNIQUE",
        "DELETE FROM " + TYPE, "INSERT INTO " + TYPE + " SET k = '" + SECRET + "'" }) {
      sendSimpleQuery(out, statement);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).as(statement).doesNotContain('E');
    }
  }

  private static Map<Character, String> failureOf(final List<WireMessage> response) {
    return errorFields(response.stream().filter(m -> m.type() == 'E').findFirst().orElseThrow());
  }
}
