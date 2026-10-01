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
 * A {@code Describe('S')} must name the columns of a statement that returns rows whenever they can be named, instead of
 * promising {@code NoData} and then having the Execute refused with {@code 0A000} (issue #8562): a write with a
 * {@code RETURN} clause, and a catalog query whose filters are bound parameters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8562DescribeStatementNamesReturnedColumnsIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#8562] INSERT ... RETURN and UPDATE ... RETURN AFTER are described with a RowDescription and executed with their rows")
  void writeWithReturnIsDescribedAndExecuted() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE Article8562 IF NOT EXISTS");
        simple(out, in, "CREATE PROPERTY Article8562.id IF NOT EXISTS INTEGER");

        final String[][] statements = {
            { "S1", "INSERT INTO Article8562 SET id = 1 RETURN @rid" },
            { "S2", "UPDATE Article8562 SET id = 9 RETURN AFTER" },
            { "S3", "INSERT INTO Article8562 SET id = 3 RETURN id" },
            { "S4", "UPDATE Article8562 SET id = 9 RETURN AFTER @rid" },
            { "S5", "DELETE FROM Article8562 RETURN BEFORE WHERE id >= 0" },
            { "S6", "INSERT INTO Article8562 SET id = (SELECT max(id) FROM Article8562) RETURN id" } };

        for (final String[] statement : statements) {
          sendParse(out, statement[0], statement[1]);
          sendDescribe(out, 'S', statement[0]);
          sendSync(out);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as(statement[1]).containsExactly('1', 't', 'T', 'Z');

          sendBind(out, "", statement[0]);
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> execute = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(execute)).as(statement[1]).contains('D').doesNotContain('E');
        }
      });
    }
  }

  @Test
  @DisplayName("[#8562] a pg_type lookup with a bound parameter is described with its columns and executed")
  void catalogQueryWithBoundParameterIsDescribedAndExecuted() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "C1", "SELECT typname AS name, oid, typarray AS array_oid, oid::regtype::text AS regtype, typdelim AS delimiter "
            + "FROM pg_type t WHERE t.oid = to_regtype($1) ORDER BY t.oid");
        sendDescribe(out, 'S', "C1");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'T', 'Z');

        sendBind(out, "", "C1", "int4");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> execute = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(execute)).contains('D').doesNotContain('E');
      });
    }
  }

  private List<WireMessage> simple(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
    sendSimpleQuery(out, query);
    return readUntilReadyForQuery(in);
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }
}
