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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #10922: a named prepared catalog query with no bound parameters kept answering the schema as it stood at Parse,
 * so a type created after Parse was missing and a type dropped after Parse was still listed.
 */
class Issue10922PreparedCatalogQueryIT extends PostgresWireProtocolTestBase {
  private static final String QUERY = "SELECT relname FROM pg_catalog.pg_class";
  private static final String TYPE  = "Zzz10922New";

  @Test
  @DisplayName("[#10922] a named prepared catalog statement sees a type created and dropped after Parse")
  void namedStatementTracksSchemaChanges() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in);
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "S1", QUERY);
        sendBind(out, "P1", "S1");
        sendExecute(out, "P1");
        sendSync(out);
        assertThat(listsType(readUntilReadyForQuery(in))).isFalse();

        sendSimpleQuery(out, "CREATE VERTEX TYPE " + TYPE);
        readUntilReadyForQuery(in);

        // Bind + Execute of the already-parsed statement, no re-Parse
        sendBind(out, "P2", "S1");
        sendExecute(out, "P2");
        sendSync(out);
        assertThat(listsType(readUntilReadyForQuery(in))).as("type created after Parse").isTrue();

        sendSimpleQuery(out, "DROP TYPE " + TYPE);
        readUntilReadyForQuery(in);

        sendBind(out, "P3", "S1");
        sendExecute(out, "P3");
        sendSync(out);
        assertThat(listsType(readUntilReadyForQuery(in))).as("type dropped after Parse").isFalse();
      });
    }
  }

  @Test
  @DisplayName("[#10922] a partially fetched catalog portal keeps slicing its own answer across a schema change")
  void partiallyFetchedPortalKeepsItsAnswer() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in);
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendParse(out, "S2", QUERY);
        sendBind(out, "P1", "S2");
        sendExecute(out, "P1", 2);
        sendSync(out);
        final List<WireMessage> first = readUntilReadyForQuery(in);
        assertThat(first.stream().filter(m -> m.type() == 'D').count()).isEqualTo(2);

        sendSimpleQuery(out, "CREATE VERTEX TYPE " + TYPE + "B");
        readUntilReadyForQuery(in);

        // The resumed cursor continues the answer it started on: it neither restarts nor re-reads
        sendExecute(out, "P1", 0);
        sendSync(out);
        final List<WireMessage> rest = readUntilReadyForQuery(in);
        assertThat(rest.stream().filter(m -> m.type() == 'D').count()).isGreaterThan(0);
        assertThat(listsType(rest)).isFalse();

        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "DROP TYPE " + TYPE + "B");
        readUntilReadyForQuery(in);
      });
    }
  }

  private static boolean listsType(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'D' && new String(message.body(), StandardCharsets.UTF_8).contains(TYPE))
        return true;
    return false;
  }
}
