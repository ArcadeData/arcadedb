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
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #8708: a portal whose answer is materialized at Parse (SHOW, and a catalog query whose filters are literals)
 * skipped the Execute row-limit slice, so a fetch-size cursor over SHOW ALL got the whole result and a
 * {@code CommandComplete} instead of {@code PortalSuspended}, and a second Execute re-sent everything.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8708ParseAnsweredPortalRowLimitIT extends PostgresWireProtocolTestBase {
  private static final int LIMIT = 3;

  @Test
  @DisplayName("[#8708] SHOW ALL honours the Execute row limit and resumes where it stopped")
  void showAllIsSliced() throws Exception {
    assertSliced("SHOW ALL");
  }

  @Test
  @DisplayName("[#8708] a catalog query with literal filters honours the Execute row limit")
  void literalCatalogQueryIsSliced() throws Exception {
    assertSliced("SELECT typname FROM pg_catalog.pg_type");
  }

  private void assertSliced(final String query) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, query);
        final int total = count(readUntilReadyForQuery(in), 'D');
        assertThat(total).as("the query must return more rows than the limit").isGreaterThan(LIMIT);

        // A portal lives in a transaction, so the cursor needs an explicit block
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);

        sendParse(out, "", query);
        sendBind(out, "P1", "");
        sendExecute(out, "P1", LIMIT);
        sendSync(out);
        final List<WireMessage> first = readUntilReadyForQuery(in);
        assertThat(count(first, 'D')).as("first slice").isEqualTo(LIMIT);
        assertThat(messageTypesOf(first)).as("suspended, not complete").contains('s').doesNotContain('C');

        int received = LIMIT;
        while (true) {
          sendExecute(out, "P1", LIMIT);
          sendSync(out);
          final List<WireMessage> next = readUntilReadyForQuery(in);
          final int rows = count(next, 'D');
          assertThat(rows).isLessThanOrEqualTo(LIMIT);
          received += rows;
          if (messageTypesOf(next).contains('C'))
            break;
          assertThat(messageTypesOf(next)).contains('s');
        }
        assertThat(received).as("every row exactly once").isEqualTo(total);

        // A drained portal answers an empty slice, it does not re-send the answer
        sendExecute(out, "P1", LIMIT);
        sendSync(out);
        assertThat(count(readUntilReadyForQuery(in), 'D')).isZero();
      });
    }
  }

  private static int count(final List<WireMessage> messages, final char type) {
    return (int) messageTypesOf(messages).stream().filter(t -> t == type).count();
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }
}
