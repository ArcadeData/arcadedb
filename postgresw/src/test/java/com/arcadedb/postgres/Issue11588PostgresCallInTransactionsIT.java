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

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #11588 over the Postgres extended protocol: CALL { ... } IN TRANSACTIONS keeps running in batches in the implicit
 * transaction block {@code beginImplicitTransactionBlock} opens around a pipeline, and is refused once a client BEGIN has
 * joined that block and made it the client's, whose ROLLBACK could not undo the batches.
 */
class Issue11588PostgresCallInTransactionsIT extends PostgresWireProtocolTestBase {
  private static final String STATEMENT = "{cypher} UNWIND range(1, 25) AS i CALL { WITH i CREATE (:%s {i: i}) } IN TRANSACTIONS OF 10 ROWS";

  @Test
  void runsInBatchesInTheImplicitBlockOfAPipeline() throws Exception {
    final String label = "PgImplicit11588";
    withConnection((out, in) -> {
      parseBindExecute(out, "c", STATEMENT.formatted(label));
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as(errorMessage(answers)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
    });

    assertThat(count(label)).isEqualTo(25);
  }

  @Test
  void refusedOnceAClientBeginJoinsTheImplicitBlock() throws Exception {
    final String label = "PgJoined11588";
    final String type = "PgJoinedDoc11588";
    withConnection((out, in) -> {
      sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS");
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

      // The write opens the implicit block, the BEGIN joins it and declares it the client's
      parseBindExecute(out, "w", "INSERT INTO " + type + " SET id = 1");
      parseBindExecute(out, "b", "BEGIN");
      parseBindExecute(out, "c", STATEMENT.formatted(label));
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsOnlyOnce('E');
      assertThat(errorMessage(answers)).contains("can only be executed in an implicit transaction");

      sendSimpleQuery(out, "ROLLBACK");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
    });

    assertThat(count(label)).isZero();
    assertThat(getServerDatabase(0, getDatabaseName()).countType(type, false)).isZero();
  }

  private interface WireExchange {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void withConnection(final WireExchange exchange) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> exchange.run(out, in));
    }
  }

  private static void parseBindExecute(final DataOutputStream out, final String name, final String query) throws Exception {
    sendParse(out, name, query);
    sendBind(out, name, name);
    sendExecute(out, name);
  }

  private static String errorMessage(final List<WireMessage> answers) {
    for (final WireMessage message : answers)
      if (message.type() == 'E')
        return errorFields(message).get('M');
    return "";
  }

  private long count(final String label) {
    return getServerDatabase(0, getDatabaseName()).query("opencypher", "MATCH (x:" + label + ") RETURN count(x) AS c").next()
        .<Number>getProperty("c").longValue();
  }
}
