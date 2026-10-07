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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendClose;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
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
class Issue9326Issue9327Issue9328Issue9329SessionCommandsIT extends PostgresWireProtocolTestBase {

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

  @Test
  @DisplayName("[#9329] statement_timeout cancels a statement at the deadline, and 0 disables it")
  void statementTimeoutIsHonoured() throws Exception {
    run((out, in) -> {
      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = 0"))).doesNotContain('E');
      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = '30s'"))).doesNotContain('E');
      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = DEFAULT"))).doesNotContain('E');
      assertThat(sqlStateOf(simple(out, in, "SET statement_timeout = 'forever'"))).isEqualTo("22023");

      final Database database = getServer(0).getDatabase(getDatabaseName());
      database.getSchema().getOrCreateDocumentType("Issue9329Doc");
      database.transaction(() -> {
        for (int i = 0; i < 30_000; i++)
          database.newDocument("Issue9329Doc").set("n", i).save();
      });

      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = 1"))).doesNotContain('E');
      // Not answerable from an index, so the scan has to run, and no machine scans 30,000 records in a millisecond
      final List<WireMessage> answer = simple(out, in, "SELECT count(*) FROM Issue9329Doc WHERE n % 7 = 3");
      assertThat(sqlStateOf(answer)).isEqualTo("57014");

      assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = 0"))).doesNotContain('E');
      assertThat(messageTypesOf(simple(out, in, "SELECT count(*) FROM Issue9329Doc WHERE n % 7 = 3"))).doesNotContain('E');
    });
  }

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

  private static String show(final DataOutputStream out, final DataInputStream in, final String name) throws Exception {
    return firstDataRowValue(simple(out, in, "SHOW " + name));
  }

  /**
   * The tag of the last CommandComplete among {@code messages}.
   */
  private static String commandTag(final List<WireMessage> messages) {
    final WireMessage complete = messages.stream().filter(m -> m.type() == 'C').reduce((first, second) -> second).orElseThrow();
    final byte[] body = complete.body();
    return new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
  }
}
