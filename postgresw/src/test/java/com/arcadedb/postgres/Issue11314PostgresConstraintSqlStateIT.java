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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParseWithTypes;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #11314, end to end: the refusals of the issue's own repro reach pgjdbc with the SQLSTATE PostgreSQL answers the same
 * refusal with, on both query modes, where they all came back {@code 22023} and an unknown function {@code XX000}. The controls
 * the issue recorded as already right keep their codes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314PostgresConstraintSqlStateIT extends PostgresWireProtocolTestBase {

  @ParameterizedTest
  @ValueSource(strings = { "extended", "simple" })
  @DisplayName("[#11314] a violated constraint, a refused conversion and an unknown function answer PostgreSQL's SQLSTATEs")
  void everyRefusalAnswersPostgresSqlState(final String queryMode) throws Exception {
    try (final Connection conn = connect(queryMode); final Statement st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE Ty IF NOT EXISTS");
      st.execute("CREATE PROPERTY Ty.i IF NOT EXISTS INTEGER (mandatory true, min 1, max 10)");
      st.execute("CREATE PROPERTY Ty.nn IF NOT EXISTS STRING (notnull true)");
      st.execute("CREATE PROPERTY Ty.rx IF NOT EXISTS STRING (regexp '[a-z]+')");
      st.execute("CREATE PROPERTY Ty.mx IF NOT EXISTS STRING (max 3)");
      st.execute("CREATE PROPERTY Ty.d IF NOT EXISTS DATE");
      st.execute("CREATE INDEX IF NOT EXISTS ON Ty (nn) UNIQUE");

      final String[][] cases = {
          { "INSERT INTO Ty SET i = 1, nn = null", "23502" },              // not_null_violation
          { "INSERT INTO Ty SET nn = 'm'", "23502" },                     // MANDATORY: not_null_violation
          { "INSERT INTO Ty SET i = 11, nn = 'a'", "23514" },             // check_violation
          { "INSERT INTO Ty SET i = 1, nn = 'b', rx = 'ABC'", "23514" },  // check_violation
          { "INSERT INTO Ty SET i = 1, nn = 'c', mx = 'abcdef'", "22001" }, // string_data_right_truncation
          { "INSERT INTO Ty SET i = 'abc', nn = 'd'", "22P02" },          // invalid_text_representation
          { "INSERT INTO Ty SET i = 99999999999, nn = 'e'", "22003" },    // numeric_value_out_of_range
          { "INSERT INTO Ty SET i = 1, nn = 'f', d = 'notadate'", "22007" }, // invalid_datetime_format
          { "SELECT foo(1)", "42883" },                                   // undefined_function
          { "SELECT now()", "42883" },                                    // undefined_function
          // the controls the issue recorded as already right
          { "SELECT 1 / 0", "22012" },                                    // division_by_zero
          { "SELECT FROM NoSuchType", "42P01" } };                        // undefined_table

      for (final String[] c : cases) {
        final SQLException refused = catchThrowableOfType(SQLException.class, () -> st.execute(c[0]));
        assertThat((Throwable) refused).as("%s [%s] was not refused", c[0], queryMode).isNotNull();
        assertThat(refused.getSQLState()).as("%s [%s]: %s", c[0], queryMode, refused.getMessage()).isEqualTo(c[1]);
      }

      st.execute("INSERT INTO Ty SET i = 1, nn = 'dup'");
      final SQLException duplicate = catchThrowableOfType(SQLException.class, () -> st.execute("INSERT INTO Ty SET i = 2, nn = 'dup'"));
      assertThat(duplicate.getSQLState()).isEqualTo("23505"); // unique_violation
    }
  }

  /**
   * A bound parameter declared DATE or TIMESTAMP whose text is no date is the client's value, answered {@code 22007}
   * invalid_datetime_format at the Bind, as the same text written into the statement is. Its parse failure reached the wire raw
   * and was answered {@code XX000} internal_error.
   */
  @Test
  @DisplayName("[#11314] a DATE or TIMESTAMP parameter whose text is no date is refused with 22007 at the Bind")
  void anUnparsableTemporalParameterIsInvalidDatetimeFormat() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final int typeOid : new int[] { 1082, 1114 }) { // date, timestamp
          sendParseWithTypes(out, "", "SELECT ? AS d", typeOid);
          sendBind(out, "", "", "notadate");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> answer = readUntilReadyForQuery(in);
          assertThat(sqlStateOf(answer)).as("type oid %s", typeOid).isEqualTo("22007");
        }
      });
    }
  }

  private Connection connect(final String queryMode) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    props.setProperty("preferQueryMode", queryMode);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }
}
