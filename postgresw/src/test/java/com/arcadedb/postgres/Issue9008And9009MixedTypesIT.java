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

import com.arcadedb.database.Database;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #9008 and #9009: a value must never be announced under a type that cannot hold it.
 * <ul>
 *   <li>#9008: a LIST column was typed from its first element, so {@code [1, 2.5]} was announced as {@code int4[]}
 *   and read back as {@code [1, 2]}, and {@code [1, 3000000000]} and {@code [1, "a"]} could not be read at all.</li>
 *   <li>#9009: a prepared SELECT on a type with undeclared properties described its columns from one row and then
 *   serialized every row in that row's own layout, so pgjdbc read 3000000000 as 0 or -1294967296, 1.5 as a bit pattern
 *   or 1, and {@code @rid} in the column of another property.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9008And9009MixedTypesIT extends PostgresWireProtocolTestBase {

  @Test
  void aMixedListIsAnnouncedUnderAnArrayTypeThatHoldsEveryElement() throws Exception {
    try (final Connection connection = openJdbcConnection(""); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE List9008");
      statement.execute("INSERT INTO List9008 SET id = 1, lst = [1, 2.5]");
      statement.execute("INSERT INTO List9008 SET id = 2, lst = [1, 3000000000]");
      statement.execute("INSERT INTO List9008 SET id = 3, lst = [1, 'a']");
      statement.execute("INSERT INTO List9008 SET id = 4, lst = [1, 2, 3]");

      assertThat(listOf(statement, 1)).containsExactly("1.0", "2.5");
      assertThat(listOf(statement, 2)).containsExactly("1", "3000000000");
      assertThat(listOf(statement, 3)).containsExactly("1", "a");
      assertThat(listOf(statement, 4)).containsExactly("1", "2", "3");

      try (final ResultSet resultSet = statement.executeQuery("SELECT lst FROM List9008 WHERE id = 4")) {
        resultSet.next();
        assertThat(resultSet.getMetaData().getColumnTypeName(1)).as("a list of integers keeps the narrow type").isEqualTo("_int4");
      }
    }
  }

  @Test
  void aPreparedStatementKeepsOneLayoutForRowsOfDifferentTypes() throws Exception {
    // default pgjdbc settings: executions 1-4 are unnamed, the 5th is described and prepared on the server, 6+ reuse it
    verifyRows("", new int[] { 1, 1, 1, 1, 1, 2, 3, 1 });
  }

  @Test
  void aStatementDescribedBeforeItsFirstExecutionKeepsOneLayoutToo() throws Exception {
    verifyRows("prepareThreshold=-1", new int[] { 1, 2, 3 });
  }

  private void verifyRows(final String options, final int[] ids) throws Exception {
    try (final Connection connection = openJdbcConnection(options); final Statement statement = connection.createStatement()) {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> {
        database.getSchema().createDocumentType("Schemaless9009").createProperty("id", Type.INTEGER);
        database.newDocument("Schemaless9009").set("id", 1).set("u", 1).set("a", "x").save();
        database.newDocument("Schemaless9009").set("id", 2).set("u", 3000000000L).save();
        database.newDocument("Schemaless9009").set("id", 3).set("u", 1.5).save();
      });

      try (final PreparedStatement select = connection.prepareStatement("SELECT FROM Schemaless9009 WHERE id = ?")) {
        for (final int id : ids) {
          select.setInt(1, id);
          try (final ResultSet resultSet = select.executeQuery()) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt("id")).isEqualTo(id);
            final String expectedU = switch (id) {
              case 1 -> "1";
              case 2 -> "3000000000";
              default -> "1.5";
            };
            assertThat(resultSet.getString("u")).as("u of id " + id).isEqualTo(expectedU);
            assertThat(resultSet.getString("a")).as("a of id " + id).isEqualTo(id == 1 ? "x" : null);
            assertThat(resultSet.getString("@rid")).as("@rid of id " + id).startsWith("#");
            assertThat(resultSet.getString("@type")).isEqualTo("Schemaless9009");
            assertThat(resultSet.getString("@cat")).isEqualTo("d");
          }
        }
      }
    }
  }

  private static String[] listOf(final Statement statement, final int id) throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SELECT lst FROM List9008 WHERE id = " + id)) {
      resultSet.next();
      final Object[] values = (Object[]) resultSet.getArray(1).getArray();
      return Arrays.stream(values).map(String::valueOf).toArray(String[]::new);
    }
  }

  private Connection openJdbcConnection(final String options) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl() + (options.isEmpty() ? "" : "?" + options), properties);
  }
}
