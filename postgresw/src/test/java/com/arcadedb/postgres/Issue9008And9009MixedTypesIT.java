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
  void aNumericListRoundTripsOverTheWire() throws Exception {
    // default settings only: an array column described before its first execution is read as binary by pgjdbc on
    // the later executions whatever the format announced, an array limitation that predates this change
    for (final String options : new String[] { "" }) {
      try (final Connection connection = openJdbcConnection(options)) {
        final Database database = getServerDatabase(0, getDatabaseName());
        database.transaction(() -> {
          if (!database.getSchema().existsType("Numeric9008")) {
            database.getSchema().createDocumentType("Numeric9008");
            database.newDocument("Numeric9008").set("id", 1)
                .set("lst", java.util.List.of(new java.math.BigDecimal("1.25"), 2, new java.math.BigDecimal("10000000000"))).save();
          }
        });
        try (final PreparedStatement select = connection.prepareStatement("SELECT lst FROM Numeric9008 WHERE id = ?")) {
          for (int i = 0; i < 3; i++) {
            select.setInt(1, 1);
            try (final ResultSet resultSet = select.executeQuery()) {
              assertThat(resultSet.next()).isTrue();
              assertThat(resultSet.getMetaData().getColumnTypeName(1)).isIn("_numeric", "_text");
              final String text = resultSet.getString(1);
              assertThat(text).as("options=" + options).isNotNull();
              final String[] values = text.replaceAll("[{}\"]", "").split(",");
              assertThat(values).hasSize(3);
              assertThat(new java.math.BigDecimal(values[0]).compareTo(new java.math.BigDecimal("1.25"))).isZero();
              assertThat(new java.math.BigDecimal(values[2]).compareTo(new java.math.BigDecimal("10000000000"))).isZero();
            }
          }
        }
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

  @Test
  void aProjectionOfAnUndeclaredPropertyKeepsOneLayoutToo() throws Exception {
    for (final String options : new String[] { "", "prepareThreshold=-1" }) {
      try (final Connection connection = openJdbcConnection(options)) {
        final Database database = getServerDatabase(0, getDatabaseName());
        database.transaction(() -> {
          if (!database.getSchema().existsType("Projected9009")) {
            database.getSchema().createDocumentType("Projected9009").createProperty("id", Type.INTEGER);
            database.newDocument("Projected9009").set("id", 1).set("u", 1).save();
            database.newDocument("Projected9009").set("id", 2).set("u", 3000000000L).save();
            database.newDocument("Projected9009").set("id", 3).set("u", 1.5).save();
          }
        });
        final int[] ids = options.isEmpty() ? new int[] { 1, 1, 1, 1, 1, 2, 3, 1 } : new int[] { 1, 2, 3 };
        try (final PreparedStatement select = connection.prepareStatement("SELECT u FROM Projected9009 WHERE id = ?")) {
          for (final int id : ids) {
            select.setInt(1, id);
            try (final ResultSet resultSet = select.executeQuery()) {
              assertThat(resultSet.next()).isTrue();
              assertThat(resultSet.getString("u")).as("u of id " + id + " with " + options)
                  .isEqualTo(switch (id) { case 1 -> "1"; case 2 -> "3000000000"; default -> "1.5"; });
            }
          }
        }
      }
    }
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
            assertThat(resultSet.getMetaData().getColumnTypeName(1)).as("a declared property keeps its native type").isEqualTo("int4");
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
