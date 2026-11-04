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
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10927: a column declared LIST OF BINARY was announced as {@code _text} when the matched rows held empty lists, as
 * {@code json} when they held populated ones, as {@code varchar} when they held both, and as {@code _text} by a statement
 * Describe whatever the rows held, so the same row travelled as {@code [[1,2,3]]} or {@code {"[1,2,3]"}} depending on the
 * protocol. A BINARY is a {@code bytea}, so a list of them is a {@code bytea[]}, announced from the declaration alone, and
 * the client reads back the bytes stored.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10927ListOfBinaryColumnTypeIT extends PostgresWireProtocolTestBase {
  private static final String[] FILTERS = { "WHERE n = 1", "WHERE n = 2", "WHERE n = 3", "", "WHERE n = 99" };

  @BeforeEach
  void createData() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("BinList");
      type.createProperty("b", Type.LIST, "BINARY");
      type.createProperty("n", Type.INTEGER);
    });
    database.transaction(() -> {
      database.newVertex("BinList").set("n", 1).set("b", new ArrayList<>()).save();
      database.newVertex("BinList").set("n", 2).set("b", List.of(new byte[] { 1, 2, 3 })).save();
      database.newVertex("BinList").set("n", 3).set("b", List.of(new byte[] { 1, 2, 3 }, new byte[] { (byte) 0xff, 0 })).save();
    });
  }

  @Test
  void simpleQueryAnnouncesByteaArrayWhateverTheRowsHold() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("preferQueryMode", "simple");
    assertEveryShapeIsByteaArray(properties);
  }

  @Test
  void extendedQueryAnnouncesByteaArrayWhateverTheRowsHold() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("prepareThreshold", "0");
    assertEveryShapeIsByteaArray(properties);
  }

  /** A server-prepared statement: Describe('S') types the columns from the schema and the values come in binary. */
  @Test
  void serverPreparedBinaryQueryAnnouncesByteaArrayWhateverTheRowsHold() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("prepareThreshold", "-1");
    assertEveryShapeIsByteaArray(properties);
  }

  private void assertEveryShapeIsByteaArray(final Properties properties) throws Exception {
    try (final Connection connection = connect(properties)) {
      for (final String filter : FILTERS) {
        final String sql = "SELECT n, b FROM BinList " + filter + " ORDER BY n";
        try (final Statement statement = connection.createStatement(); final java.sql.ResultSet rs = statement.executeQuery(sql)) {
          assertRows(sql, rs, filter);
        }
        try (final PreparedStatement ps = connection.prepareStatement(sql)) {
          // twice: pgjdbc switches to a named statement on a later execution
          for (int i = 0; i < 2; i++)
            try (final java.sql.ResultSet rs = ps.executeQuery()) {
              assertRows(sql, rs, filter);
            }
        }
      }
    }
  }

  private static void assertRows(final String sql, final java.sql.ResultSet rs, final String filter) throws Exception {
    assertThat(rs.getMetaData().getColumnTypeName(2)).as(sql).isEqualTo("_bytea");
    while (rs.next()) {
      final int n = rs.getInt(1);
      final Array array = rs.getArray(2);
      final Object[] elements = (Object[]) array.getArray();
      switch (n) {
      case 1 -> assertThat(elements).as(sql).isEmpty();
      case 2 -> {
        assertThat(elements).as(sql).hasSize(1);
        assertThat((byte[]) elements[0]).as(sql).containsExactly(1, 2, 3);
      }
      case 3 -> {
        assertThat(elements).as(sql).hasSize(2);
        assertThat((byte[]) elements[0]).as(sql).containsExactly(1, 2, 3);
        assertThat((byte[]) elements[1]).as(sql).containsExactly((byte) 0xff, 0);
      }
      default -> throw new AssertionError("Unexpected row " + n + " for " + filter);
      }
    }
  }

  private Connection connect(final Properties properties) throws Exception {
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
