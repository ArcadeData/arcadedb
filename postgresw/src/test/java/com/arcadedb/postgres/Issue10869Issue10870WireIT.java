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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #10869 and #10870 through the PostgreSQL JDBC driver:
 * <ul>
 *   <li>a catalog query that matches no row announces the columns it projected, typed as when it matches (#10869);</li>
 *   <li>an array parameter sent in text format keeps its empty-string and NULL elements, and an untyped JSON document is not read
 *   as a list (#10870).</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10869Issue10870WireIT extends PostgresWireProtocolTestBase {

  @Test
  void aTypeLookupThatFindsNothingStillHasItsColumns() throws Exception {
    try (final Connection connection = connect(new Properties()); final Statement statement = connection.createStatement()) {
      for (final String filter : List.of("typname = 'nosuchtype'", "oid = 999999", "typname = 'int4'")) {
        try (final java.sql.ResultSet rs = statement.executeQuery("SELECT oid, typname, typlen FROM pg_type WHERE " + filter)) {
          final ResultSetMetaData metaData = rs.getMetaData();
          assertThat(metaData.getColumnCount()).as(filter).isEqualTo(3);
          assertThat(metaData.getColumnTypeName(1)).as(filter).isEqualTo("int4");
          assertThat(metaData.getColumnTypeName(2)).as(filter).isEqualTo("varchar");
          assertThat(metaData.getColumnTypeName(3)).as(filter).isEqualTo("int4");
          assertThat(rs.findColumn("typname")).isEqualTo(2);
        }
      }
    }
  }

  @Test
  void anEmptyRelationAnswerIsTypedLikeAFullOne() throws Exception {
    createType();
    try (final Connection connection = connect(new Properties()); final Statement statement = connection.createStatement()) {
      for (final String relname : List.of("T10870", "NoSuchRelation"))
        try (final java.sql.ResultSet rs = statement.executeQuery(
            "SELECT oid, relnatts, relhasindex, relkind FROM pg_class WHERE relname = '" + relname + "'")) {
          final ResultSetMetaData metaData = rs.getMetaData();
          assertThat(metaData.getColumnTypeName(1)).as(relname).isEqualTo("int4");
          assertThat(metaData.getColumnTypeName(2)).as(relname).isEqualTo("int4");
          assertThat(metaData.getColumnTypeName(3)).as(relname).isEqualTo("bool");
          assertThat(metaData.getColumnTypeName(4)).as(relname).isEqualTo("varchar");
        }
    }
  }

  @Test
  void aTextFormatArrayParameterKeepsEveryElement() throws Exception {
    createType();
    final Properties properties = new Properties();
    // text format for every parameter, which is what a client that does not negotiate binary sends
    properties.setProperty("binaryTransfer", "false");
    try (final Connection connection = connect(properties)) {
      try (final PreparedStatement ps = connection.prepareStatement("INSERT INTO T10870 SET id = ?, tags = ?")) {
        ps.setInt(1, 1);
        ps.setArray(2, connection.createArrayOf("text", new String[] { "a", "", "b", null, "NULL" }));
        ps.execute();

        // an untyped parameter is a list only when it is an array literal
        ps.setInt(1, 2);
        ps.setObject(2, "{\"a\",\"\",NULL}", Types.OTHER);
        ps.execute();

        ps.setInt(1, 3);
        ps.setObject(2, "{\"k\": 1}", Types.OTHER);
        ps.execute();
      }
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(tagsOf(database, 1)).containsExactly("a", "", "b", null, "NULL");
    assertThat(tagsOf(database, 2)).containsExactly("a", "", null);
    try (final ResultSet rs = database.query("sql", "SELECT tags FROM T10870 WHERE id = 3")) {
      assertThat(rs.next().<Object>getProperty("tags")).isEqualTo("{\"k\": 1}");
    }
  }

  private static List<Object> tagsOf(final Database database, final int id) {
    try (final ResultSet rs = database.query("sql", "SELECT tags FROM T10870 WHERE id = ?", id)) {
      return rs.next().getProperty("tags");
    }
  }

  private Connection connect(final Properties properties) throws Exception {
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  private void createType() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> database.getSchema().getOrCreateDocumentType("T10870").getOrCreateProperty("id", Type.INTEGER));
  }
}
