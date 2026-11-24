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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11956: a {@code date} parameter (OID 1082) was decoded to a {@link java.util.Date} at UTC midnight, a datetime to the
 * engine. SQL coerced it against a DATE property, but an openCypher equality on a DATE property never matched it, and an
 * undeclared property stored it as a DATETIME. It is now the {@link LocalDate} a Bolt date decodes to.
 */
class Issue11956PostgresDateParameterIT extends PostgresWireProtocolTestBase {
  private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

  @BeforeEach
  void createData() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("D11956");
      type.createProperty("k", Type.INTEGER);
      type.createProperty("da", Type.DATE);
      type.createProperty("dt", Type.DATETIME);
    });
    database.command("sql", "INSERT INTO D11956 SET k = 1, da = '2024-02-29', dt = '2024-02-29 13:14:15'");
  }

  /**
   * A named statement with the date in text format. (pgjdbc's server-prepared binary mode describes the statement before it
   * binds, which a {cypher} statement does not answer; the binary DATE decoding is covered by {@code PostgresTypeTest}.)
   */
  @Test
  void cypherEqualityOnADatePropertyMatchesADateParameter() throws Exception {
    try (final Connection connection = connect("0")) {
      assertThat(keys(connection, "{cypher}MATCH (n:D11956) WHERE n.da = ? RETURN n.k AS k")).containsExactly(1);
    }
  }

  /** arcadedb.dateImplementation=java.util.Date reads a DATE back as a java.util.Date: the date parameter still matches it (issue #9647). */
  @Test
  void cypherEqualityOnADatePropertyMatchesADateParameterUnderTheJavaUtilDateImplementation() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'").close();
    // the setting took effect: otherwise the test silently runs the default LocalDate path
    try (final ResultSet rs = database.query("sql", "SELECT da FROM D11956 WHERE k = 1")) {
      assertThat(rs.next().<Object>getProperty("da")).isInstanceOf(Date.class);
    }

    try (final Connection connection = connect("0")) {
      assertThat(keys(connection, "{cypher}MATCH (n:D11956) WHERE n.da = ? RETURN n.k AS k")).containsExactly(1);
      assertThat(keys(connection, "SELECT k FROM D11956 WHERE da = ?")).containsExactly(1);
    }
  }

  /** The SQL forms kept working, against a DATE and against a DATETIME property: -1 also sends the date in binary. */
  @ParameterizedTest
  @ValueSource(strings = { "0", "-1" })
  void sqlComparisonsWithADateParameterAreUnchanged(final String prepareThreshold) throws Exception {
    try (final Connection connection = connect(prepareThreshold)) {
      assertThat(keys(connection, "SELECT k FROM D11956 WHERE da = ?")).containsExactly(1);
      assertThat(keys(connection, "SELECT k FROM D11956 WHERE dt >= ?")).containsExactly(1);
      assertThat(keys(connection, "SELECT k FROM D11956 WHERE dt < ?")).isEmpty();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "0", "-1" })
  void anUndeclaredPropertyStoresADateParameterAsADate(final String prepareThreshold) throws Exception {
    try (final Connection connection = connect(prepareThreshold);
        final PreparedStatement insert = connection.prepareStatement("INSERT INTO D11956 SET k = 2, u = ?")) {
      insert.setObject(1, DAY);
      insert.execute();
    }
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT u, u.type() AS t FROM D11956 WHERE k = 2")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("t")).isEqualTo("DATE");
      assertThat(row.<Object>getProperty("u")).isEqualTo(DAY);
    }
  }

  private static List<Integer> keys(final Connection connection, final String query) throws Exception {
    final List<Integer> keys = new ArrayList<>();
    try (final PreparedStatement statement = connection.prepareStatement(query)) {
      statement.setObject(1, DAY);
      try (final java.sql.ResultSet rs = statement.executeQuery()) {
        while (rs.next())
          keys.add(rs.getInt(1));
      }
    }
    return keys;
  }

  private Connection connect(final String prepareThreshold) throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    properties.setProperty("prepareThreshold", prepareThreshold);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
