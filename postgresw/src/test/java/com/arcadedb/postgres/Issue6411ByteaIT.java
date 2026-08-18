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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6411: a BINARY property was announced as {@code varchar} when the OID came
 * from the schema (empty result set) and as {@code "char"[]} when it came from a sampled row, so a client
 * that prepared against an empty result and then re-executed got two different types for one column. Both
 * answers were wrong anyway - PostgreSQL's type for a blob is {@code bytea} (OID 17), and decoding arbitrary
 * bytes as text loses data for any non-UTF-8 payload.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6411ByteaIT extends PostgresWireProtocolTestBase {

  /** Every byte value, so that anything the text encoding mangles shows up. */
  private static final byte[] PAYLOAD = allByteValues();

  @Test
  void aBinaryPropertyRoundTripsThroughSetBytesAndGetBytes() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createBlobType(connection);

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Blob6411 SET id = 1, payload = ?")) {
        insert.setBytes(1, PAYLOAD);
        insert.execute();
      }

      try (final PreparedStatement select = connection.prepareStatement("SELECT payload FROM Blob6411 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBytes("payload")).isEqualTo(PAYLOAD);
      }
    }
  }

  @Test
  void theColumnIsAnnouncedAsByteaWhetherOrNotARowWasSampled() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createBlobType(connection);

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Blob6411 SET id = 1, payload = ?")) {
        insert.setBytes(1, PAYLOAD);
        insert.execute();
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement("SELECT payload FROM Blob6411 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
      }

      final String typeFromSchema;
      try (final PreparedStatement select = connection.prepareStatement("SELECT payload FROM Blob6411 WHERE id = 2");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isFalse();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        typeFromSchema = metaData.getColumnTypeName(1);
      }

      assertThat(typeFromRow).isEqualTo("bytea");
      assertThat(typeFromSchema)
          .as("the column's OID must not depend on whether the result set happens to be empty")
          .isEqualTo(typeFromRow);
    }
  }

  @Test
  void anEmptyBlobRoundTrips() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createBlobType(connection);

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Blob6411 SET id = 3, payload = ?")) {
        insert.setBytes(1, new byte[0]);
        insert.execute();
      }

      try (final PreparedStatement select = connection.prepareStatement("SELECT payload FROM Blob6411 WHERE id = 3");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBytes("payload")).isEmpty();
      }
    }
  }

  private void createBlobType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Blob6411 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Blob6411.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Blob6411.payload IF NOT EXISTS BINARY");
    }
  }

  private static byte[] allByteValues() {
    final byte[] bytes = new byte[256];
    for (int i = 0; i < bytes.length; i++)
      bytes[i] = (byte) i;
    return bytes;
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + getDatabaseName(), properties);
  }
}
