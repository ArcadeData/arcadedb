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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9007: pgjdbc asks for the binary format of an array column once a PreparedStatement is prepared on
 * the server (the 5th execution by default), and the plugin answered with the text literal, so every execution from the 6th on
 * failed with BufferUnderflowException.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9007ArrayBinaryFormatIT extends PostgresWireProtocolTestBase {

  @Test
  void aListColumnSurvivesServerPreparedExecutions() throws Exception {
    try (final Connection connection = connect(new Properties())) {
      createType();
      try (final PreparedStatement ps = connection.prepareStatement("SELECT lst, txt FROM T9007 WHERE id = ?")) {
        for (int execution = 1; execution <= 8; execution++) {
          ps.setInt(1, 1);
          try (final ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat((Object[]) rs.getArray(1).getArray()).as("execution %d", execution).containsExactly(1, 2, 3);
            final Array text = rs.getArray(2);
            assertThat((Object[]) text.getArray()).as("execution %d", execution).containsExactly("a", "b\"c", "d,e");
          }
        }
      }
    }
  }

  @Test
  void binaryResultsFromTheFirstExecution() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("prepareThreshold", "-1");
    try (final Connection connection = connect(properties)) {
      createType();
      try (final PreparedStatement ps = connection.prepareStatement("SELECT lst FROM T9007 WHERE id = ?")) {
        ps.setInt(1, 1);
        try (final ResultSet rs = ps.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat((Object[]) rs.getArray(1).getArray()).containsExactly(1, 2, 3);
        }
      }
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
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("T9007");
      type.getOrCreateProperty("id", Type.INTEGER);
      type.getOrCreateProperty("lst", Type.LIST);
      type.getOrCreateProperty("txt", Type.LIST);
      database.newDocument("T9007").set("id", 1).set("lst", List.of(1, 2, 3)).set("txt", List.of("a", "b\"c", "d,e")).save();
    });
  }
}
