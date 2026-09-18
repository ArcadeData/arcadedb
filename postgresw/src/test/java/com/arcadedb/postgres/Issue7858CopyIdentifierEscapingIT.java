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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7858: the table form of {@code COPY} splices its type and column names into a
 * generated {@code SELECT} between back-ticks, and guarded that splice by REFUSING the back-tick, on the stated
 * ground that ArcadeDB's SQL cannot escape inside an identifier. It can - that is what issue #7740 established
 * and what {@code Identifier.quote()} does - so the guard refused a name that has a correct rendering, while the
 * OTHER character that carries meaning inside a back-tick quoted identifier, the backslash, was spliced raw.
 * <p>
 * A type named {@code a\b} therefore produced ``SELECT FROM `a\b` ``, whose identifier unescapes to {@code ab}:
 * the statement read a type the server knows under a name the client never asked for, or failed "type not
 * found", while a plain {@code SELECT} through the same wire returned the rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7858CopyIdentifierEscapingIT extends PostgresWireProtocolTestBase {

  /** The name of the type the COPY must actually read: it holds the character the splice used to consume. */
  private static final String ESCAPED_TYPE = "copy\\7858";
  /** The name the raw splice used to collapse to, which exists and holds DIFFERENT rows - so a misread is visible. */
  private static final String COLLAPSED_TYPE = "copy7858";

  @BeforeEach
  void populate() {
    final Database database = getServerDatabase(0, getDatabaseName());

    final DocumentType escaped = database.getSchema().createDocumentType(ESCAPED_TYPE);
    escaped.createProperty("id", Type.INTEGER);
    escaped.createProperty("na\\me", Type.STRING);

    final DocumentType collapsed = database.getSchema().createDocumentType(COLLAPSED_TYPE);
    collapsed.createProperty("id", Type.INTEGER);

    database.transaction(() -> {
      database.newDocument(ESCAPED_TYPE).set("id", 1, "na\\me", "escaped").save();
      database.newDocument(COLLAPSED_TYPE).set("id", 99).save();
    });
  }

  @Test
  @DisplayName("[#7858] a COPY of a type whose name holds a backslash reads that type, not the name the splice collapsed to")
  void aBackslashInATypeOrColumnNameIsEscapedRatherThanConsumed() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      // Whole type: the rewriter turns the client's double quotes into back-ticks, and the COPY builder must
      // escape the backslash inside them or the identifier names `copy7858` instead.
      assertThat(copyOut(connection, "COPY \"" + ESCAPED_TYPE + "\" (id) TO STDOUT"))
          .as("the backslash used to be consumed as an escape, so this read the other type")
          .isEqualTo("1\n");

      // And the column list, which is spliced by the same builder.
      assertThat(copyOut(connection, "COPY \"" + ESCAPED_TYPE + "\" (id, \"na\\me\") TO STDOUT"))
          .isEqualTo("1\tescaped\n");

      // The type the misread used to land on is still reachable under its own name, which is what makes the
      // assertion above a real distinction rather than a coincidence.
      assertThat(copyOut(connection, "COPY \"" + COLLAPSED_TYPE + "\" (id) TO STDOUT")).isEqualTo("99\n");
    }
  }

  private static String copyOut(final Connection connection, final String copy) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    copyManager(connection).copyOut(copy, bytes);
    return bytes.toString(StandardCharsets.UTF_8);
  }

  private static CopyManager copyManager(final Connection connection) throws Exception {
    return connection.unwrap(PGConnection.class).getCopyAPI();
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    properties.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + getDatabaseName(), properties);
  }
}
