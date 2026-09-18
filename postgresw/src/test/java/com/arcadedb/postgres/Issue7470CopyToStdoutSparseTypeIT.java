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
import org.postgresql.util.PSQLException;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7470: {@code COPY ... TO STDOUT} fixed its column set from the one-row sampler in
 * {@code getColumnsFromType}, so a property absent from that single row was absent from the export entirely -
 * for every row - while a plain {@code SELECT} over the same type returned it. An export that silently drops
 * columns produces a dataset that looks complete and is not, and the loss is only found downstream.
 * <p>
 * Two halves to the fix, and one test each:
 * <ul>
 *   <li>a property the type DECLARES is part of its shape whether or not the sampled row carries it, so the
 *       sample is widened with the schema instead of being trusted alone - which is the common case and keeps
 *       the export streaming;</li>
 *   <li>a genuinely schemaless property cannot be predicted from any sample, so a row carrying one the stream
 *       never announced is REFUSED rather than written without it, with a message that names the column and the
 *       way to export it.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7470CopyToStdoutSparseTypeIT extends PostgresWireProtocolTestBase {

  private static final String SPARSE   = "copysparse7470";
  private static final String DECLARED = "copydeclared7470";
  private static final String BULK     = "copybulk7470";
  /** Comfortably past the 64KB the COPY writer buffers before it flushes a batch of rows to the socket. */
  private static final int    BULK_ROWS    = 200;
  private static final int    BULK_PAYLOAD = 1024;

  @BeforeEach
  void populate() {
    final Database database = getServerDatabase(0, getDatabaseName());

    // Schemaless: no property is declared, and the two rows carry different ones. Whichever of them the sampler
    // reads, the other carries a column the sample could not have predicted - so the test does not depend on
    // the order the two records come back in.
    database.getSchema().createDocumentType(SPARSE);

    // Declared: every column is in the schema, but the first row leaves two of them unset. The sampler alone
    // would announce only "id".
    final DocumentType declared = database.getSchema().createDocumentType(DECLARED);
    declared.createProperty("id", Type.INTEGER);
    declared.createProperty("name", Type.STRING);
    declared.createProperty("email", Type.STRING);

    database.transaction(() -> {
      database.newDocument(SPARSE).set("id", 1, "alpha", "a").save();
      database.newDocument(SPARSE).set("id", 2, "beta", "b").save();

      database.newDocument(DECLARED).set("id", 1).save();
      database.newDocument(DECLARED).set("id", 2, "name", "second", "email", "second@example.com").save();
    });

    // One bucket, so the scan order is the insertion order and the one-row sampler is guaranteed to read the
    // first row rather than the odd one out at the end. Every row but the last carries only the declared
    // columns; the last adds a schemaless one, long after the writer has flushed its first batch to the socket.
    final DocumentType bulk = database.getSchema().createDocumentType(BULK, 1);
    bulk.createProperty("id", Type.INTEGER);
    bulk.createProperty("payload", Type.STRING);
    final String payload = "x".repeat(BULK_PAYLOAD);
    database.transaction(() -> {
      for (int i = 0; i < BULK_ROWS; i++)
        database.newDocument(BULK).set("id", i, "payload", payload).save();
      database.newDocument(BULK).set("id", BULK_ROWS, "payload", payload, "extra", "the column nobody announced").save();
    });
  }

  @Test
  @DisplayName("[#7470] a refusal after the stream has already flushed rows ends the copy with an ErrorResponse")
  void aRefusalPartwayThroughAFlushedStreamIsReportedToTheClient() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final ByteArrayOutputStream received = new ByteArrayOutputStream();
      assertThatThrownBy(() -> connection.unwrap(PGConnection.class).getCopyAPI()
          .copyOut("COPY (SELECT FROM " + BULK + " ORDER BY id) TO STDOUT", received))
          .isInstanceOf(PSQLException.class)
          .hasMessageContaining("\"extra\"")
          .hasMessageContaining("which is not among the columns the stream announced");

      // The point of this case: the failure lands after real CopyData has gone out, so the client sees an
      // ErrorResponse where CopyDone would have been - which is how PostgreSQL reports a COPY whose source fails
      // mid-stream, and what pgjdbc's CopyManager turns into the exception above.
      assertThat(received.size())
          .as("the writer had already flushed a batch of rows before the offending one was reached")
          .isGreaterThan(64 * 1024);
    }
  }

  @Test
  @DisplayName("[#7470] a declared property the sampled row does not carry is still exported")
  void aDeclaredColumnMissingFromTheSampledRowIsStillExported() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      assertThat(copyOut(connection, "COPY (SELECT id, name, email FROM " + DECLARED + " ORDER BY id) TO STDOUT (FORMAT csv, HEADER)"))
          .as("the column set a query names explicitly was always exact; it is the yardstick for the one below")
          .isEqualTo("""
              id,name,email
              1,,
              2,second,second@example.com
              """);

      // The whole-type form, whose column set the sampler resolves. The first row carries only "id", so before
      // the fix "name" and "email" were missing from the export - including from the second row, which has them.
      final String exported = copyOut(connection, "COPY (SELECT FROM " + DECLARED + " ORDER BY id) TO STDOUT (FORMAT csv, HEADER)");
      assertThat(exported.lines().findFirst().orElseThrow().split(","))
          .as("every declared property is announced, whether or not the sampled row carried it")
          .contains("id", "name", "email");
      assertThat(exported).contains("second@example.com");
    }
  }

  @Test
  @DisplayName("[#7470] a row carrying a column the stream never announced is refused, not exported without it")
  void aSchemalessColumnTheStreamNeverAnnouncedIsRefused() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      assertThatThrownBy(() -> copyOut(connection, "COPY " + SPARSE + " TO STDOUT"))
          .isInstanceOf(PSQLException.class)
          .hasMessageContaining("which is not among the columns the stream announced")
          .hasMessageContaining("List the columns to export in the COPY itself");

      // And the way out the message names really works: with the columns listed, the export is exact.
      assertThat(copyOut(connection, "COPY (SELECT id, alpha, beta FROM " + SPARSE + " ORDER BY id) TO STDOUT (FORMAT csv, HEADER)"))
          .isEqualTo("""
              id,alpha,beta
              1,a,
              2,,b
              """);
    }
  }

  private static String copyOut(final Connection connection, final String copy) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
    copyManager.copyOut(copy, bytes);
    return bytes.toString(StandardCharsets.UTF_8);
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
