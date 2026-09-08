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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A row that cannot be read aborts the vertex pass from inside the row loop. What the importer owes
 * the caller at that point is the subject of issue #7264: the transaction it opened has to be
 * resolved rather than left on the caller's stack, and the count it reports has to be the number of
 * vertices that actually reached the disk rather than zero.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class GraphImporterFailedSourceTest {

  private static final String DB_PATH = "target/databases/graph-importer-failed-source-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Row");
      database.getSchema().createVertexType("Marker");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * Rows held in memory rather than in a file: the intermediate commit fires every 50,000 rows, and
   * placing a failure just past one needs that many rows without writing them to disk first.
   * {@code score} is declared as an integer, so the row at {@code badRow} throws from
   * {@code readProperty} - one of the throw sites the issue lists, reached the way a malformed
   * source reaches it.
   */
  private static GraphImporter.RecordSource rows(final int total, final int badRow) {
    return visitor -> {
      for (int i = 0; i < total; i++) {
        final int row = i;
        visitor.visit(attribute -> switch (attribute) {
          case "id" -> String.valueOf(row);
          case "score" -> row == badRow ? "not-a-number" : String.valueOf(row);
          default -> null;
        });
      }
    };
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  /**
   * No caller transaction: the one the importer pushed is the only one there is, and a failure must
   * not leave it active for whatever runs next on this thread.
   */
  @Test
  void aFailedVertexSourceLeavesNoTransactionActive() throws Exception {
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Row", rows(10, 5), v -> {
          v.id("id");
          v.intProperty("score", "score");
        })
        .build()) {

      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("score");
    }

    assertThat(database.isTransactionActive())
        .as("the transaction processVertexSource opened must be resolved before the failure propagates")
        .isFalse();
    assertThat(countOf("Row"))
        .as("nothing was committed before the failure, so the rolled-back rows must not be on disk")
        .isZero();
  }

  /**
   * The case the issue reports: the caller already holds a transaction, so {@code database.begin()}
   * pushes a nested one. Leaving it there shadows the caller's - the caller's next {@code commit()}
   * pops the importer's instead, committing rows the importer had abandoned and leaving the
   * caller's own work uncommitted and still open.
   */
  @Test
  void aFailedVertexSourceGivesTheCallerItsOwnTransactionBack() throws Exception {
    database.begin();
    database.newVertex("Marker").set("name", "caller").save();

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Row", rows(10, 5), v -> {
          v.id("id");
          v.intProperty("score", "score");
        })
        .build()) {

      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("score");
    }

    // The caller commits the transaction it opened, unaware that the importer failed inside one of
    // its own. That commit has to land on the caller's work
    database.commit();

    assertThat(database.isTransactionActive())
        .as("one commit for the one transaction the caller opened must leave nothing active")
        .isFalse();
    assertThat(countOf("Marker"))
        .as("the caller's own record must be what its commit made durable")
        .isEqualTo(1);
    assertThat(countOf("Row"))
        .as("the importer's abandoned rows must not ride out on the caller's commit")
        .isZero();
  }

  /**
   * Past the first intermediate commit the partial import is durable whatever happens next, and the
   * number the operator reads is what decides whether to resume, truncate or start over. Reporting
   * zero for 50,000 rows that are on the disk turns a partial import into the wrong decision.
   */
  @Test
  void aFailureAfterAnIntermediateCommitReportsTheRowsThatCommitted() throws Exception {
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Row", rows(50_010, 50_005), v -> {
          v.id("id");
          v.intProperty("score", "score");
        })
        .build()) {

      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("score");

      assertThat(importer.getVertexCount())
          .as("the report must name the rows the intermediate commit made durable, not zero")
          .isEqualTo(50_000);
    }

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countOf("Row"))
        .as("the reported count must be the count the database actually holds")
        .isEqualTo(50_000);
  }
}
