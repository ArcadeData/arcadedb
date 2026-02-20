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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaStatsQueryTest extends TestHelper {

  @Test
  void selectFromSchemaStats() {
    // Perform some operations to populate stats
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestDoc");
      database.newDocument("TestDoc").set("name", "test").save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:stats")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();

      assertThat((String) r.getProperty("databaseName")).isEqualTo(database.getName());
      assertThat(r.getPropertyNames()).contains("txCommits", "txRollbacks",
          "createRecord", "readRecord", "updateRecord", "deleteRecord",
          "queries", "commands", "scanType", "scanBucket",
          "iterateType", "iterateBucket", "countType", "countBucket");

      // We committed at least one transaction
      assertThat((Long) r.getProperty("txCommits")).isGreaterThan(0);
      // We created at least one record
      assertThat((Long) r.getProperty("createRecord")).isGreaterThan(0);

      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void statsReflectOperations() {
    try (final ResultSet rs1 = database.query("sql", "SELECT FROM schema:stats")) {
      final Result before = rs1.next();
      final long queriesBefore = (Long) before.getProperty("queries");

      // Execute some queries
      database.transaction(() -> {
        database.getSchema().createDocumentType("Counter");
        database.newDocument("Counter").set("n", 1).save();
      });

      try (final ResultSet rs2 = database.query("sql", "SELECT FROM schema:stats")) {
        final Result after = rs2.next();
        // queries counter should have increased (at least the first stats query + the one inside the tx)
        assertThat((Long) after.getProperty("queries")).isGreaterThan(queriesBefore);
      }
    }
  }
}
