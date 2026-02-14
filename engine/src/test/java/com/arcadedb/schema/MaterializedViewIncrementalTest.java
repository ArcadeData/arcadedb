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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewIncrementalTest extends TestHelper {

  @Test
  void insertPropagatesAfterCommit() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Employee");
    });

    database.transaction(() -> {
      database.newDocument("Employee").set("name", "Alice").save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("EmployeeView")
          .withQuery("SELECT name FROM Employee")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    // Initial refresh should have 1 record
    try (final ResultSet rs = database.query("sql", "SELECT FROM EmployeeView")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }

    // Insert in a new transaction — should trigger post-commit refresh
    database.transaction(() -> {
      database.newDocument("Employee").set("name", "Bob").save();
    });

    // View should now have 2 records
    try (final ResultSet rs = database.query("sql", "SELECT FROM EmployeeView")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void rollbackDoesNotAffectView() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("RollbackTest");
    });

    database.transaction(() -> {
      database.newDocument("RollbackTest").set("value", 1).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("RollbackView")
          .withQuery("SELECT value FROM RollbackTest")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    // Insert and rollback
    database.begin();
    database.newDocument("RollbackTest").set("value", 2).save();
    database.rollback();

    // View should still have 1 record
    try (final ResultSet rs = database.query("sql", "SELECT FROM RollbackView")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void complexQueryFullRefreshAfterCommit() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Sale");
    });

    database.transaction(() -> {
      database.newDocument("Sale").set("product", "A").set("amount", 10).save();
      database.newDocument("Sale").set("product", "A").set("amount", 20).save();
      database.newDocument("Sale").set("product", "B").set("amount", 30).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("SaleSummary")
          .withQuery("SELECT product, sum(amount) as total FROM Sale GROUP BY product")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    // Initial refresh should have 2 groups
    try (final ResultSet rs = database.query("sql", "SELECT FROM SaleSummary")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }

    // Add more data — complex query triggers full refresh
    database.transaction(() -> {
      database.newDocument("Sale").set("product", "C").set("amount", 50).save();
    });

    // Should have 3 groups now
    try (final ResultSet rs = database.query("sql", "SELECT FROM SaleSummary")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }
  }
}
