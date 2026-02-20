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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
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
  void dropUnregistersListeners() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("UnregTest");
    });

    database.transaction(() -> {
      database.newDocument("UnregTest").set("v", 1).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("UnregView")
          .withQuery("SELECT v FROM UnregTest")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    // Drop the view
    database.transaction(() -> {
      database.getSchema().dropMaterializedView("UnregView");
    });

    // Insert after drop should not cause errors (listeners should be unregistered)
    database.transaction(() -> {
      database.newDocument("UnregTest").set("v", 2).save();
    });

    assertThat(database.getSchema().existsMaterializedView("UnregView")).isFalse();
  }

  @Test
  void incrementalRefreshWorksAfterRollback() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("PostRollback");
    });

    database.transaction(() -> {
      database.newDocument("PostRollback").set("v", 1).save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("PostRollbackView")
          .withQuery("SELECT v FROM PostRollback")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM PostRollbackView")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }

    // Rollback a transaction
    database.begin();
    database.newDocument("PostRollback").set("v", 2).save();
    database.rollback();

    // Now commit a new transaction — incremental refresh should still work
    database.transaction(() -> {
      database.newDocument("PostRollback").set("v", 3).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM PostRollbackView")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void updatePropagatesAfterCommit() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Staff");
      database.getSchema().getType("Staff").createProperty("name", Type.STRING);
      database.getSchema().getType("Staff").createProperty("dept", Type.STRING);
    });

    database.transaction(() -> {
      database.newDocument("Staff").set("name", "Alice").set("dept", "Eng").save();
      database.newDocument("Staff").set("name", "Bob").set("dept", "Sales").save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("StaffView")
          .withQuery("SELECT name, dept FROM Staff")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM StaffView")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }

    // Update a record — should trigger post-commit refresh
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Staff WHERE name = 'Alice'")) {
        final MutableDocument doc = rs.next().toElement().modify();
        doc.set("dept", "HR");
        doc.save();
      }
    });

    // View should reflect the updated dept
    try (final ResultSet rs = database.query("sql", "SELECT FROM StaffView WHERE name = 'Alice'")) {
      final Result result = rs.next();
      assertThat((String) result.getProperty("dept")).isEqualTo("HR");
    }
  }

  @Test
  void deletePropagatesAfterCommit() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Ticket");
      database.getSchema().getType("Ticket").createProperty("title", Type.STRING);
    });

    database.transaction(() -> {
      database.newDocument("Ticket").set("title", "Bug 1").save();
      database.newDocument("Ticket").set("title", "Bug 2").save();
      database.newDocument("Ticket").set("title", "Bug 3").save();
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("TicketView")
          .withQuery("SELECT title FROM Ticket")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM TicketView")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }

    // Delete one record — should trigger post-commit refresh
    database.transaction(() -> database.command("sql", "DELETE FROM Ticket WHERE title = 'Bug 2'"));

    try (final ResultSet rs = database.query("sql", "SELECT FROM TicketView")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void listenerNoOpWithoutActiveTransaction() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("NoTxSrc");
    });

    database.transaction(() -> {
      database.getSchema().buildMaterializedView()
          .withName("NoTxView")
          .withQuery("SELECT FROM NoTxSrc")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create();
    });

    final MaterializedViewImpl view = (MaterializedViewImpl)
        database.getSchema().getMaterializedView("NoTxView");
    final MaterializedViewChangeListener listener = view.getChangeListener();

    // No transaction is active — all three callback methods must be no-ops (no exception)
    assertThat(database.isTransactionActive()).isFalse();
    listener.onAfterCreate(null);
    listener.onAfterUpdate(null);
    listener.onAfterDelete(null);

    // Status and record count must be unchanged
    assertThat(view.getStatus()).isEqualTo("VALID");
    try (final ResultSet rs = database.query("sql", "SELECT FROM NoTxView")) {
      assertThat(rs.stream().count()).isEqualTo(0);
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
