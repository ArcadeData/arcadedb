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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.AlignDatabaseStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11781: CHECK DATABASE (with or without FIX), CHECK DATABASE COMPRESS and ALIGN DATABASE rolled the caller's open
 * transaction back before they ran, so the writes made earlier in it were lost and the next statement failed with "Transaction
 * not begun". They now run outside it, as COMPACT INDEX does since #11580, and leave it as it was. BACKUP DATABASE shares the
 * same rule; it needs the integration module and is covered there.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11781MaintenanceKeepsCallerTransactionTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.id INTEGER");
    database.command("sql", "CREATE INDEX ON D (id) UNIQUE");
  }

  @Test
  void maintenanceStatementsInsideAJavaTransactionKeepIt() {
    int id = 0;
    for (final String statement : new String[] { "CHECK DATABASE", "CHECK DATABASE FIX", "CHECK DATABASE COMPRESS",
        "CHECK DATABASE TYPE D", "COMPACT INDEX `D[id]`" }) {
      final int first = ++id;
      final int second = ++id;
      final long before = count();
      database.transaction(() -> {
        database.command("sql", "INSERT INTO D SET id = ?", first).close();
        try (final ResultSet rs = database.command("sql", statement)) {
          assertThat(rs.hasNext()).as(statement).isTrue();
        }
        assertThat(database.isTransactionActive()).as("the transaction is still open after " + statement).isTrue();
        database.command("sql", "INSERT INTO D SET id = ?", second).close();
      });
      assertThat(count()).as("both writes around " + statement + " are committed").isEqualTo(before + 2);
    }
  }

  @Test
  void maintenanceStatementsInsideAScriptTransactionKeepIt() {
    int id = 100;
    for (final String statement : new String[] { "CHECK DATABASE", "CHECK DATABASE FIX", "CHECK DATABASE COMPRESS" }) {
      final long before = count();
      database.command("sqlscript",
          "BEGIN; INSERT INTO D SET id = " + (++id) + "; " + statement + "; INSERT INTO D SET id = " + (++id) + "; COMMIT;").close();
      assertThat(count()).as("both writes around " + statement + " are committed").isEqualTo(before + 2);
    }
  }

  @Test
  void checkerApiInsideATransactionKeepsIt() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO D SET id = 1000").close();
      final Map<String, Object> result = new DatabaseChecker(database).check();
      assertThat(result).containsKey("autoFix");
      new DatabaseChecker(database).compress();
      assertThat(database.isTransactionActive()).isTrue();
      database.command("sql", "INSERT INTO D SET id = 1001").close();
    });
    assertThat(count()).isEqualTo(2);
  }

  @Test
  void checkReportsTheCommittedDatabaseNotTheCallersPendingChanges() {
    database.transaction(() -> database.command("sql", "INSERT INTO D SET id = 1").close());
    database.transaction(() -> {
      database.command("sql", "INSERT INTO D SET id = 2").close();
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE TYPE D")) {
        assertThat(rs.next().<Number>getProperty("totalActiveRecords").longValue()).isEqualTo(1L);
      }
    });
    assertThat(count()).isEqualTo(2);
  }

  @Test
  void alignDatabaseIsRefusedAsAClientErrorAndKeepsTheTransaction() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO D SET id = 1").close();
      // issue #11796: no database aligns replicas, refused with a message saying so instead of a bare
      // UnsupportedOperationException (HTTP 500)
      assertThatThrownBy(() -> database.command("sql", "ALIGN DATABASE").close()).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED);
      assertThat(database.isTransactionActive()).isTrue();
      database.command("sql", "INSERT INTO D SET id = 2").close();
    });
    assertThat(count()).isEqualTo(2);
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM D")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
