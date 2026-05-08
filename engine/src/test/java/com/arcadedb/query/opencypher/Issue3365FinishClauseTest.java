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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the GQL FINISH clause (issue #3365 section 1.3).
 * FINISH explicitly terminates a query that produces no results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3365FinishClauseTest {
  private Database database;
  private String   databasePath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-finish-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Person");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void finishAfterCreatePersistsAndReturnsEmpty() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "CREATE (n:Person {name: 'Alice'}) FINISH");
      assertThat(result.hasNext()).isFalse();
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  @Test
  void finishAfterMatchReturnsEmptyWithNoRowLeak() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), (:Person {name: 'C'})");
    });

    final ResultSet result = database.query("opencypher", "MATCH (n:Person) FINISH");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void finishAfterInsertPersistsAndReturnsEmpty() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "INSERT (n:Person {name: 'Bob'}) FINISH");
      assertThat(result.hasNext()).isFalse();
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Bob'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  @Test
  void finishWithReturnIsParseError() {
    assertThatThrownBy(() ->
        database.query("opencypher", "MATCH (n:Person) RETURN n FINISH"))
        .isInstanceOf(CommandParsingException.class);
  }
}
