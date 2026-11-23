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
package com.arcadedb.bolt;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11974: the failures the engine raises for three openCypher user errors are client errors over Bolt, with Neo4j's codes,
 * where they were {@code Neo.DatabaseError.General.UnknownError}, an unexplained server fault to a driver. Classified from the
 * exception the engine really throws, wrapped as the query engine wraps it.
 */
class Issue11974BoltUserErrorClassificationTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/Issue11974BoltUserErrorClassificationTest").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:A {x: 1})-[:R]->(:B), (:C {x: 1})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void deletingANodeWithRelationshipsIsAConstraintValidationFailure() {
    assertThat(codeOf("MATCH (n:A) DELETE n")).isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed");
  }

  @Test
  void usingANodeDeletedEarlierInTheStatementIsEntityNotFound() {
    assertThat(codeOf("MATCH (n:C) DELETE n RETURN n.x AS x")).isEqualTo("Neo.ClientError.Statement.EntityNotFound");
    assertThat(codeOf("MATCH (n:C) DELETE n SET n.y = 1")).isEqualTo("Neo.ClientError.Statement.EntityNotFound");
    assertThat(codeOf("MATCH (n:C) DELETE n CREATE (n)-[:R]->(:B)")).isEqualTo("Neo.ClientError.Statement.EntityNotFound");
  }

  @Test
  void aLoadCsvFileThatDoesNotExistIsAnExternalResourceFailure() {
    assertThat(codeOf("LOAD CSV FROM 'file:///Issue11974-no-such-file.csv' AS l RETURN l"))
        .isEqualTo("Neo.ClientError.Statement.ExternalResourceFailed");
  }

  private String codeOf(final String statement) {
    // the rows are pulled, as a client does: a LOAD CSV opens its file at the first row
    final Throwable error = catchThrowable(() -> database.transaction(() -> database.command("opencypher", statement).stream().count()));
    assertThat(error).as(statement).isNotNull();
    return BoltNetworkExecutor.classifyExecutionError(error, BoltErrorCodes.DATABASE_ERROR);
  }
}
