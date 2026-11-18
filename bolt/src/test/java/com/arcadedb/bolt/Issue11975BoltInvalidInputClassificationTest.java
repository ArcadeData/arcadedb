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
 * Issue #11975: the refusals of three invalid inputs reach a Bolt client as the client errors Neo4j answers them with: an
 * invalid regular expression a SemanticError, a DELETE of a value that is no entity a TypeError, a YIELD of an output the
 * procedure does not declare a SyntaxError. Classified from the exception the engine really throws.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11975BoltInvalidInputClassificationTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/Issue11975BoltInvalidInputClassificationTest");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher", "CREATE (:L1)"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void eachRefusalIsTheClientErrorNeo4jAnswers() {
    assertThat(codeOf("RETURN 'a' =~ '[' AS x")).isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
    assertThat(codeOf("WITH 1 AS x DELETE x")).isEqualTo(BoltErrorCodes.TYPE_ERROR);
    assertThat(codeOf("CALL db.labels() YIELD nosuch RETURN nosuch")).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
  }

  private String codeOf(final String statement) {
    final Throwable error = catchThrowable(() -> database.transaction(() -> database.command("opencypher", statement).stream().count()));
    assertThat(error).as(statement).isNotNull();
    return BoltNetworkExecutor.classifyExecutionError(error, BoltErrorCodes.DATABASE_ERROR);
  }
}
