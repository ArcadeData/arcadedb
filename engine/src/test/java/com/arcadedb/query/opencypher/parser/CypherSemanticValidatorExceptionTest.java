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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test verifying that semantic validation errors raised by {@link CypherSemanticValidator}
 * (for example an undefined variable reference) are reported as {@link CommandSemanticException}, a
 * subclass of {@link CommandParsingException}, so callers can distinguish a semantic error from a
 * genuine syntax error while existing handlers that only know about the parent class keep working.
 */
class CypherSemanticValidatorExceptionTest {
  private static final String DB_PATH = "./target/databases/CypherSemanticValidatorExceptionTest";
  private Database db;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    db = factory.create();
    db.getSchema().createVertexType("Beer");
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void undefinedVariableThrowsSemanticException() {
    assertThatThrownBy(() -> db.query("opencypher", "MATCH (n:Beer) RETURN undefinedVariable").close())
        .isInstanceOf(CommandSemanticException.class)
        .isInstanceOf(CommandParsingException.class);
  }
}
