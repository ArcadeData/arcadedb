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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #4227.
 * <p>
 * {@code CREATE INDEX ON <type> (<properties>)} without an explicit index type
 * (UNIQUE, NOTUNIQUE, FULL_TEXT, ...) used to throw a confusing
 * {@link NullPointerException}: {@code "Cannot invoke
 * Identifier.getStringValue() because this.type is null"}. The ANTLR grammar
 * marks {@code indexType} as optional, so the parser accepts the statement
 * and the missing type was only detected by an unguarded NPE at execution
 * time. After the fix, a {@link CommandSQLParsingException} is thrown with a
 * clear message listing the supported index types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4227CreateIndexMissingTypeTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4227-create-index-missing-type").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void createIndexWithoutTypeThrowsParsingException() {
    database.command("sql", "CREATE VERTEX TYPE DOCUMENT");
    database.command("sql", "CREATE VERTEX TYPE CHUNK");
    database.command("sql", "CREATE EDGE TYPE `in`");

    database.command("sql", "CREATE PROPERTY CHUNK.subtype STRING");
    database.command("sql", "CREATE PROPERTY CHUNK.name STRING");
    database.command("sql", "CREATE PROPERTY CHUNK.text STRING");
    database.command("sql", "CREATE PROPERTY CHUNK.`index` INTEGER");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON CHUNK (name)"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Index type is required")
        .hasMessageContaining("UNIQUE")
        .hasMessageContaining("NOTUNIQUE");
  }

  @Test
  void createIndexWithTypeStillWorks() {
    database.command("sql", "CREATE VERTEX TYPE CHUNK");
    database.command("sql", "CREATE PROPERTY CHUNK.name STRING");

    database.command("sql", "CREATE INDEX ON CHUNK (name) NOTUNIQUE");

    assertThat(database.getSchema().existsIndex("CHUNK[name]")).isTrue();
  }
}
