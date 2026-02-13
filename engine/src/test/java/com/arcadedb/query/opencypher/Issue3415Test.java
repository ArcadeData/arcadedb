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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3415.
 * Tests that CREATE with a path variable assignment returns the path correctly,
 * allowing length(p) to work.
 * <p>
 * Query: {@code CREATE p=(:CP1)-[:Rel]->(:CP2) RETURN length(p) as l}
 * Expected: returns 1 (one relationship in the path)
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3415">Issue 3415</a>
 */
class Issue3415Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue3415-test").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void createPathVariableWithLengthFunction() {
    final ResultSet rs = database.command("opencypher",
        "CREATE p=(:CP1)-[:Rel]->(:CP2) RETURN length(p) as l");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<Long>getProperty("l")).isEqualTo(1L);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void createPathVariableReturnsPath() {
    final ResultSet rs = database.command("opencypher",
        "CREATE p=(:CP1)-[:Rel]->(:CP2) RETURN p");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object p = result.getProperty("p");
    assertThat(p).isNotNull();
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void createMultiHopPathVariableWithLength() {
    final ResultSet rs = database.command("opencypher",
        "CREATE p=(:A)-[:R1]->(:B)-[:R2]->(:C) RETURN length(p) as l");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<Long>getProperty("l")).isEqualTo(2L);
    assertThat(rs.hasNext()).isFalse();
  }
}
