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
 * Regression test for issue #5178: backtick-escaped reserved keywords used as variable
 * names in Cypher MATCH clauses must be treated as valid identifiers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherBacktickedVariableTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcypherbacktickedvariable").create();
    database.getSchema().createVertexType("A");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void backtickedKeywordAsVariable() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:A {v:1})"));

    final ResultSet result = database.query("opencypher", "MATCH (`match`:A) RETURN `match`.v AS v");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<Integer>getProperty("v")).isEqualTo(1);
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void backtickedNonKeywordAsVariable() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:A {v:2})"));

    final ResultSet result = database.query("opencypher", "MATCH (`foo`:A) RETURN `foo`.v AS v");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("v")).isEqualTo(2);
  }

  @Test
  void backtickedKeywordBareVariableReturn() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:A {v:3})"));

    final ResultSet result = database.query("opencypher", "MATCH (`return`:A) RETURN `return`.v AS v");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("v")).isEqualTo(3);
  }

  @Test
  void backtickedKeywordInWhere() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:A {v:4})"));

    final ResultSet result = database.query("opencypher",
        "MATCH (`where`:A) WHERE `where`.v = 4 RETURN `where`.v AS v");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("v")).isEqualTo(4);
  }
}
