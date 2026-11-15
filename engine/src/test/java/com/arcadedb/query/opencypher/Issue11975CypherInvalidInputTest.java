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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11975: two invalid inputs ran without an error. An invalid regular expression answered false, and a DELETE of a value
 * that is no entity deleted nothing. Each is now refused with the class of error Neo4j answers it with.
 */
class Issue11975CypherInvalidInputTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/Issue11975CypherInvalidInputTest").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:L1 {name: 'abc'}), (:L2 {name: 'xyz'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void anInvalidRegularExpressionIsRefused() {
    assertThatThrownBy(() -> rows("RETURN 'a' =~ '[' AS x", Map.of())).isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Invalid Regex");
    assertThatThrownBy(() -> rows("MATCH (n:L1) WHERE n.name =~ $p RETURN n", Map.of("p", "(abc"))).isInstanceOf(
        CommandSemanticException.class).hasMessageContaining("Invalid Regex");
  }

  @Test
  void aValidOrNullRegularExpressionIsUnchanged() {
    assertThat(rows("MATCH (n) WHERE n.name =~ 'a.c' RETURN n.name AS name", Map.of())).extracting(r -> r.getProperty("name"))
        .containsExactly("abc");
    assertThat(rows("RETURN 'a' =~ null AS x", Map.of()).getFirst().<Object>getProperty("x")).isNull();
    assertThat(rows("RETURN null =~ 'a' AS x", Map.of()).getFirst().<Object>getProperty("x")).isNull();
  }

  @Test
  void deletingAValueThatIsNoEntityIsATypeError() {
    for (final String statement : new String[] { "WITH 1 AS x DELETE x", "WITH 'a' AS x DELETE x", "WITH true AS x DELETE x",
        "WITH [1, 2] AS x DELETE x", "MATCH (n:L1) WITH n, 5 AS x DELETE n, x" })
      assertThatThrownBy(() -> write(statement)).as(statement).isInstanceOf(TypeMismatchException.class)
          .hasMessageContaining("DELETE expects a node, a relationship or a path");
    // nothing was deleted by the refused statements
    assertThat(database.countType("L1", false)).isEqualTo(1);
  }

  @Test
  void deletingEntitiesNullsAndCollectionsOfEntitiesIsUnchanged() {
    write("WITH null AS x DELETE x");
    write("MATCH (n:L1) WITH collect(n) AS ns DELETE ns");
    assertThat(database.countType("L1", false)).isZero();
    write("MATCH (n:L2) DELETE n, n");
    assertThat(database.countType("L2", false)).isZero();
  }

  private List<Result> rows(final String statement, final Map<String, Object> parameters) {
    return database.query("opencypher", statement, parameters).stream().toList();
  }

  private void write(final String statement) {
    database.transaction(() -> database.command("opencypher", statement).stream().count());
  }
}
