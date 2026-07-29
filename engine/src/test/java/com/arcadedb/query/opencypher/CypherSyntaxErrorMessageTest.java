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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A Cypher syntax error must say what was expected, not dump the grammar.
 * <p>
 * In Cypher nearly every keyword is also a legal identifier, so wherever an expression or a name is
 * expected ANTLR's raw message enumerates 300+ token names (~4 KB). That buries the position and the
 * offending token, which are the only parts a user acts on. Reference implementations phrase it as a
 * concept instead - Neo4j 2026.06.0 answers {@code Invalid input ']', expected: an expression} - and
 * this test pins the same shape for ArcadeDB. Short alternative sets keep ANTLR's own enumeration,
 * which is already actionable (e.g. {@code expecting '}', ','}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSyntaxErrorMessageTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphersyntaxmsg").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void missingExpressionReportsTheConceptNotTheTokenDump() {
    final String message = syntaxErrorOf("RETURN [1, 2,] AS v");

    assertThat(message).contains("line 1:13").contains("']'").endsWith("expected: an expression");
    assertThat(message).doesNotContain("UNSIGNED_HEX_INTEGER").doesNotContain("ADMINISTRATOR");
    assertThat(message.length()).isLessThan(200);
  }

  @Test
  void missingNameReportsTheConceptNotTheTokenDump() {
    final String message = syntaxErrorOf("RETURN {a: 1, } AS v");

    assertThat(message).contains("'}'").endsWith("expected: a name");
    assertThat(message.length()).isLessThan(200);
  }

  @Test
  void shortAlternativeSetsKeepAntlrEnumeration() {
    // Only a handful of tokens are legal after a map entry, so the explicit list stays useful
    final String message = syntaxErrorOf("RETURN {a: 1 2} AS v");

    assertThat(message).contains("'2'").doesNotContain("expected: an expression").doesNotContain("expected: a name");
    assertThat(message.length()).isLessThan(200);
  }

  @Test
  void messagesAlwaysCarryPositionAndOffendingToken() {
    assertThat(syntaxErrorOf("MATCH (n) RETURN n.")).startsWith("Syntax error at line 1:19");
    assertThat(syntaxErrorOf("RETURN coalesce(1, 2,) AS v")).startsWith("Syntax error at line 1:21");
  }

  private String syntaxErrorOf(final String query) {
    final Throwable thrown = catchThrowable(() -> {
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          rs.next();
      }
    });

    assertThat(thrown).as("query <%s> must be rejected", query).isInstanceOf(CommandParsingException.class);
    return thrown.getMessage();
  }
}
