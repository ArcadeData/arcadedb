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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6402: {@code file()} and {@code linenumber()} answered correctly in a projection and {@code null} in a
 * {@code WHERE} predicate, because the LOAD CSV row context was lifted onto the {@link
 * com.arcadedb.query.sql.executor.CommandContext} by {@code ExpressionEvaluator} - the path a projection takes -
 * and by nothing on the AST path a predicate takes.
 * <p>
 * The shape of the test is the one {@code CypherArithmeticEvaluatorParityTest} uses: the same expression in a
 * projection and in a predicate, asserted to answer identically. What made it worth a test rather than a fix is
 * that two predicate forms disagreed with <i>each other</i> in the same clause position - {@code linenumber()}
 * was null enough for {@code IS NOT NULL} to reject every row and non-null enough for {@code > 1} to keep two -
 * so no reading of Cypher makes both answers right, and asserting one form would not have caught the other.
 * <p>
 * Neo4j, the openCypher reference implementation, documents both functions as usable anywhere in the query
 * following {@code LOAD CSV}, predicates included.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLoadCSVRowContextIssue6402Test {
  private Database database;
  private String   url;

  @BeforeEach
  void setUp() throws IOException {
    final File csv = new File("./target/databases/cypher-loadcsv-6402/people.csv");
    csv.getParentFile().mkdirs();
    try (final PrintWriter writer = new PrintWriter(csv, "UTF-8")) {
      writer.println("name,age");
      writer.println("alice,30");
      writer.println("bob,40");
    }
    url = csv.getAbsolutePath();

    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-loadcsv-6402/db");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // The matrix: the same call, in a projection and in each predicate form
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void fileAnswersTheSameInAProjectionAndInEveryPredicateForm() {
    assertThat(rows("RETURN file() AS r")).hasSize(3).allMatch(url::equals);

    // Every row of the file has a file(), so every predicate form must keep all three.
    assertThat(rows("WITH row WHERE file() IS NOT NULL RETURN 1 AS r")).hasSize(3);
    assertThat(rows("WITH row WHERE file() = '" + url + "' RETURN 1 AS r")).hasSize(3);
    assertThat(rows("WITH row WHERE file() IS NOT NULL AND linenumber() > 0 RETURN 1 AS r")).hasSize(3);
    assertThat(rows("WITH row WHERE NOT file() IS NULL RETURN 1 AS r")).hasSize(3);
    assertThat(rows("WITH row WHERE file() IS NULL RETURN 1 AS r")).isEmpty();
  }

  @Test
  void lineNumberAnswersTheSameInAProjectionAndInEveryPredicateForm() {
    assertThat(rows("RETURN linenumber() AS r")).containsExactly(1, 2, 3);

    assertThat(rows("WITH row WHERE linenumber() IS NOT NULL RETURN linenumber() AS r")).containsExactly(1, 2, 3);
    // Skipping the header this way is the single most common LOAD CSV idiom.
    assertThat(rows("WITH row WHERE linenumber() > 1 RETURN linenumber() AS r")).containsExactly(2, 3);
    assertThat(rows("WITH row WHERE linenumber() > 1 AND linenumber() IS NOT NULL RETURN linenumber() AS r"))
        .containsExactly(2, 3);
    assertThat(rows("WITH row WHERE linenumber() >= 1 RETURN linenumber() AS r")).containsExactly(1, 2, 3);
    assertThat(rows("WITH row WHERE linenumber() > 5 RETURN linenumber() AS r")).isEmpty();
  }

  @Test
  void theRowContextSurvivesAProjectionThatDoesNotNameIt() {
    // A projection keeps only what it names, so after `WITH row` the context used to be gone - and, because the
    // two functions read a context variable no later row reset, every row answered with the line number of
    // whichever row happened to be evaluated last.
    assertThat(rows("WITH row RETURN linenumber() AS r")).containsExactly(1, 2, 3);
    assertThat(rows("WITH row AS renamed RETURN linenumber() AS r")).containsExactly(1, 2, 3);
    assertThat(rows("WITH row RETURN file() AS r")).hasSize(3).allMatch(url::equals);
    assertThat(rows("WITH row, linenumber() AS ln WITH ln RETURN ln AS r")).containsExactly(1, 2, 3);
  }

  @Test
  void theRowContextSurvivesAProjectionThatDropsRowEntirely() {
    // The deliberate reading, pinned per code review: file()/linenumber() are a property of the query's
    // position relative to LOAD CSV, not of row still being in scope - the same way count(*) needs no variable
    // bound. "WITH 1 AS x" drops row from scope entirely and must not turn the functions off.
    assertThat(rows("WITH 1 AS x RETURN linenumber() AS r")).containsExactly(1, 2, 3);
    assertThat(rows("WITH 1 AS x RETURN file() AS r")).hasSize(3).allMatch(url::equals);
  }

  // ---------------------------------------------------------------------------------------------------------
  // The context is execution state, so it does not become a column of its own
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void theRowContextIsNotProjectedByReturnStar() {
    for (final String query : new String[] { "RETURN *", "WITH * RETURN *", "WITH row RETURN *" })
      try (final ResultSet resultSet = database.query("opencypher", load(query))) {
        while (resultSet.hasNext()) {
          final Result result = resultSet.next();
          assertThat(result.getPropertyNames())
              .as("%s must project the query's own variables and nothing else", query)
              .containsExactly("row");
        }
      }
  }

  // ---------------------------------------------------------------------------------------------------------

  private String load(final String tail) {
    return "LOAD CSV FROM '" + url + "' AS row " + tail;
  }

  private List<Object> rows(final String tail) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", load(tail))) {
      while (resultSet.hasNext())
        values.add(resultSet.next().getProperty("r"));
    }
    return values;
  }
}
