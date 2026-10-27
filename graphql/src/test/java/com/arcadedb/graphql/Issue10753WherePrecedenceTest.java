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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10753: the free-form {@code where} argument was appended to the generated clause without
 * parentheses, so an {@code or} inside it bound looser than the {@code and} joining it to the other arguments and dropped
 * their equality for every row matching its right-hand disjunct - and which rows came back depended on the order the
 * arguments were written in, although GraphQL arguments are unordered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10753WherePrecedenceTest {

  private static final String DB_PATH = "./target/testgraphql_issue10753";

  /** Predicates whose top-level operator binds looser than the {@code and} the clause joins them with, or that negate. */
  private static final String[] PREDICATES = { "pages > 100 or pages < 10", "pages < 10 OR pages > 100", "not pages = 50 or pages = 5",
      "pages > 100 or pages < 10 or pages = 50" };

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void theWherePredicateIsOneOperandOfTheAndWhicheverOrderTheArgumentsAreWritten() {
    executeTest(database -> {
      for (final String predicate : PREDICATES) {
        final List<String> expected = sql(database,
            "select name, pages from Book where name = 'Target' and (" + predicate + ") order by name, pages");

        assertThat(expected).as(predicate).isNotEmpty().allMatch(row -> row.startsWith("Target/"));
        assertThat(graphql(database, "{ books(name: \"Target\", where: \"" + predicate + "\") { name pages } }")).as(predicate)
            .isEqualTo(expected);
        assertThat(graphql(database, "{ books(where: \"" + predicate + "\", name: \"Target\") { name pages } }")).as(predicate)
            .isEqualTo(expected);
      }
      return null;
    });
  }

  @Test
  void theReportedQueryAnswersOnlyTheTargetRows() {
    executeTest(database -> {
      assertThat(graphql(database, "{ books(name: \"Target\", where: \"pages > 100 or pages < 10\") { name pages } }"))
          .containsExactly("Target/5", "Target/500");
      assertThat(graphql(database, "{ books(where: \"pages > 100 or pages < 10\", name: \"Target\") { name pages } }"))
          .containsExactly("Target/5", "Target/500");
      return null;
    });
  }

  @Test
  void aPredicateThatClosesTheParenthesesItIsWrappedInIsRefused() {
    executeTest(database -> {
      // Wrapping alone does not make the predicate one operand: a predicate that closes the parenthesis it is put in and opens
      // another one balances the wrapping and escapes it. It is no complete condition on its own, which is what is checked
      for (final String predicate : new String[] { "pages > 100) or (pages < 10", "pages > 100) or (1 = 1", "pages > 0 order by pages",
          "pages > 0) order by (pages" })
        assertThatThrownBy(() -> graphql(database, "{ books(name: \"Target\", where: \"" + predicate + "\") { name pages } }"))
            .as(predicate).isInstanceOf(CommandParsingException.class);
      return null;
    });
  }

  @Test
  void aPredicateEndingInALineCommentKeepsItsOperandClosed() {
    executeTest(database -> {
      assertThat(graphql(database, "{ books(name: \"Target\", where: \"pages > 100 or pages < 10 -- big or small\") { name pages } }"))
          .containsExactly("Target/5", "Target/500");
      assertThat(graphql(database, "{ books(name: \"Target\", where: \"pages > 100 /* big */ or pages < 10\") { name pages } }"))
          .containsExactly("Target/5", "Target/500");
      // a block comment left open would swallow the closing parenthesis: it is no complete condition, and is refused
      assertThatThrownBy(() -> graphql(database, "{ books(name: \"Target\", where: \"pages > 100 /* or pages < 10\") { name pages } }"))
          .isInstanceOf(CommandParsingException.class);
      return null;
    });
  }

  @Test
  void aPredicateAloneAndAnArgumentAloneAreUnchanged() {
    executeTest(database -> {
      assertThat(graphql(database, "{ books(where: \"pages > 100 or pages < 10\") { name pages } }"))
          .containsExactly("Other/5", "Other/500", "Target/5", "Target/500");
      assertThat(graphql(database, "{ books(name: \"Other\") { name pages } }")).containsExactly("Other/5", "Other/500");
      return null;
    });
  }

  private static List<String> graphql(final Database database, final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("graphql", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        rows.add(row.getProperty("name") + "/" + row.getProperty("pages"));
      }
    }
    rows.sort(null);
    return rows;
  }

  private static List<String> sql(final Database database, final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        rows.add(row.getProperty("name") + "/" + row.getProperty("pages"));
      }
    }
    rows.sort(null);
    return rows;
  }

  private void executeTest(final Callable<Void, Database> callback) {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      if (factory.exists())
        factory.open().drop();

      final Database database = factory.create();
      try {
        database.transaction(() -> {
          database.getSchema().getOrCreateVertexType("Book");
          database.newVertex("Book").set("name", "Target", "pages", 500).save();
          database.newVertex("Book").set("name", "Target", "pages", 5).save();
          database.newVertex("Book").set("name", "Target", "pages", 50).save();
          database.newVertex("Book").set("name", "Other", "pages", 500).save();
          database.newVertex("Book").set("name", "Other", "pages", 5).save();
        });

        database.command("graphql", """
            type Query {
              books(name: String, where: WHERE): [Book]
            }

            type Book {
              name: String
              pages: Int
            }""");

        callback.call(database);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }
  }
}
