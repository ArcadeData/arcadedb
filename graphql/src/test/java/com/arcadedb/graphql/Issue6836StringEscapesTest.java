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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6836: the lexer accepts the whole GraphQL escape set but {@code StringValue.getValue()}
 * only stripped the surrounding quotes, so escape sequences reached the database as literal backslash sequences,
 * both as bound parameters and interpolated into the generated SQL. Argument values are now bound as SQL parameters
 * as well, so a value carrying a quote can no longer influence the statement text.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6836StringEscapesTest {

  private static final String DB_PATH = "./target/testgraphql_issue6836";

  private static final String NEW_LINE   = "a\nb";
  private static final String QUOTED     = "He said \"hi\" to me";
  private static final String TABBED     = "left\tright";
  private static final String BACKSLASH  = "C:\\temp";
  private static final String UNICODE    = "caff\u00e8";
  private static final String SQL_QUOTES = "O'Brien \" or 1=1 --";

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void escapeSequencesAreDecodedInArgumentValues() {
    executeTest(database -> {
      defineTypes(database);

      // Each of these compared a 4-character `a\nb` against the 3-character `a<LF>b` before the fix, so a record
      // that really contains the character never matched.
      assertSingleMatch(database, "{ noteByText(text: \"a\\nb\") { id } }", "note-newline");
      assertSingleMatch(database, "{ noteByText(text: \"He said \\\"hi\\\" to me\") { id } }", "note-quoted");
      assertSingleMatch(database, "{ noteByText(text: \"left\\tright\") { id } }", "note-tabbed");
      assertSingleMatch(database, "{ noteByText(text: \"C:\\\\temp\") { id } }", "note-backslash");
      assertSingleMatch(database, "{ noteByText(text: \"caff\\u00e8\") { id } }", "note-unicode");

      return null;
    });
  }

  @Test
  void escapeSequencesAreDecodedOnTheNativeDirectivePath() {
    executeTest(database -> {
      defineTypes(database);

      // The @sql directive path binds the argument as a SQL parameter: the raw token image used to be bound there
      // too, so the very same mismatch happened one layer down.
      assertSingleMatch(database, "{ noteBySql(textParameter: \"a\\nb\") { id } }", "note-newline");
      assertSingleMatch(database, "{ noteBySql(textParameter: \"caff\\u00e8\") { id } }", "note-unicode");

      return null;
    });
  }

  @Test
  void quoteBearingValueIsBoundAndNotInterpolated() {
    executeTest(database -> {
      defineTypes(database);

      // The value carries both quote flavours and a SQL comment marker. Quoting it into the statement text by hand
      // made its correctness depend on the SQL lexer implementing exactly the GraphQL escape conventions; binding it
      // as a parameter makes it data by construction.
      assertSingleMatch(database, "{ noteByText(text: \"O'Brien \\\" or 1=1 --\") { id } }", "note-sql");

      return null;
    });
  }

  @Test
  void unescapedValuesKeepWorking() {
    executeTest(database -> {
      defineTypes(database);

      assertSingleMatch(database, "{ noteByText(text: \"plain\") { id } }", "note-plain");
      assertSingleMatch(database, "{ noteByRank(rank: 42) { id } }", "note-plain");

      return null;
    });
  }

  @Test
  void freeFormPredicateCannotClaimTheReservedParameterPrefix() {
    executeTest(database -> {
      defineTypes(database);

      // The `where` argument is interpolated verbatim, so it could name a parameter that collides with the ones
      // bound for the other arguments. The prefix is reserved by rejecting any predicate that uses it, rather than
      // by assuming nobody would pick it.
      assertThatThrownBy(() -> database.query("graphql", "{ notesWhere( where: \"text = :__gqlArg0\" ) { id } }").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("__gqlArg");

      // An ordinary predicate is untouched.
      assertSingleMatch(database, "{ notesWhere( where: \"id = 'note-plain'\" ) { id } }", "note-plain");

      return null;
    });
  }

  private static void assertSingleMatch(final Database database, final String query, final String expectedId) {
    try (final ResultSet resultSet = database.query("graphql", query)) {
      assertThat(resultSet.hasNext()).as(query).isTrue();
      assertThat(resultSet.next().<String>getProperty("id")).as(query).isEqualTo(expectedId);
      assertThat(resultSet.hasNext()).as(query).isFalse();
    }
  }

  private static void defineTypes(final Database database) {
    final String types = """
        type Query {
          noteByText(text: String): Note
          noteByRank(rank: Int): Note
          noteBySql(textParameter: String): Note @sql(statement: "select from Note where text = :textParameter")
          notesWhere(where: WHERE): [Note]
        }

        type Note {
          id: String
          text: String
          rank: Int
        }""";
    database.command("graphql", types);
  }

  private void executeTest(final Callable<Void, Database> callback) {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      if (factory.exists())
        factory.open().drop();

      final Database database = factory.create();
      try {
        database.transaction(() -> {
          final Schema schema = database.getSchema();
          schema.getOrCreateVertexType("Note");

          newNote(database, "note-newline", NEW_LINE);
          newNote(database, "note-quoted", QUOTED);
          newNote(database, "note-tabbed", TABBED);
          newNote(database, "note-backslash", BACKSLASH);
          newNote(database, "note-unicode", UNICODE);
          newNote(database, "note-sql", SQL_QUOTES);
          newNote(database, "note-plain", "plain").set("rank", 42).save();
        });

        database.transaction(() -> callback.call(database));
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }
  }

  private static MutableVertex newNote(final Database database, final String id, final String text) {
    final MutableVertex note = database.newVertex("Note");
    note.set("id", id);
    note.set("text", text);
    note.save();
    return note;
  }
}
