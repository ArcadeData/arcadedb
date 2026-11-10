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
package com.arcadedb.postgres;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11483: the split of a simple-query ('Q') text into its statements, on {@code ;} outside every quoted form and
 * comment, as PostgreSQL's lexer reads them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11483StatementSplitTest {

  private static List<String> split(final String text) {
    return PostgresQuotedIdentifierRewriter.splitStatements(text, true, true);
  }

  @Test
  @DisplayName("[#11483] the psqlodbc 16 connect-time string is three statements")
  void psqlodbcConnectString() {
    assertThat(split("SET DateStyle = 'ISO';SET extra_float_digits = 2;show transaction_isolation"))
        .containsExactly("SET DateStyle = 'ISO'", "SET extra_float_digits = 2", "show transaction_isolation");
  }

  @Test
  @DisplayName("[#11483] a text without a separator is one statement, the same instance")
  void singleStatement() {
    final String text = "SELECT * FROM V WHERE name = 'a'";
    assertThat(split(text)).containsExactly(text);
    assertThat(split(text).get(0)).isSameAs(text);
  }

  @Test
  @DisplayName("[#11483] a trailing ';' and empty statements add no statement")
  void emptyStatementsAreDropped() {
    assertThat(split("SELECT 1;")).containsExactly("SELECT 1");
    assertThat(split("SELECT 1;;  ;SELECT 2 ; ")).containsExactly("SELECT 1", "SELECT 2 ");
    assertThat(split(";")).isEmpty();
    assertThat(split(" ; ;; ")).isEmpty();
    assertThat(split("")).isEmpty();
  }

  @Test
  @DisplayName("[#11483] a statement made only of comments is no statement")
  void commentOnlyStatementsAreDropped() {
    assertThat(split("-- nothing here")).isEmpty();
    assertThat(split("/* nothing */")).isEmpty();
    assertThat(split("SELECT 1; -- trailing; comment")).containsExactly("SELECT 1");
    assertThat(split("SELECT 1; /* a; b */ ; SELECT 2")).containsExactly("SELECT 1", " SELECT 2");
  }

  @Test
  @DisplayName("[#11483] a ';' inside a single-quoted literal does not split, doubling and E'' escapes honoured")
  void singleQuotedLiterals() {
    assertThat(split("SELECT 'a;b';SELECT 'it''s; fine'")).containsExactly("SELECT 'a;b'", "SELECT 'it''s; fine'");
    assertThat(split("SELECT E'x\\';y';SELECT 2")).containsExactly("SELECT E'x\\';y'", "SELECT 2");
    // with standard_conforming_strings on, a backslash in a plain literal is an ordinary character: the quote after it closes
    assertThat(split("SELECT 'C:\\';SELECT 2")).containsExactly("SELECT 'C:\\'", "SELECT 2");
  }

  @Test
  @DisplayName("[#11483] with standard_conforming_strings off a backslash escapes the quote of a plain literal")
  void nonStandardConformingStrings() {
    assertThat(PostgresQuotedIdentifierRewriter.splitStatements("SELECT 'x\\';y';SELECT 2", false, true))
        .containsExactly("SELECT 'x\\';y'", "SELECT 2");
  }

  @Test
  @DisplayName("[#11483] a ';' inside a quoted identifier, a back-tick name, a dollar-quoted literal or a comment does not split")
  void otherQuotedFormsAndComments() {
    assertThat(split("SELECT \"a;\"\"b\" FROM t;SELECT 2")).containsExactly("SELECT \"a;\"\"b\" FROM t", "SELECT 2");
    assertThat(split("SELECT `a;b` FROM t;SELECT 2")).containsExactly("SELECT `a;b` FROM t", "SELECT 2");
    assertThat(split("SELECT $$a;b$$;SELECT $tag$c;$$;d$tag$;SELECT 3"))
        .containsExactly("SELECT $$a;b$$", "SELECT $tag$c;$$;d$tag$", "SELECT 3");
    assertThat(split("SELECT 1 -- a;b\n;SELECT 2")).containsExactly("SELECT 1 -- a;b\n", "SELECT 2");
    assertThat(split("SELECT 1 /* a; /* nested; */ b; */;SELECT 2")).containsExactly("SELECT 1 /* a; /* nested; */ b; */", "SELECT 2");
  }

  @Test
  @DisplayName("[#11483] a ';' inside an ArcadeDB JSON object or collection literal does not split")
  void arcadeDbJsonLiterals() {
    assertThat(split("INSERT INTO T CONTENT {\"k\": \"v;\\\"w\"};SELECT 2"))
        .containsExactly("INSERT INTO T CONTENT {\"k\": \"v;\\\"w\"}", "SELECT 2");
    assertThat(split("UPDATE T SET tags = [\"a;b\"];SELECT 2")).containsExactly("UPDATE T SET tags = [\"a;b\"]", "SELECT 2");
  }

  @Test
  @DisplayName("[#11483] an ArcadeDB context variable is no dollar-quoted literal")
  void contextVariable() {
    assertThat(split("SELECT $current;SELECT 2")).containsExactly("SELECT $current", "SELECT 2");
  }

  @Test
  @DisplayName("[#11483] an unterminated literal swallows the rest of the text, for the parser to report")
  void unterminatedLiteral() {
    assertThat(split("SELECT 'abc;SELECT 2")).containsExactly("SELECT 'abc;SELECT 2");
  }
}
