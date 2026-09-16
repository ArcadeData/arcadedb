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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code split()} finds its delimiter as a literal. Its pieces must be the ones a quoted regex split with no limit
 * gives - the definition it had - for delimiters that are regex syntax, overlap themselves, or sit at either end.
 */
class CypherSplitLiteralDelimiterTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-split-literal").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  static Stream<Arguments> cases() {
    return Stream.of(
        Arguments.of("chunk-1<SEP>chunk-2<SEP>chunk-3", "<SEP>"),
        Arguments.of("<SEP>a<SEP><SEP>b<SEP>", "<SEP>"),
        Arguments.of("<SEP>", "<SEP>"),
        Arguments.of("", "<SEP>"),
        Arguments.of("no delimiter here", "<SEP>"),
        Arguments.of("a.b.c", "."),
        Arguments.of("a|b||c", "|"),
        Arguments.of("x\\Qy\\Ez\\Qw", "\\Q"),
        Arguments.of("a.*b.*", ".*"),
        Arguments.of("aaaaa", "aa"),
        Arguments.of("ab", "abc"),
        Arguments.of("한글,구분,,자", ","),
        Arguments.of("😀x😀😀y", "😀"),
        Arguments.of("$1$2$", "$"));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void piecesAreThoseOfAQuotedRegexSplit(final String str, final String delimiter) {
    final List<String> expected = Arrays.asList(str.split(Pattern.quote(delimiter), -1));
    try (final ResultSet rs = database.query("opencypher", "RETURN split($s, $d) AS pieces",
        Map.of("s", str, "d", delimiter))) {
      final List<String> pieces = rs.next().getProperty("pieces");
      assertThat(pieces).containsExactlyElementsOf(expected);
    }
  }
}
