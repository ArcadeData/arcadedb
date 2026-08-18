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
package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A batch of SQL collection/string/misc methods and functions threw a raw runtime exception - an HTTP 500 - for
 * ordinary valid input, or returned a confident answer for input they never looked at (issue #6389). Each is now
 * either the natural answer or a typed argument error, which the HTTP layer maps to 400.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6389FunctionArgumentTest extends TestHelper {

  @Test
  void lastIndexOfGuardsNullsLikeItsIndexOfSibling() {
    try (final ResultSet rs = database.query("sql",
        "SELECT nothing.lastindexof('a') AS a, nothing.indexof('a') AS b, 'abcabc'.lastindexof('a') AS c")) {
      final var row = rs.next();
      assertThat(row.<Object>getProperty("a")).isNull();
      assertThat(row.<Object>getProperty("b")).isNull();
      assertThat(row.<Number>getProperty("c").intValue()).isEqualTo(3);
    }
  }

  @Test
  void joinRendersANullElementInsteadOfThrowing() {
    try (final ResultSet rs = database.query("sql", "SELECT [1,null,3].join('-') AS j")) {
      assertThat(rs.next().<String>getProperty("j")).isEqualTo("1-null-3");
    }
  }

  @Test
  void mapRejectsANonStringKey() {
    assertThatThrownBy(() -> database.query("sql", "SELECT map(1,'a') AS m").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("STRING key");
  }

  /**
   * A null key is rejected rather than turned into a null entry. The raw cast used to produce {@code (String) null}
   * and store it silently, so a mistyped call built a map with a key nobody can name.
   */
  @Test
  void mapRejectsANullKey() {
    assertThatThrownBy(() -> database.query("sql", "SELECT map(null,'a') AS m").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("STRING key");
  }

  @Test
  void asMapConvertsANonStringKey() {
    try (final ResultSet rs = database.query("sql", "SELECT [1,2,3,4].asMap() AS m")) {
      assertThat(rs.next().<Map<String, Object>>getProperty("m")).containsEntry("1", 2).containsEntry("3", 4);
    }
  }

  @Test
  void convertRejectsAnUnknownTypeName() {
    assertThatThrownBy(() -> database.query("sql", "SELECT 'x'.convert('nope') AS c").next())
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("nope");
  }

  @Test
  void convertStillWorksForAKnownType() {
    try (final ResultSet rs = database.query("sql", "SELECT '42'.convert('integer') AS c")) {
      assertThat(rs.next().<Object>getProperty("c")).isEqualTo(42);
    }
  }

  @Test
  void decodeReportsMalformedInputAndPassesNullThrough() {
    assertThatThrownBy(() -> database.query("sql", "SELECT decode('!!!','base64') AS d").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("base64");

    try (final ResultSet rs = database.query("sql", "SELECT decode(null,'base64') AS d")) {
      assertThat(rs.next().<Object>getProperty("d")).isNull();
    }
  }

  @Test
  void decodeStillRoundTripsWithEncode() {
    try (final ResultSet rs = database.query("sql", "SELECT decode(encode('hello','base64'),'base64').asString() AS d")) {
      assertThat(rs.next().<String>getProperty("d")).contains("hello");
    }
  }

  @Test
  void formatReportsAMismatchedConversion() {
    assertThatThrownBy(() -> database.query("sql", "SELECT format('%d','x') AS f").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("%d");
  }

  @Test
  void formatReportsAnUnknownConversion() {
    assertThatThrownBy(() -> database.query("sql", "SELECT format('%y') AS f").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("%y");
  }

  @Test
  void formatRefusesAnExcessiveFieldWidth() {
    assertThatThrownBy(() -> database.query("sql", "SELECT format('%99999999s','x') AS f").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
    assertThatThrownBy(() -> database.query("sql", "SELECT 'x'.format('%99999999s') AS f").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
  }

  /**
   * Both halves of a `%<width>.<precision>s` allocate, and `<` is a Formatter flag - reuse the previous argument -
   * not a conversion character. Measuring only the last number read, and stopping the scan at `<`, each let a field
   * far over the ceiling through the check the ceiling exists for.
   */
  @Test
  void formatWidthCeilingHasNoBypass() {
    // Width behind a precision: the argument is truncated to one character, then padded back out to two million.
    assertThatThrownBy(() -> database.query("sql", "SELECT format('%2000000.1s','x') AS f").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
    // The same shape on a numeric conversion, where the width is large enough to be an OutOfMemoryError.
    assertThatThrownBy(() -> database.query("sql", "SELECT format('%2000000000.1f', 3.14) AS f").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
    // '<' reuses the previous argument; the width follows it.
    assertThatThrownBy(() -> database.query("sql", "SELECT format('%s%<2000000s','x') AS f").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
    // And an argument index still is not a field size, so an ordinary reference is not mistaken for one.
    try (final ResultSet rs = database.query("sql", "SELECT format('%2000000$s','x','y') AS f")) {
      assertThatThrownBy(rs::next).isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("limit");
    }
  }

  @Test
  void formatStillFormats() {
    try (final ResultSet rs = database.query("sql",
        "SELECT format('%s-%05d','a',7) AS f, format('%1$s%1$s','z') AS g, format('100%%') AS h, "
            + "format('%10.3s|','abcdef') AS i, format('%s%<s','q') AS j")) {
      final var row = rs.next();
      assertThat(row.<String>getProperty("f")).isEqualTo("a-00007");
      assertThat(row.<String>getProperty("g")).isEqualTo("zz");
      assertThat(row.<String>getProperty("h")).isEqualTo("100%");
      assertThat(row.<String>getProperty("i")).isEqualTo("       abc|");
      assertThat(row.<String>getProperty("j")).isEqualTo("qq");
    }
  }

  @Test
  void maxAndMinRejectMutuallyIncomparableValues() {
    assertThatThrownBy(() -> database.query("sql", "SELECT max([1,'a']) AS m").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot compare");
    assertThatThrownBy(() -> database.query("sql", "SELECT min([1,'a']) AS m").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot compare");
  }

  @Test
  void maxAndMinStillWorkOnHomogeneousInput() {
    try (final ResultSet rs = database.query("sql", "SELECT max([3,1,2]) AS a, min([3,1,2]) AS b, max(['a','c','b']) AS c")) {
      final var row = rs.next();
      assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(3);
      assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(1);
      assertThat(row.<String>getProperty("c")).isEqualTo("c");
    }
  }

  @Test
  void boolAndOrRejectNonBooleanInput() {
    for (final String query : new String[] { "SELECT bool_and([1,2,3]) AS b", "SELECT bool_and(1,2) AS b",
        "SELECT bool_and(5) AS b", "SELECT bool_or([1,2,3]) AS b", "SELECT bool_or(5) AS b" })
      assertThatThrownBy(() -> database.query("sql", query).next())
          .as(query)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("boolean input");
  }

  @Test
  void boolAndOrStillWorkOnBooleans() {
    try (final ResultSet rs = database.query("sql",
        "SELECT bool_and([true,true]) AS a, bool_and([true,false]) AS b, bool_or([false,true]) AS c")) {
      final var row = rs.next();
      assertThat(row.<Boolean>getProperty("a")).isTrue();
      assertThat(row.<Boolean>getProperty("b")).isFalse();
      assertThat(row.<Boolean>getProperty("c")).isTrue();
    }
  }

  @Test
  void stringIndexMethodsAcceptADecimalIndex() {
    try (final ResultSet rs = database.query("sql",
        "SELECT 'abcdef'.substring(2.5) AS a, 'abcdef'.left(2.9) AS b, 'abcdef'.right(2.1) AS c, 'abcdef'.charAt(1.7) AS d")) {
      final var row = rs.next();
      // Truncated toward zero, then the #5885 clamps apply as they always did.
      assertThat(row.<String>getProperty("a")).isEqualTo("cdef");
      assertThat(row.<String>getProperty("b")).isEqualTo("ab");
      assertThat(row.<String>getProperty("c")).isEqualTo("ef");
      assertThat(row.<String>getProperty("d")).isEqualTo("b");
    }
  }

  @Test
  void stringIndexMethodsRejectNonNumericIndexes() {
    assertThatThrownBy(() -> database.query("sql", "SELECT 'abcdef'.substring('x') AS a").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("numeric");
  }

  /**
   * The decimal fallback must not hand an unbounded literal to BigDecimal: a character index is ten digits and a
   * sign, so a literal orders of magnitude longer is refused by length rather than parsed to find out.
   */
  @Test
  void stringIndexMethodsRefuseAnAbsurdlyLongLiteral() {
    final String huge = "9".repeat(100_000);
    assertThatThrownBy(() -> database.query("sql", "SELECT 'abcdef'.substring('" + huge + "') AS a").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit")
        // The message summarises the literal by length rather than echoing 100,000 characters into a log.
        .hasMessageNotContaining(huge);
    // A long-but-plausible literal still saturates rather than being refused.
    try (final ResultSet rs = database.query("sql", "SELECT 'abcdef'.substring('00000000000000000002.5') AS a")) {
      assertThat(rs.next().<String>getProperty("a")).isEqualTo("cdef");
    }
  }
}
