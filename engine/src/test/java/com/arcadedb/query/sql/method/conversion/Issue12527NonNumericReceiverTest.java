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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12527: the seven numeric conversion methods ({@code asInteger()}, {@code asLong()}, {@code asByte()},
 * {@code asShort()}, {@code asFloat()}, {@code asDouble()}, {@code asDecimal()}) parsed a string receiver with
 * {@code Integer.valueOf(text)} and friends and caught nothing, so one non-numeric value in a column ({@code 'abc'}) threw a
 * raw {@link NumberFormatException} out of the executor and aborted the whole ResultSet. {@code asDecimal()}'s own Javadoc says
 * "if the conversion is not possible, null is returned". A receiver is data, not an argument the caller got wrong: the value
 * that cannot be converted answers {@code null}, like the blank one (#6825) and the null one, and the rest of the rows convert.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12527NonNumericReceiverTest extends TestHelper {

  /** A receiver that is not a number of the width asked for. */
  private static final String[] NOT_A_NUMBER = { "abc", "12abc", "1e", "--1", "0x10", "1.5.5", "NaN1", "++3", "1,5", "4 2" };

  static Stream<SQLMethod> numericMethods() {
    return Stream.of(new SQLMethodAsInteger(), new SQLMethodAsLong(), new SQLMethodAsShort(), new SQLMethodAsByte(),
        new SQLMethodAsFloat(), new SQLMethodAsDouble(), new SQLMethodAsDecimal());
  }

  @ParameterizedTest
  @MethodSource("numericMethods")
  void aNonNumericStringConvertsToNull(final SQLMethod method) {
    for (final String text : NOT_A_NUMBER)
      assertThat(method.execute(text, null, null, null)).as("%s on '%s'", method.getName(), text).isNull();
  }

  @ParameterizedTest
  @MethodSource("numericMethods")
  void aNumericStringStillConverts(final SQLMethod method) {
    final Object result = method.execute("42", null, null, null);
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).intValue()).isEqualTo(42);
  }

  @Test
  void aTextOutsideTheWidthConvertsToNull() {
    // a text that is a number, but not one the width can hold: the conversion is not possible
    assertThat(new SQLMethodAsByte().execute("300", null, null, null)).isNull();
    assertThat(new SQLMethodAsShort().execute("70000", null, null, null)).isNull();
    assertThat(new SQLMethodAsInteger().execute("3000000000", null, null, null)).isNull();
    assertThat(new SQLMethodAsLong().execute("99999999999999999999", null, null, null)).isNull();
    // the widths that hold it still answer
    assertThat(new SQLMethodAsLong().execute("3000000000", null, null, null)).isEqualTo(3000000000L);
    assertThat(new SQLMethodAsDecimal().execute("99999999999999999999", null, null, null)).isEqualTo(
        new BigDecimal("99999999999999999999"));
  }

  @Test
  void oneDirtyRowDoesNotAbortTheQuery() {
    database.getSchema().createDocumentType("T12527");
    database.transaction(() -> {
      database.newDocument("T12527").set("id", 1).set("code", "42").save();
      database.newDocument("T12527").set("id", 2).set("code", "abc").save();
      database.newDocument("T12527").set("id", 3).set("code", "7").save();
    });

    for (final String method : new String[] { "asInteger", "asLong", "asByte", "asShort", "asFloat", "asDouble", "asDecimal" }) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT code." + method + "() AS v FROM T12527 ORDER BY id")) {
        while (rs.hasNext())
          values.add(rs.next().getProperty("v"));
      }
      assertThat(values).as(method).hasSize(3);
      assertThat(((Number) values.get(0)).intValue()).as(method).isEqualTo(42);
      assertThat(values.get(1)).as(method + " on the non-numeric row").isNull();
      assertThat(((Number) values.get(2)).intValue()).as(method).isEqualTo(7);
    }
  }

  @Test
  void theControlFromTheReportAnswersNull() {
    try (final ResultSet rs = database.query("sql", "SELECT 'abc'.asInteger() AS v")) {
      assertThat((Object) rs.next().getProperty("v")).isNull();
    }
  }

  /**
   * The registry is the authority: a method called without arguments can only fail for its receiver, so no built-in method may
   * fail on a malformed text with an accidental JDK exception, see {@link #isAccidental}. {@code NumberFormatException} is the
   * shape the conversion methods leaked; a {@code ClassCastException} or an {@code IndexOutOfBoundsException} would be the same
   * defect.
   */
  @Test
  void noBuiltInMethodLeaksARawJdkExceptionForAMalformedReceiver() {
    final DefaultSQLMethodFactory factory = ((SQLQueryEngine) database.getQueryEngine("sql")).getMethodFactory();
    final Set<String> names = new TreeSet<>();
    for (final String name : factory.getBuiltInMethodNames())
      if (factory.isBuiltIn(name))
        names.add(name);
    assertThat(names).as("the census must reach the SQL method registry").contains("asinteger", "aslong", "asbyte", "asshort",
        "asfloat", "asdouble", "asdecimal");

    final String[] receivers = { "'abc'", "''", "'   '", "'1e'", "'--1'", "'0x'", "'NaN'", "'1.5.5'", "'99999999999999999999999'",
        "'2026-13-45'", "'{'", "'[1,'" };
    final List<String> offenders = new ArrayList<>();
    for (final String name : names)
      for (final String receiver : receivers) {
        final String sql = "SELECT (" + receiver + ")." + name + "() AS v";
        try (final ResultSet rs = database.command("sql", sql)) {
          while (rs.hasNext())
            rs.next().toJSON();
        } catch (final Throwable e) {
          if (database.isTransactionActive())
            database.rollback();
          if (isAccidental(e))
            offenders.add(sql + " -> " + e);
        }
      }
    assertThat(offenders).as("no-argument SQL method calls failing with a raw JDK exception for their receiver").isEmpty();
  }

  /**
   * A failure nobody wrote on purpose: what a parse, a cast, an index or a null dereference leaves behind. An
   * {@link IllegalArgumentException} thrown by the method itself is the validation idiom ({@code intersectsWith()} named
   * {@code requires a shape as parameter}, issue #12020 answers it as the caller's mistake), so it is not counted, except
   * for its subclass {@link NumberFormatException}, which is what {@code Integer.valueOf} leaves behind.
   */
  private static boolean isAccidental(final Throwable e) {
    return e instanceof NumberFormatException || e instanceof ClassCastException || e instanceof IndexOutOfBoundsException
        || e instanceof NullPointerException || e instanceof ArithmeticException || e instanceof DateTimeException
        || e instanceof NegativeArraySizeException;
  }
}
