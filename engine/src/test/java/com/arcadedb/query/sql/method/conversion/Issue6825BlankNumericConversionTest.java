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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #6825: {@code asInteger()} alone special-cased the empty string and answered {@code null}, while
 * its six siblings handed {@code ""} straight to {@code Long.valueOf}/{@code Short.valueOf}/... and blew the whole
 * query up with a NumberFormatException. The one guard that existed was also inconsistent with itself: it tested the
 * untrimmed string and parsed the trimmed one, so {@code ' '.asInteger()} still threw.
 * <p>
 * An empty string is the normal representation of a blank field in imported CSV/JSON, so a cast that worked at one
 * width broke the query at every other width.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6825BlankNumericConversionTest extends TestHelper {

  static Stream<SQLMethod> numericMethods() {
    return Stream.of(new SQLMethodAsInteger(), new SQLMethodAsLong(), new SQLMethodAsShort(), new SQLMethodAsByte(),
        new SQLMethodAsFloat(), new SQLMethodAsDouble(), new SQLMethodAsDecimal());
  }

  @ParameterizedTest
  @MethodSource("numericMethods")
  void blankStringConvertsToNull(final SQLMethod method) {
    assertThat(method.execute(null, null, null, null)).as("null").isNull();
    assertThat(method.execute("", null, null, null)).as("empty string").isNull();
    assertThat(method.execute("   ", null, null, null)).as("whitespace only").isNull();
    assertThat(method.execute("\t\n ", null, null, null)).as("whitespace only").isNull();
  }

  @ParameterizedTest
  @MethodSource("numericMethods")
  void nonBlankStringStillConverts(final SQLMethod method) {
    final Object result = method.execute(" 42 ", null, null, null);
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).intValue()).isEqualTo(42);
  }

  @Test
  void everyWidthAgreesOverAnEmptyProperty() {
    database.getSchema().createDocumentType("Doc6825");
    database.transaction(() -> database.newDocument("Doc6825").set("s", "").save());

    try (final ResultSet rs = database.query("sql", """
        select s.asInteger() as i, s.asLong() as l, s.asShort() as sh, s.asByte() as b, \
        s.asFloat() as f, s.asDouble() as d, s.asDecimal() as dec from Doc6825""")) {
      final var row = rs.next();
      assertThat(row.<Object>getProperty("i")).isNull();
      assertThat(row.<Object>getProperty("l")).isNull();
      assertThat(row.<Object>getProperty("sh")).isNull();
      assertThat(row.<Object>getProperty("b")).isNull();
      assertThat(row.<Object>getProperty("f")).isNull();
      assertThat(row.<Object>getProperty("d")).isNull();
      assertThat(row.<Object>getProperty("dec")).isNull();
    }
  }
}
