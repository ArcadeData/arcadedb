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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The extreme values of each scalar type, after Neo4j's ExtremeValuesLibrary: an LSM index on a property answers every
 * comparison and the order of its values exactly as a scan of the same values in an unindexed property does. A comparator
 * that subtracts two keys, narrows them, or converts them through a lossy unit breaks at these values first.
 */
class IndexExtremeValuesTest extends TestHelper {

  private static final String[] OPERATORS = { "=", "<", ">", "<=", ">=", "<>" };

  static Stream<Arguments> extremes() {
    return Stream.of(//
        Arguments.of("LONG", List.of(Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L, (1L << 53) + 1, -(1L << 53) - 1, 1L << 53)),
        Arguments.of("INTEGER", List.of(Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1)),
        Arguments.of("SHORT", List.of(Short.MIN_VALUE, Short.MAX_VALUE, (short) 0, (short) -1)),
        Arguments.of("BYTE", List.of(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0, (byte) -1)),
        Arguments.of("DOUBLE",
            List.of(Double.MIN_VALUE, Double.MAX_VALUE, -Double.MIN_VALUE, -Double.MAX_VALUE, 0d, 1e-300, (double) (1L << 53) + 2,
                Double.MIN_NORMAL)),
        Arguments.of("FLOAT", List.of(Float.MIN_VALUE, Float.MAX_VALUE, -Float.MIN_VALUE, -Float.MAX_VALUE, 0f, Float.MIN_NORMAL)),
        Arguments.of("STRING",
            List.of("", "a", "z", "\u007f", "\u00e9", "\ud7ff", "\ue000", "\uffff", new String(Character.toChars(0x10000)),
                new String(Character.toChars(Character.MAX_CODE_POINT)), "a" + new String(Character.toChars(0x1f600)), "a\uffff",
                "\u0000x", " a", "a ")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("extremes")
  void anIndexAnswersTheExtremeValuesAsAScanDoes(final String type, final List<Object> values) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.k INTEGER");
      database.command("sql", "CREATE PROPERTY T.indexed " + type);
      database.command("sql", "CREATE PROPERTY T.scanned " + type);
      database.command("sql", "CREATE INDEX ON T (indexed) NOTUNIQUE");
      for (int k = 0; k < values.size(); k++)
        database.command("sql", "INSERT INTO T SET k = ?, indexed = ?, scanned = ?", k, values.get(k), values.get(k));
    });

    assertThat(keys("SELECT k FROM T ORDER BY indexed, k")).isEqualTo(keys("SELECT k FROM T ORDER BY scanned, k"));
    assertThat(keys("SELECT k FROM T ORDER BY indexed DESC, k")).isEqualTo(keys("SELECT k FROM T ORDER BY scanned DESC, k"));
    for (final Object value : values)
      for (final String operator : OPERATORS)
        assertThat(keys("SELECT k FROM T WHERE indexed " + operator + " ? ORDER BY k", value)).as("%s %s %s", type, operator, value)
            .isEqualTo(keys("SELECT k FROM T WHERE scanned " + operator + " ? ORDER BY k", value));
    // every value finds itself, and no other, through the index
    for (int k = 0; k < values.size(); k++)
      assertThat(keys("SELECT k FROM T WHERE indexed = ?", values.get(k))).as("%s = %s", type, values.get(k)).containsExactly(k);
  }

  private List<Integer> keys(final String query, final Object... args) {
    final List<Integer> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, args)) {
      rs.forEachRemaining(r -> keys.add(r.<Integer>getProperty("k")));
    }
    return keys;
  }
}
