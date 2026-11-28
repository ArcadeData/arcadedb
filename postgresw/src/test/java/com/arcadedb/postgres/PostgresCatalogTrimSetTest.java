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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code btrim}, {@code ltrim}, {@code rtrim} and {@code trim(... from ...)} of a catalog statement, given the characters to
 * remove: they cost time in proportion to the text plus the set of characters, whatever the sizes, and read characters whole.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogTrimSetTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogTrimSetTest";
  private static final String USER          = "root";

  /** Characters of the text and of the set in the timed cases: a product of them is out of reach, their sum is nothing. */
  private static final int    SIZE     = 400_000;
  /** Every character of the text but one is in this set, and the first one it names is the last of the set. */
  private static final String SET      = "b".repeat(SIZE - 1) + "a";
  private static final long   BOUND_MS = 8_000;
  private static final String CLAIM    = "work in proportion to the text and the set, not to their product";

  private static final String ASTRAL_A = new String(Character.toChars(0x1F600));
  private static final String ASTRAL_B = new String(Character.toChars(0x1F601));
  private static final String ASTRAL_C = new String(Character.toChars(0x1F200));

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> database.getSchema().createDocumentType("Person"));
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- time

  @Test
  void aLargeSetOfCharactersCostsTheSizeOfItsArguments() {
    final String leading = "a".repeat(SIZE - 1) + "z";
    final String trailing = "z" + "a".repeat(SIZE - 1);
    final String both = "a".repeat(SIZE / 2) + "z" + "a".repeat(SIZE / 2);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(value("ltrim('" + leading + "', '" + SET + "')")).isEqualTo("z");
    assertThat(value("rtrim('" + trailing + "', '" + SET + "')")).isEqualTo("z");
    assertThat(value("btrim('" + both + "', '" + SET + "')")).isEqualTo("z");
    assertThat(value("trim(both '" + SET + "' from '" + both + "')")).isEqualTo("z");
    stopwatch.assertStayedUnder(BOUND_MS, CLAIM);
  }

  // ---------------------------------------------------------------- the characters named come off the ends

  @Test
  void theCharactersNamedComeOffTheEnds() {
    assertThat(value("btrim('xxabcxx', 'x')")).isEqualTo("abc");
    assertThat(value("ltrim('xxabcxx', 'x')")).isEqualTo("abcxx");
    assertThat(value("rtrim('xxabcxx', 'x')")).isEqualTo("xxabc");
    assertThat(value("btrim('xyxabcyxy', 'xy')")).isEqualTo("abc");
    assertThat(value("trim(both 'x' from 'xxabcxx')")).isEqualTo("abc");
    assertThat(value("trim(leading 'x' from 'xxabcxx')")).isEqualTo("abcxx");
    assertThat(value("trim(trailing 'x' from 'xxabcxx')")).isEqualTo("xxabc");
  }

  @Test
  void aLargeSetComesOffTheEndsTheSameWay() {
    final String set = "'0123456789abcdef'";

    assertThat(value("btrim('00ff-x-ff00', " + set + ")")).isEqualTo("-x-");
    assertThat(value("ltrim('00ff-x-ff00', " + set + ")")).isEqualTo("-x-ff00");
    assertThat(value("rtrim('00ff-x-ff00', " + set + ")")).isEqualTo("00ff-x-");
    assertThat(value("btrim('cafe0123', " + set + ")")).isEqualTo("");
    assertThat(value("btrim('', " + set + ")")).isEqualTo("");
    assertThat(value("btrim('-x-', " + set + ")")).isEqualTo("-x-");
  }

  @Test
  void anEmptySetOrAMissingOperandChangesNothing() {
    assertThat(value("btrim('xxabcxx', '')")).isEqualTo("xxabcxx");
    assertThat(value("btrim('', 'x')")).isEqualTo("");
    assertThat(value("btrim(null, 'x')")).isNull();
    assertThat(value("btrim('xxabcxx', null)")).isNull();
  }

  @Test
  void charactersOutsideTheBasicPlaneComeOffWhole() {
    final String set = "0123456789abcdef" + ASTRAL_A;

    assertThat(value("btrim('" + ASTRAL_A + "x" + ASTRAL_A + "', '" + ASTRAL_A + "')")).isEqualTo("x");
    assertThat(value("btrim('" + ASTRAL_A + "x" + ASTRAL_A + "', '" + set + "')")).isEqualTo("x");
    // U+1F601 shares its high surrogate with U+1F600, and U+1F200 its low one: neither is in the set
    assertThat(value("btrim('" + ASTRAL_B + "x" + ASTRAL_C + "', '" + ASTRAL_A + "')")).isEqualTo(ASTRAL_B + "x" + ASTRAL_C);
    assertThat(value("btrim('" + ASTRAL_B + "x" + ASTRAL_C + "', '" + set + "')")).isEqualTo(ASTRAL_B + "x" + ASTRAL_C);
  }

  // ---------------------------------------------------------------- helpers

  /** The one value the expression has on the one row of {@code pg_database}. */
  private Object value(final String expression) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select " + expression + " as v from pg_database", database, USER);
    assertThat(answer).as(expression).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).as(expression).hasSize(1);
    return answer.rows.get(0).get("v");
  }
}
