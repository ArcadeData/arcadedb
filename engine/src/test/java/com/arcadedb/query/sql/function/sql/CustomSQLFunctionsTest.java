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
package com.arcadedb.query.sql.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.within;

public class CustomSQLFunctionsTest {
  @Test
  public void testRandom() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      final ResultSet result = db.query("sql", "select math_random() as random");
      assertThat(result.next().<Double>getProperty("random")).isGreaterThan(0);
    });
  }

  @Test
  public void testLog10() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      final ResultSet result = db.query("sql", "select math_log10(10000) as log10");
      assertThat(result.next().<Double>getProperty("log10")).isCloseTo(4.0, within(0.0001));
    });
  }

  @Test
  public void testAbsInt() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      final ResultSet result = db.query("sql", "select math_abs(-5) as abs");
      assertThat(result.next().<Integer>getProperty("abs")).isEqualTo(5);
    });
  }

  @Test
  public void testAbsDouble() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      final ResultSet result = db.query("sql", "select math_abs(-5.0d) as abs");
      assertThat(result.next().<Double>getProperty("abs")).isEqualTo(5.0);
    });
  }

  @Test
  public void testAbsFloat() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      final ResultSet result = db.query("sql", "select math_abs(-5.0f) as abs");
      assertThat(result.next().<Float>getProperty("abs")).isEqualTo(5.0f);
    });
  }

  @Test
  public void testNonExistingFunction() {
    assertThatExceptionOfType(CommandParsingException.class).isThrownBy(
        () -> TestHelper.executeInNewDatabase("testRandom", (db) -> {
          final ResultSet result = db.query("sql", "select math_min('boom', 'boom') as boom");
          result.next();
        }));
  }
}
