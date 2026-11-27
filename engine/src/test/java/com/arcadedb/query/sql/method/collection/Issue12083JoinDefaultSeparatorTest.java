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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #12083: {@code join()} without a separator was refused at parse time ("expects 1 argument but
 * got 0") although the documented default separator is {@code ','}.
 */
class Issue12083JoinDefaultSeparatorTest extends TestHelper {

  @Test
  void listWithoutSeparatorUsesComma() {
    assertThat(single("SELECT [1,2,3].join() AS r")).isEqualTo("1,2,3");
  }

  @Test
  void arrayWithoutSeparatorUsesComma() {
    assertThat(single("SELECT 'a,b,c'.split(',').join() AS r")).isEqualTo("a,b,c");
  }

  @Test
  void scalarReceiverWithoutSeparator() {
    assertThat(single("SELECT 'abc'.join() AS r")).isEqualTo("abc");
  }

  @Test
  void nullReceiverWithoutSeparator() {
    assertThat(single("SELECT null.join() AS r")).isNull();
  }

  @Test
  void explicitSeparatorUnchanged() {
    assertThat(single("SELECT [1,2,3].join(',') AS r")).isEqualTo("1,2,3");
    assertThat(single("SELECT [1,2,3].join('|') AS r")).isEqualTo("1|2|3");
  }

  @Test
  void nullSeparatorFallsBackToComma() {
    assertThat(single("SELECT [1,2,3].join(null) AS r")).isEqualTo("1,2,3");
  }

  @Test
  void twoArgumentsStillRefused() {
    assertThatThrownBy(() -> single("SELECT [1,2,3].join(',', '|') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("join");
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().getProperty("r");
    }
  }
}
