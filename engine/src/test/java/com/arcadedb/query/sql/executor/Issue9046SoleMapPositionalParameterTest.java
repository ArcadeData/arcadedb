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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9046: a positional parameter that is a Map was lost when it was the only parameter of a SELECT, because the
 * statement took it for the named-parameter map.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9046SoleMapPositionalParameterTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.id INTEGER");
      database.command("sql", "INSERT INTO T SET id = 1, m = {'a': 1}");
    });
  }

  @Test
  void soleMapIsBoundToThePlaceholder() {
    final Map<String, Object> m = Map.of("a", 1);
    try (final ResultSet rs = database.query("sql", "SELECT ? AS v", (Object) m)) {
      assertThat(rs.next().<Map<String, Object>>getProperty("v")).isEqualTo(m);
    }
  }

  @Test
  void soleMapInWhere() {
    final Map<String, Object> m = Map.of("a", 1);
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE m = ?", (Object) m)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(1);
    }
  }

  @Test
  void mapWithNumericKeyIsNotReadAsParameterZero() {
    try (final ResultSet rs = database.query("sql", "SELECT ? AS v", (Object) Map.of("0", "x"))) {
      assertThat(rs.next().<Object>getProperty("v")).isEqualTo(Map.of("0", "x"));
    }
  }

  @Test
  void namedMapOverloadStillWorks() {
    try (final ResultSet rs = database.query("sql", "SELECT :p AS v", Map.of("p", 5))) {
      assertThat(rs.next().<Integer>getProperty("v")).isEqualTo(5);
    }
  }
}
