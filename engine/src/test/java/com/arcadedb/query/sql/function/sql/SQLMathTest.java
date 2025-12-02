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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/*
* @author Luca Garulli (l.garulli@arcadedata.com)
*/
class SQLMathTest {
  @Test
  void sum() throws Exception {
    TestHelper.executeInNewDatabase("sql-math", (graph) -> {
      try (final ResultSet rs = graph.query("sql", "select (1+1+1) as math")) {
        assertThat(rs.next().<Integer>getProperty("math")).isEqualTo(3);
      }

      try (final ResultSet rs = graph.query("sql", "select (2+5*5) as math")) {
        assertThat(rs.next().<Integer>getProperty("math")).isEqualTo(27);
      }

      try (final ResultSet rs = graph.query("sql", "select ((2+5*5-3)/2 + pow(3,2)) as math")) {
        assertThat(rs.next().<Integer>getProperty("math")).isEqualTo(21);
      }
    });
  }
}
