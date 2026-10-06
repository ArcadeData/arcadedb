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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9281: sum() and avg() over a BYTE property threw IllegalArgumentException (SQL and openCypher).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9281SumByteTest extends TestHelper {
  @Test
  void sumAvgMinMaxOverByte() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.b BYTE");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.b BYTE");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET b = 100");
      database.command("sql", "INSERT INTO T SET b = 50");
      database.command("sql", "CREATE VERTEX V SET b = 100");
      database.command("sql", "CREATE VERTEX V SET b = 50");
    });

    assertThat(one("sql", "SELECT sum(b) AS r FROM T")).isEqualTo(150);
    assertThat(((Number) one("sql", "SELECT avg(b) AS r FROM T")).doubleValue()).isEqualTo(75.0);
    assertThat(((Number) one("sql", "SELECT min(b) AS r FROM T")).intValue()).isEqualTo(50);
    assertThat(((Number) one("sql", "SELECT max(b) AS r FROM T")).intValue()).isEqualTo(100);
    assertThat(((Number) one("opencypher", "MATCH (n:V) RETURN sum(n.b) AS r")).intValue()).isEqualTo(150);
    assertThat(((Number) one("opencypher", "MATCH (n:V) RETURN avg(n.b) AS r")).doubleValue()).isEqualTo(75.0);
    assertThat(((Number) one("opencypher", "MATCH (n:V) RETURN min(n.b) AS r")).intValue()).isEqualTo(50);
    assertThat(((Number) one("opencypher", "MATCH (n:V) RETURN max(n.b) AS r")).intValue()).isEqualTo(100);
  }

  private Object one(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      return rs.next().getProperty("r");
    }
  }
}
